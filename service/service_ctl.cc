/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdexcept>

#include <seastar/core/loop.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/core/semaphore.hh>

#include "service_ctl.hh"
#include "log.hh"

namespace service {

logging::logger sclog("service_ctl");

sstring to_string(enum base_controller::state s) {
    switch (s) {
    case base_controller::state::initialized:   return "initialized";
    case base_controller::state::starting:      return "starting";
    case base_controller::state::started:       return "started";
    case base_controller::state::starting_service: return "starting_service";
    case base_controller::state::serving:       return "serving";
    case base_controller::state::draining:      return "draining";
    case base_controller::state::shutting_down: return "shutting down";
    case base_controller::state::shutdown:      return "shutdown";
    case base_controller::state::stopping:      return "stopping";
    case base_controller::state::stopped:       return "stopped";
    }
    on_internal_error_noexcept(sclog, format("Invalid base_controller::state: {}", s));
    return "(invalid)";
}

std::ostream& operator<<(std::ostream& os, enum base_controller::state s) {
    return os << to_string(s);
}

base_controller::base_controller(services_controller& sctl, sstring name)
    : _sctl(sctl)
    , _name(std::move(name))
    , _sem(1)
{
    _sctl.add_service(*this);
}

base_controller::~base_controller() {
    switch (_state) {
    case state::initialized:
    case state::stopped:
        break;
    case state::starting:
    case state::stopping:
        if (failed()) {
            break;
        }
    default:
        sclog.warn("{} destroyed in {} state: failed={} error={}", _name, _state, failed(), get_exception());
    }

    if (!_dependants.empty()) {
        sclog.warn("{} destroyed with {} dependants", _name, _dependants.size());
    }

    for (auto& dep : _dependencies) {
        auto found = dep->_dependants.erase(this);
        if (!found) {
            sclog.warn("{} not found in {}'s dependants", _name, dep->name());
        }
    }
}

base_controller* base_controller::lookup_dep(sstring name) {
    base_controller* sp = _sctl.lookup_service(name);
    if (sp == nullptr) {
        throw std::out_of_range(format("{}: controller not found", name));
    }
    depends_on(*sp);
    return sp;
}

base_controller* base_controller::lookup_dep(base_controller& o) {
    // for now
    return lookup_dep(o.name());
}

base_controller& base_controller::depends_on(base_controller& o) noexcept {
    o._dependants.insert(this);
    _dependencies.insert(&o);
    sclog.debug("'{}' depends on '{}'", name(), o.name());
    return *this;
}

future<> base_controller::pending_op(std::function<future<>()> func) noexcept {
    _pending = seastar::with_semaphore(_sem, 1, [&func] {
        return func();
    });
    co_await _pending.get_future();
}

future<> base_controller::start() {
    switch (_state) {
    case state::initialized:
        break;
    case state::starting:
        co_return co_await _pending.get_future();
    case state::started:
        co_return;
    default:
        throw std::runtime_error(format("Cannot start service in state '{}'", _state));
    }

    sclog.info("Starting {}: dependencies={}", _name, _dependencies.size());
    _state = state::starting;
    co_await pending_op([this] () -> future<> {
        try {
            co_await parallel_for_each(_dependencies, [] (base_controller* dep) {
                return dep->start();
            });
            co_await do_start();
        } catch (...) {
            _ex = std::current_exception();
            sclog.error("Starting {} failed: {}", _name, _ex);
            throw;
        }
        _state = state::started;
        sclog.info("Started {}", _name);
    });
}

future<> base_controller::serve() {
    switch (_state) {
    case state::starting_service:
        co_return co_await _pending.get_future();
    case state::serving:
        co_return;
    case state::started:
        break;
    default:
        throw std::runtime_error(format("Cannot start service in state '{}'", _state));
    }

    _state = state::starting_service;
    sclog.info("{}: Starting to serve", _name);
    co_await pending_op([this] () -> future<> {
        try {
            co_await parallel_for_each(_dependencies, [] (base_controller* dep) {
                return dep->serve();
            });
            co_await do_serve();
        } catch (...) {
            _ex = std::current_exception();
            sclog.info("{}: Starting to serve failed: {}", _name, _ex);
            throw;
        }
        _state = state::serving;
        sclog.info("{}: Started to serve", _name);
    });
}

future<> base_controller::drain() {
    switch (_state) {
    case state::initialized:
    case state::starting:
    case state::started:
        co_return;
    case state::draining:
        co_return co_await _pending.get_future();
    case state::serving:
        break;
    default:
        auto msg = format("Draining {} in unexpected state '{}': failed={} error={}", _name, _state, failed(), get_exception());
        on_internal_error_noexcept(sclog, msg);
    }
    _state = state::draining;
    sclog.info("Draining {}", _name);
    co_await pending_op([this] () -> future<> {
        try {
            co_await do_drain();
            sclog.info("Drained {}", _name);
            co_await parallel_for_each(_dependencies, [] (base_controller* dep) {
                return dep->drain();
            });
        } catch (...) {
            _ex = std::current_exception();
            sclog.error("Draining {} failed: {}", _name, _ex);
            throw;
        }
        _state = state::started;
    });
}

future<> base_controller::shutdown() {
    switch (_state) {
    case state::initialized:
    case state::starting:
    case state::shutdown:
        co_return;
    case state::shutting_down:
        co_return co_await _pending.get_future();
    case state::serving:
        co_await drain().handle_exception([this] (std::exception_ptr ex) {
            auto msg = format("Auto-drain while shutting down {} failed: {}", _name, _ex);
            on_internal_error_noexcept(sclog, msg);
        });
        [[fallthrough]];
    case state::started:
        break;
    default:
        auto msg = format("Shutting down {} in unexpected state '{}': failed={} error={}", _name, _state, failed(), get_exception());
        on_internal_error_noexcept(sclog, msg);
    }
    _state = state::shutting_down;
    sclog.info("Shutting down {}", _name);
    co_await pending_op([this] () -> future<> {
        try {
            co_await do_shutdown();
            sclog.info("Shut down {}", _name);
            co_await parallel_for_each(_dependencies, [] (base_controller* dep) {
                return dep->shutdown();
            });
        } catch (...) {
            _ex = std::current_exception();
            sclog.error("Shutting down {} failed: {}", _name, _ex);
            throw;
        }
        _state = state::shutdown;
    });
}

future<> base_controller::stop() {
    switch (_state) {
    case state::initialized:
    case state::stopped:
        co_return;
    case state::stopping:
        co_return co_await _pending.get_future();
    case state::starting:
    case state::draining:
    case state::shutting_down:
        if (failed()) {
            break;
        }
    case state::started:
    case state::serving:
        co_await shutdown().handle_exception([this] (std::exception_ptr ex) {
            auto msg = format("Auto-shutdown while stopping {} failed: {}", _name, _ex);
            on_internal_error_noexcept(sclog, msg);
        });
        [[fallthrough]];
    case state::shutdown:
        break;
    default:
        auto msg = format("Stopping {} in unexpected state '{}': failed={} error={}", _name, _state, failed(), get_exception());
        on_internal_error_noexcept(sclog, msg);
    }
    _state = state::stopping;
    sclog.info("Stopping {}", _name);
    co_await pending_op([this] () -> future<> {
        try {
            co_await do_stop();
            sclog.info("Stopped {}", _name);
            co_await parallel_for_each(_dependencies, [] (base_controller* dep) {
                return dep->stop();
            });
        } catch (...) {
            _ex = std::current_exception();
            auto msg = format("Stopping {} failed: {}", _name, _ex);
            // stop() should never fail
            on_internal_error(sclog, msg);
        }
        _state = state::stopped;
    });
}

void services_controller::add_service(base_controller& s) {
    _all_services.emplace_back(&s);
    try {
        _top_level.insert(&s);
        auto [_, inserted] = _services_map.insert({s.name(), &s});
        if (!inserted) {
            on_internal_error_noexcept(sclog, format("service '{}' already inserted", s.name()));
        }
    } catch (...) {
        _all_services.pop_back();
        _top_level.erase(&s);
        throw;
    }
}

base_controller* services_controller::lookup_service(sstring name) noexcept {
    auto it = _services_map.find(name);
    if (it == _services_map.end()) {
        return nullptr;
    }
    base_controller* sp = it->second;
    _top_level.erase(sp);
    return sp;
}

base_controller* services_controller::lookup_service(base_controller& o) {
    // for now
    return lookup_service(o.name());
}

future<> services_controller::start() noexcept {
    for (auto sp : _top_level) {
        co_await sp->start();
    }
}

future<> services_controller::serve() noexcept {
    for (auto sp : _top_level) {
        co_await sp->serve();
    }
}

future<> services_controller::drain() noexcept {
    for (auto sp : _top_level) {
        co_await sp->drain();
    }
}

future<> services_controller::shutdown() noexcept {
    for (auto sp : _top_level) {
        co_await sp->shutdown();
    }
}

future<> services_controller::stop() noexcept {
    while (!_all_services.empty()) {
        auto* sp = _all_services.back();
        _all_services.pop_back();
        _top_level.erase(sp);
        _services_map.erase(sp->name());
        co_await sp->stop();
    }
}

} // namespace service
