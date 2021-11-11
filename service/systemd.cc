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

#include <seastar/core/loop.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/coroutine.hh>

#include "systemd.hh"
#include "log.hh"

namespace service {

logging::logger sclog("systemd");

sstring to_string(enum base_controller::state s) {
    switch (s) {
    case base_controller::state::initialized:   return "initialized";
    case base_controller::state::starting:      return "starting";
    case base_controller::state::started:       return "started";
    case base_controller::state::starting_service: return "starting_service";
    case base_controller::state::serving:       return "serving";
    case base_controller::state::draining:      return "draining";
    case base_controller::state::drained:       return "drained";
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

sstring to_string(enum base_controller::service_mode m) {
    switch (m) {
    case base_controller::service_mode::none:           return "none";
    case base_controller::service_mode::normal:         return "normal";
    case base_controller::service_mode::maintenance:    return "maintenance";
    }
    on_internal_error_noexcept(sclog, format("Invalid base_controller::service_mode: {}", m));
    return "(invalid)";
}

std::ostream& operator<<(std::ostream& os, enum base_controller::service_mode s) {
    return os << to_string(s);
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

base_controller& base_controller::depends_on(base_controller& o) noexcept {
    o._dependants.insert(this);
    _dependencies.insert(&o);
    return *this;
}

future<> base_controller::start() noexcept {
    switch (_state) {
    case state::initialized:
        break;
    case state::starting:
        return _pending.get_future();
    case state::started:
        return make_ready_future<>();
    default:
        return make_exception_future<>(std::runtime_error(format("Cannot start service in state '{}'", _state)));
    }

    sclog.info("Starting {}", _name);
    _state = state::starting;
    _pending = parallel_for_each(_dependencies, [] (base_controller* dep) {
        return dep->start();
    }).then([this] {
        return do_start();
    }).then_wrapped([this] (future<> f) {
        if (f.failed()) {
            _ex = f.get_exception();
            sclog.error("Starting {} failed: {}", _name, _ex);
            return make_exception_future<>(get_exception());
        }
        _state = state::started;
        sclog.info("Started {}", _name);
        return make_ready_future<>();
    });
    return _pending.get_future();
}

future<> base_controller::serve(service_mode m) noexcept {
    switch (_state) {
    case state::starting_service:
        return _pending.get_future();
    case state::serving:
        // FIXME: support service_mode transition
        assert(m == _service_mode);
        return make_ready_future<>();
    case state::started:
        assert(_service_mode == service_mode::none);
        break;
    default:
        return make_exception_future<>(std::runtime_error(format("Cannot start service in state '{}'", _state)));
    }

    _state = state::starting_service;
    _service_mode = m;
    sclog.info("{}: Starting {} service mode", _name, _service_mode);
    _pending = parallel_for_each(_dependencies, [m] (base_controller* dep) {
        return dep->serve(m);
    }).then([this, m] {
        return do_serve(m);
    }).then_wrapped([this] (future<> f) {
        if (f.failed()) {
            _ex = f.get_exception();
            sclog.info("{}: Starting {} service mode {} failed: ", _name, _service_mode, _ex);
            return make_exception_future<>(get_exception());
        }
        _state = state::serving;
        sclog.info("{}: Started {} service mode", _name, _service_mode);
        return make_ready_future<>();
    });
    return _pending.get_future();
}

future<> base_controller::drain() noexcept {
    switch (_state) {
    case state::initialized:
    case state::starting:
    case state::drained:
        return make_ready_future<>();
    case state::draining:
        return _pending.get_future();
    case state::started:
    case state::serving:
        break;
    default:
        auto msg = format("Draining {} in unexpected state '{}': failed={} error={}", _name, _state, failed(), get_exception());
        on_internal_error_noexcept(sclog, msg);
    }
    _state = state::draining;
    sclog.info("Draining {}", _name);
    _pending = do_drain().then([this] {
        sclog.info("Drained {}", _name);
        return parallel_for_each(_dependencies, [] (base_controller* dep) {
            return dep->drain();
        });
    }).then_wrapped([this] (future<> f) {
        if (f.failed()) {
            _ex = f.get_exception();
            sclog.error("Draining {} failed: {}", _name, _ex);
            return make_exception_future<>(get_exception());
        }
        _state = state::drained;
        _service_mode = service_mode::none;
        return make_ready_future<>();
    });
    return _pending.get_future();
}

future<> base_controller::shutdown() noexcept {
    switch (_state) {
    case state::initialized:
    case state::starting:
    case state::shutdown:
        return make_ready_future<>();
    case state::shutting_down:
        return _pending.get_future();
    case state::started:
    case state::drained:
        break;
    default:
        auto msg = format("Shutting down {} in unexpected state '{}': failed={} error={}", _name, _state, failed(), get_exception());
        on_internal_error_noexcept(sclog, msg);
    }
    _state = state::shutting_down;
    sclog.info("Shutting down {}", _name);
    _pending = do_shutdown().then([this] {
        sclog.info("Shut down {}", _name);
        return parallel_for_each(_dependencies, [] (base_controller* dep) {
            return dep->shutdown();
        });
    }).then_wrapped([this] (future<> f) {
        if (f.failed()) {
            _ex = f.get_exception();
            sclog.error("Shutting down {} failed: {}", _name, _ex);
            return make_exception_future<>(get_exception());
        }
        _state = state::shutdown;
        return make_ready_future<>();
    });
    return _pending.get_future();
}

future<> base_controller::stop() noexcept {
    switch (_state) {
    case state::initialized:
    case state::stopped:
        return make_ready_future<>();
    case state::stopping:
        return _pending.get_future();
    case state::starting:
    case state::draining:
    case state::shutting_down:
        if (failed()) {
            break;
        }
    case state::started:
    case state::shutdown:
        break;
    default:
        auto msg = format("Stopping {} in unexpected state '{}': failed={} error={}", _name, _state, failed(), get_exception());
        on_internal_error_noexcept(sclog, msg);
    }
    _state = state::stopping;
    sclog.info("Stopping {}", _name);
    _pending = do_stop().then([this] {
        sclog.info("Stopped {}", _name);
        return parallel_for_each(_dependencies, [] (base_controller* dep) {
            return dep->stop();
        });
    }).then_wrapped([this] (future<> f) {
        if (f.failed()) {
            _ex = f.get_exception();
            sclog.error("Stopping {} failed: {}", _name, _ex);
            return make_exception_future<>(get_exception());
        }
        _state = state::stopped;
        return make_ready_future<>();
    });
    return _pending.get_future();
}

services_controller::services_controller()
{
}

void services_controller::add_service(base_controller& s) noexcept {
    _all_services.emplace_back(&s);
}

future<> services_controller::start() noexcept {
    for (auto it = _all_services.begin(); it != _all_services.end(); it++) {
        co_await (*it)->start();
    }
}

future<> services_controller::serve(base_controller::service_mode m) noexcept {
    for (auto it = _all_services.begin(); it != _all_services.end(); it++) {
        co_await (*it)->serve(m);
    }
}

future<> services_controller::drain() noexcept {
    for (auto it = _all_services.rbegin(); it != _all_services.rend(); it++) {
        co_await (*it)->drain();
    }
}

future<> services_controller::shutdown() noexcept {
    for (auto it = _all_services.rbegin(); it != _all_services.rend(); it++) {
        co_await (*it)->shutdown();
    }
}

future<> services_controller::stop() noexcept {
    for (auto it = _all_services.rbegin(); it != _all_services.rend(); it++) {
        co_await (*it)->stop();
    }
}

} // namespace service
