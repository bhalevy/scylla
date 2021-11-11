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

#pragma once

#include "seastar/core/shared_ptr.hh"
#include <unordered_set>
#include <functional>
#include <iostream>
#include <deque>
#include <unordered_map>
#include <type_traits>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/semaphore.hh>

using namespace seastar;

namespace service {

class base_controller;

// Top-level controller and registry
class services_controller {
    std::deque<base_controller*> _all_services;
    std::unordered_map<sstring, base_controller*> _services_map;
    std::unordered_set<base_controller*> _top_level;

public:
    void add_service(base_controller& s);
    base_controller* lookup_service(sstring name) noexcept;
    base_controller* lookup_service(base_controller& o);

    future<> start() noexcept;
    future<> serve() noexcept;
    future<> drain() noexcept;
    future<> shutdown() noexcept;
    future<> stop() noexcept;
};

class base_controller {
public:
    enum class state {
        initialized,
        starting,
        started,
        starting_service,
        serving,
        draining,
        shutting_down,
        shutdown,
        stopping,
        stopped,
    };

private:
    services_controller& _sctl;
    sstring _name;
    std::unordered_set<base_controller*> _dependencies;
    std::unordered_set<base_controller*> _dependants;
    state _state = state::initialized;
    semaphore _sem;
    shared_future<> _pending = {};
    std::exception_ptr _ex;

public:
    explicit base_controller(services_controller& sctl, sstring name);

    base_controller(base_controller&&) = default;

    ~base_controller();

    base_controller* lookup_dep(sstring name);
    base_controller* lookup_dep(base_controller& o);

    base_controller& depends_on(base_controller& o);

    const sstring& name() const noexcept {
        return _name;
    }

    state state() const noexcept {
        return _state;
    }

    bool failed() const noexcept {
        return bool(_ex);
    }

    std::exception_ptr get_exception() const noexcept {
        return _ex;
    }

    future<> start();
    future<> serve();
    future<> drain();
    future<> shutdown();
    future<> stop();

protected:
    virtual future<> do_start() = 0;
    virtual future<> do_serve() = 0;
    virtual future<> do_drain() = 0;
    virtual future<> do_shutdown() = 0;
    virtual future<> do_stop() = 0;

private:
    bool does_depend_on(base_controller* op) noexcept;
    future<> pending_op(std::function<future<>()> func) noexcept;
};

sstring to_string(enum base_controller::state s);
std::ostream& operator<<(std::ostream&, enum base_controller::state s);

template <typename Service>
class sharded_service_ctl : public base_controller {
public:
    using func_t = std::function<future<> (sharded<Service>&)>;

private:
    sharded<Service> _service;

public:
    func_t start_func;
    func_t serve_func = [] (sharded<Service>&) { return make_ready_future<>(); };
    func_t drain_func = [] (sharded<Service>&) { return make_ready_future<>(); };
    func_t shutdown_func = [] (sharded<Service>&) { return make_ready_future<>(); };
    func_t stop_func = [] (sharded<Service>& s) { return s.stop(); };

    sharded_service_ctl(services_controller& sctl, sstring name)
        : base_controller(sctl, std::move(name))
    {
    }

    sharded_service_ctl(services_controller& sctl, sstring name, func_t start_fn)
        : base_controller(sctl, std::move(name))
        , start_func(std::move(start_fn))
    {
    }

    template <typename T>
    auto lookup_dep(sharded_service_ctl<T>& o) {
        base_controller::lookup_dep(o);
        return std::ref(o.service());
    }

    sharded<Service>& service() noexcept {
        return _service;
    }

    const sharded<Service>& service() const noexcept {
        return _service;
    }

    Service& local() noexcept {
        return _service.local();
    }

    const Service& local() const noexcept {
        return _service.local();
    }

protected:
    virtual future<> do_start() noexcept override {
        return futurize_invoke(start_func, _service);
    }

    virtual future<> do_serve() noexcept override {
        return futurize_invoke(serve_func, _service);
    }

    virtual future<> do_drain() noexcept override {
        return futurize_invoke(drain_func, _service);
    }

    virtual future<> do_shutdown() noexcept override {
        return futurize_invoke(shutdown_func, _service);
    }

    virtual future<> do_stop() noexcept override {
        return futurize_invoke(stop_func, _service);
    }
};

} // namespace service
