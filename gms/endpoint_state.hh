/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <boost/range/adaptors.hpp>

#include <seastar/core/coroutine.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

#include "utils/serialization.hh"
#include "gms/heart_beat_state.hh"
#include "gms/application_state.hh"
#include "gms/versioned_value.hh"
#include "gms/endpoint_id.hh"
#include "utils/stall_free.hh"

#include <optional>
#include <chrono>

namespace gms {

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */
class endpoint_state : public enable_lw_shared_from_this<endpoint_state> {
public:
    using clk = seastar::lowres_system_clock;
private:
    heart_beat_state _heart_beat_state;
    std::map<application_state, versioned_value> _application_state;
    /* fields below do not get serialized */
    clk::time_point _update_timestamp;
    bool _is_alive;
    bool _is_normal = false;

public:
    bool operator==(const endpoint_state& other) const {
        return _heart_beat_state  == other._heart_beat_state &&
               _application_state == other._application_state &&
               _update_timestamp  == other._update_timestamp &&
               _is_alive          == other._is_alive;
    }

    endpoint_state() noexcept
        : _heart_beat_state()
        , _update_timestamp(clk::now())
        , _is_alive(true) {
        update_is_normal();
    }

    endpoint_state(heart_beat_state initial_hb_state) noexcept
        : _heart_beat_state(initial_hb_state)
        , _update_timestamp(clk::now())
        , _is_alive(true) {
        update_is_normal();
    }

    endpoint_state(heart_beat_state&& initial_hb_state,
            const std::map<application_state, versioned_value>& application_state)
        : _heart_beat_state(std::move(initial_hb_state))
        , _application_state(application_state)
        , _update_timestamp(clk::now())
        , _is_alive(true) {
        update_is_normal();
    }

    future<> clear_gently() noexcept {
        return utils::clear_gently(_application_state);
    }

    // Valid only on shard 0
    heart_beat_state& get_heart_beat_state() noexcept {
        return _heart_beat_state;
    }

    // Valid only on shard 0
    const heart_beat_state& get_heart_beat_state() const noexcept {
        return _heart_beat_state;
    }

    void set_heart_beat_state_and_update_timestamp(heart_beat_state hbs) noexcept {
        update_timestamp();
        _heart_beat_state = hbs;
    }

    const versioned_value* get_application_state_ptr(application_state key) const noexcept;

    /**
     * TODO replace this with operations that don't expose private state
     */
    // @Deprecated
    std::map<application_state, versioned_value>& get_application_state_map() noexcept {
        return _application_state;
    }

    const std::map<application_state, versioned_value>& get_application_state_map() const noexcept {
        return _application_state;
    }

    void add_application_state(application_state key, versioned_value value) {
        _application_state[key] = std::move(value);
        update_is_normal();
    }

    void add_application_state(const endpoint_state& es) {
        _application_state = es._application_state;
        update_is_normal();
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     *
     * Valid only on shard 0.
     */
    clk::time_point get_update_timestamp() const noexcept {
        return _update_timestamp;
    }

    void update_timestamp() noexcept {
        _update_timestamp = clk::now();
    }

    bool is_alive() const noexcept {
        return _is_alive;
    }

    void set_alive(bool alive) noexcept {
        _is_alive = alive;
    }

    void mark_alive() noexcept {
        set_alive(true);
    }

    void mark_dead() noexcept {
        set_alive(false);
    }

    std::string_view get_status() const noexcept {
        constexpr std::string_view empty = "";
        auto* app_state = get_application_state_ptr(application_state::STATUS);
        if (!app_state) {
            return empty;
        }
        const auto& value = app_state->value();
        if (value.empty()) {
            return empty;
        }
        auto pos = value.find(',');
        if (pos == sstring::npos) {
            return std::string_view(value);
        }
        return std::string_view(value.c_str(), pos);
    }

    locator::host_id get_host_id() const noexcept {
        locator::host_id host_id;
        auto* app_state = get_application_state_ptr(application_state::HOST_ID);
        if (app_state) {
            host_id = locator::host_id(utils::UUID(app_state->value()));
        }
        return host_id;
    }

    inet_address get_address() const noexcept {
        inet_address addr;
        auto* app_state = get_application_state_ptr(application_state::RPC_ADDRESS);
        if (app_state) {
            addr = inet_address(app_state->value());
        }
        return addr;
    }

    endpoint_id get_endpoint_id() const noexcept {
        return endpoint_id(get_host_id(), get_address());
    }

    bool is_shutdown() const noexcept {
        return get_status() == versioned_value::SHUTDOWN;
    }

    bool is_normal() const noexcept {
        return _is_normal;
    }

    void update_is_normal() noexcept {
        _is_normal = get_status() == versioned_value::STATUS_NORMAL;
    }

    bool is_cql_ready() const noexcept;

    friend std::ostream& operator<<(std::ostream& os, const endpoint_state& x);
};

class endpoint_state_map {
    using endpoint_state_ptr = lw_shared_ptr<endpoint_state>;
    std::unordered_map<inet_address, endpoint_state_ptr> _state_by_address;
    std::unordered_map<locator::host_id, endpoint_state_ptr> _state_by_host_id;

    std::unordered_map<inet_address, semaphore> _address_locks;
    std::unordered_map<locator::host_id, semaphore> _host_id_locks;
    struct endpoint_permit {
        rwlock::holder global_holder;
        semaphore_units<> address_lock_holder;
        semaphore_units<> host_id_lock_holder;
    };

public:
    endpoint_state_map() = default;

    size_t size() const noexcept {
        return _state_by_address.size();
    }

    bool contains(const endpoint_id& ep) const noexcept {
        return contains(ep.addr);
    }
    bool contains(inet_address addr) const noexcept {
        return _state_by_address.contains(addr);
    }

    // Return std:out_of_range if not found
    const endpoint_state* get_ptr(const endpoint_id& ep) const noexcept;
    const endpoint_state* get_ptr(locator::host_id host_id) const noexcept;
    const endpoint_state* get_ptr(inet_address addr) const noexcept;

    endpoint_state* get_ptr(const endpoint_id& ep) noexcept;
    endpoint_state* get_ptr(locator::host_id host_id) noexcept;
    endpoint_state* get_ptr(inet_address addr) noexcept;

    // Throw std:out_of_range if not found
    const endpoint_state& at(const endpoint_id& ep) const;
    const endpoint_state& at(locator::host_id host_id) const;
    const endpoint_state& at(inet_address addr) const;

    endpoint_state& at(const endpoint_id& ep);
    endpoint_state& at(locator::host_id host_id);
    endpoint_state& at(inet_address addr);

    // Get an existing endpoint_state or create a new one and get it.
    endpoint_state& get_or_create(const endpoint_id& node);

    // Set endpoint_state.
    endpoint_state& set(const endpoint_id& node, endpoint_state&& eps);

    // Erase endpoint_state, return true iff found and erased.
    bool erase(const endpoint_id& ep);

    future<> clear_gently() noexcept {
        co_await utils::clear_gently(_address_locks);
        co_await utils::clear_gently(_state_by_address);
        co_await utils::clear_gently(_state_by_host_id);
    }

    std::vector<inet_address> get_endpoints() const;

    template <typename Func>
    requires std::same_as<std::invoke_result_t<Func, inet_address, endpoint_state>, void>
    void do_for_each(Func func) const {
        for (const auto& [addr, epsp] : _state_by_address) {
            func(addr, *epsp);
        }
    }

    template <typename Func>
    requires std::same_as<std::invoke_result_t<Func, inet_address, endpoint_state>, stop_iteration>
    stop_iteration do_for_each(Func func) const {
        for (const auto& [addr, epsp] : _state_by_address) {
            if (auto stop = func(addr, *epsp)) {
                return stop;
            }
        }
        return stop_iteration::no;
    }

    template <typename Func>
    requires std::same_as<typename futurize<std::invoke_result_t<Func, const inet_address&, const endpoint_state&>>::type, future<>>
    future<> do_for_each_gently(Func func) const {
        using futurator = futurize<std::invoke_result_t<Func, inet_address, endpoint_state>>;
        auto nodes = boost::copy_range<std::vector<inet_address>>(_state_by_address | boost::adaptors::map_keys);
        for (const auto& addr : nodes) {
            auto it = _state_by_address.find(addr);
            if (it != _state_by_address.end()) {
                const auto& eps = it->second;
                co_await futurator::invoke(func, addr, *eps);
            }
        }
    }

    template <typename Func>
    requires std::same_as<typename futurize<std::invoke_result_t<Func, inet_address, endpoint_state>>::type, future<stop_iteration>>
    future<stop_iteration> do_for_each_gently(Func func) const {
        using futurator = futurize<std::invoke_result_t<Func, inet_address, endpoint_state>>;
        auto nodes = boost::copy_range<std::vector<inet_address>>(_state_by_address | boost::adaptors::map_keys);
        for (const auto& addr : nodes) {
            auto it = _state_by_address.find(addr);
            if (it != _state_by_address.end()) {
                const auto& eps = it->second;
                if (auto stop = co_await futurator::invoke(func, addr, *eps)) {
                    co_return stop;
                }
            }
        }
        co_return stop_iteration::no;
    }

    future<endpoint_permit> lock_endpoint(endpoint_id ep);
    future<endpoint_permit> lock_endpoint(inet_address addr) {
        return lock_endpoint(endpoint_id(locator::host_id::create_null_id(), addr));
    }
};

} // gms
