/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <chrono>
#include <filesystem>
#include <set>

#include <boost/intrusive/list.hpp>

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/optimized_optional.hh>

#include "seastarx.hh"

using namespace std::chrono_literals;

namespace bi = boost::intrusive;

namespace utils {

// Instantiated only on shard 0
class disk_space_monitor {
public:
    struct config {
        scheduling_group sched_group;
        lowres_clock::duration normal_polling_interval = 10s;
        lowres_clock::duration high_polling_interval = 1s;
        // Use high_polling_interval above this thrshold
        float polling_interval_threshold = 0.95;
    };

    using subscription_callback_type = noncopyable_function<future<> (const disk_space_monitor&)>;
    class subscription : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        friend class disk_space_monitor;

        subscription_callback_type _callback;

        explicit subscription(subscription_callback_type callback)
                : _callback(std::move(callback)) {
        }

    public:
        subscription() = default;

        subscription(subscription&& other) noexcept
                : _callback(std::move(other._callback))
        {
            subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
        }

        subscription& operator=(subscription&& other) noexcept {
            if (this != &other) {
                _callback = std::move(other._callback);
                unlink();
                subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
            }
            return *this;
        }

        explicit operator bool() const noexcept {
            return is_linked();
        }

        future<> on_event(const disk_space_monitor& dsm) const noexcept {
            return _callback(dsm);
        }
    };

private:
    std::set<std::filesystem::path> _data_dirs;
    config _cfg;
    timer<lowres_clock> _timer;
    uint64_t _total_space = 0;
    uint64_t _free_space = 0;
    gate _async_gate;

    using subscription_list_type = bi::list<subscription, bi::constant_time_size<false>>;
    subscription_list_type _subscriptions;

public:
    void start(std::set<std::filesystem::path> data_dirs, config cfg);
    void start(std::filesystem::path data_dir, config cfg) {
        start(std::set<std::filesystem::path>({std::move(data_dir)}), std::move(cfg));
    }
    future<> stop() noexcept;

    const std::set<std::filesystem::path>& data_dirs() const noexcept {
        return _data_dirs;
    }
    uint64_t total_space() const noexcept { return _total_space; }
    uint64_t free_space() const noexcept { return _free_space; }
    float disk_utilization() const noexcept {
        return _total_space ? (float)(_total_space - _free_space) / _total_space : -1;
    }

    optimized_optional<subscription> subscribe(subscription_callback_type callback) {
        auto ret = subscription(std::move(callback));
        _subscriptions.push_back(ret);
        return ret;
    }

private:
    future<> poll();
    lowres_clock::duration get_polling_interval();
};

class disk_space_monitor_logger {
    optimized_optional<disk_space_monitor::subscription> _sub;
    float _next_above;
    float _next_below;

public:
    disk_space_monitor_logger(disk_space_monitor& dsm);

    future<> log(const disk_space_monitor& dsm);
};

} // namespace utils
