/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>

#include "utils/disk_space_monitor.hh"
#include "utils/log.hh"

namespace utils {

seastar::logger logger("disk_space");

void disk_space_monitor::start(std::set<std::filesystem::path> data_dirs, config cfg) {
    if (data_dirs.empty()) {
        on_internal_error(logger, "data_dir must be non-empty");
    }
    if (!_data_dirs.empty()) {
        on_internal_error(logger, "disk_space_monitor already started");
    }
    _data_dirs = std::move(data_dirs);
    _cfg = std::move(cfg);
    _timer.set_callback(cfg.sched_group, [this] { (void)poll(); });
    (void)poll();
}

future<> disk_space_monitor::stop() noexcept {
    _timer.cancel();
    co_await _async_gate.close();
}

future<> disk_space_monitor::poll() {
    auto gh = _async_gate.hold();
    uint64_t total_space = 0;
    uint64_t free_space = 0;
    for (const auto& data_dir : _data_dirs) {
        auto st = co_await engine().statvfs(data_dir.native());
        total_space += (uint64_t)st.f_blocks * st.f_frsize;
        free_space += (uint64_t)st.f_bfree * st.f_frsize;
    }
    _total_space = total_space;
    _free_space = free_space;
    auto interval = get_polling_interval();
    _timer.arm(interval);

    subscription_list_type::iterator next;
    for (auto it = _subscriptions.begin(); it != _subscriptions.end(); it = next) {
        next = it;
        ++next;
        co_await it->on_event(*this);
    }
}

lowres_clock::duration disk_space_monitor::get_polling_interval() {
    auto du = disk_utilization();
    return du < _cfg.polling_interval_threshold ? _cfg.normal_polling_interval : _cfg.high_polling_interval;
}

disk_space_monitor_logger::disk_space_monitor_logger(disk_space_monitor& dsm)
    : _sub(dsm.subscribe([this] (const disk_space_monitor& dsm) { return log(dsm); }))
{
}

future<> disk_space_monitor_logger::log(const disk_space_monitor& dsm) {
    auto du = dsm.disk_utilization();
    if (du >= _next_above || du < _next_below) {
        logger.info("total_space={} free_space={} utilization={:.1f}%", dsm.total_space(), dsm.free_space(), du * 100);
        if (du >= 0) {
            auto free_du = std::min(1. - du, 0.);
            _next_above = std::max(1. - free_du * 0.9, 0.99);
            _next_below = std::min(1. - free_du * 1.1, 0.);
        }
    }
    return make_ready_future();
}

} // namespace utils
