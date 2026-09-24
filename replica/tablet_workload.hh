/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <chrono>
#include <cmath>
#include <utility>

#include <seastar/core/lowres_clock.hh>

#include <absl/container/flat_hash_map.h>

#include "seastarx.hh"

namespace replica {

/// Tracks a per-tablet workload rate, in bytes per second.
///
/// The tablet load balancer needs a signal for how much work a tablet demands, in addition
/// to how much disk space it occupies. We approximate it with the number of bytes touched by
/// reads and writes on behalf of the tablet. Bytes rather than operation counts, because the
/// cost of an operation varies by orders of magnitude between a point lookup and a scan of a
/// wide partition, while bytes correlate reasonably with both CPU and I/O.
///
/// The metric is deliberately placement-invariant: it measures the work the tablet demands,
/// not the CPU the shard happened to spend on it. Measured shard CPU would be a feedback
/// signal - migrating a tablet changes it - and balancing on it directly invites oscillation.
///
/// Bytes are accumulated by record(), which is on the data path and must stay cheap: it is a
/// hash lookup and an addition. The exponential decay is applied lazily by decay(), which is
/// called by the periodic tablet load stats collection, so its cost is amortized over the
/// whole collection interval.
///
/// See docs/dev/multi-dimensional-tablet-load-balancing.md.
class tablet_workload_tracker {
public:
    // Time constant of the moving average. Chosen to be several times the tablet load stats
    // refresh interval (60s by default), so that the rate reported to the balancer is stable
    // across collections but still tracks a workload shift within a few minutes.
    static constexpr std::chrono::duration<double> default_time_constant{300};

private:
    struct entry {
        uint64_t accumulated = 0; // Bytes recorded since the last decay().
        double rate = 0;          // Bytes per second, exponentially weighted.
    };

    // Keyed by storage group id, which for tablet tables is the tablet id.
    absl::flat_hash_map<size_t, entry> _groups;
    lowres_clock::time_point _last_decay = lowres_clock::now();
    std::chrono::duration<double> _time_constant = default_time_constant;

public:
    tablet_workload_tracker() = default;
    explicit tablet_workload_tracker(std::chrono::duration<double> time_constant) noexcept
        : _time_constant(time_constant)
    { }

    /// Records `bytes` of read or write work done on behalf of storage group `group_id`.
    /// Called from the data path.
    void record(size_t group_id, uint64_t bytes) noexcept {
        try {
            _groups[group_id].accumulated += bytes;
        } catch (...) {
            // Losing a workload sample under memory pressure is harmless, and the data path
            // must not fail because of load balancer accounting.
        }
    }

    /// Folds the bytes accumulated since the previous call into the moving average.
    ///
    /// Samples taken less than a second apart are ignored rather than folded in, because a
    /// short interval turns a small byte count into a large apparent rate. The rates
    /// reported in between remain the ones from the last real decay.
    void decay(lowres_clock::time_point now = lowres_clock::now()) {
        const auto dt = std::chrono::duration<double>(now - _last_decay);
        if (dt < std::chrono::duration<double>(1)) {
            return;
        }
        _last_decay = now;
        const double alpha = std::exp(-dt / _time_constant);
        for (auto& [group_id, e] : _groups) {
            const double sample = double(std::exchange(e.accumulated, 0)) / dt.count();
            e.rate = alpha * e.rate + (1 - alpha) * sample;
        }
    }

    /// Returns the workload rate of storage group `group_id` in bytes per second, as of the
    /// last decay(). Returns 0 for a group which was never recorded.
    double rate(size_t group_id) const noexcept {
        auto it = _groups.find(group_id);
        return it == _groups.end() ? 0 : it->second.rate;
    }

    /// Returns the sum of all group rates, in bytes per second, as of the last decay().
    double total_rate() const noexcept {
        double total = 0;
        for (const auto& [group_id, e] : _groups) {
            total += e.rate;
        }
        return total;
    }

    /// Drops the accounting for a storage group which no longer exists, e.g. after a tablet
    /// merge or after the tablet replica is migrated away.
    void remove(size_t group_id) noexcept {
        _groups.erase(group_id);
    }

    void clear() noexcept {
        _groups.clear();
    }
};

} // namespace replica
