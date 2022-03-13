/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <seastar/core/scheduling.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/file.hh>
#include <chrono>
#include <cmath>

#include "seastarx.hh"

// Simple proportional controller to adjust shares for processes for which a backlog can be clearly
// defined.
//
// Goal is to consume the backlog as fast as we can, but not so fast that we steal all the CPU from
// incoming requests, and at the same time minimize user-visible fluctuations in the quota.
//
// What that translates to is we'll try to keep the backlog's firt derivative at 0 (IOW, we keep
// backlog constant). As the backlog grows we increase CPU usage, decreasing CPU usage as the
// backlog diminishes.
//
// The exact point at which the controller stops determines the desired CPU usage. As the backlog
// grows and approach a maximum desired, we need to be more aggressive. We will therefore define two
// thresholds, and increase the constant as we cross them.
//
// Doing that divides the range in three (before the first, between first and second, and after
// second threshold), and we'll be slow to grow in the first region, grow normally in the second
// region, and aggressively in the third region.
//
// The constants q1 and q2 are used to determine the proportional factor at each stage.
class backlog_controller {
public:
    future<> shutdown() {
        _update_timer.cancel();
        return std::move(_inflight_update);
    }
protected:
    struct control_point {
        float input;
        float output;
    };

    struct control_config {
        std::chrono::milliseconds interval;
        std::vector<control_point> control_points;
        std::function<float()> current_backlog;
    };

    using config = std::variant<float /* static_shares */, control_config>;

    seastar::scheduling_group _scheduling_group;
    const ::io_priority_class& _io_priority;
    timer<> _update_timer;

    std::vector<control_point> _control_points;

    std::function<float()> _current_backlog;
    // updating shares for an I/O class may contact another shard and returns a future.
    future<> _inflight_update;

    virtual void update_controller(float quota);

    void adjust();

    backlog_controller(seastar::scheduling_group sg, const ::io_priority_class& iop, config cfg)
        : _scheduling_group(sg)
        , _io_priority(iop)
        , _inflight_update(make_ready_future<>())
    {
        if (std::holds_alternative<float>(cfg)) {
            update_controller(std::get<float>(cfg));
        } else {
            auto& ccfg = std::get<control_config>(cfg);
            _control_points = std::move(ccfg.control_points);
            _current_backlog = std::move(ccfg.current_backlog);
            _update_timer.set_callback([this] { adjust(); });
            _update_timer.arm_periodic(ccfg.interval);
        }
    }

    virtual ~backlog_controller() {}
public:
    backlog_controller(backlog_controller&&) = default;
    float backlog_of_shares(float shares) const;
    seastar::scheduling_group sg() {
        return _scheduling_group;
    }
};

// memtable flush CPU controller.
//
// - First threshold is the soft limit line,
// - Maximum is the point in which we'd stop consuming request,
// - Second threshold is halfway between them.
//
// Below the soft limit, we are in no particular hurry to flush, since it means we're set to
// complete flushing before we a new memtable is ready. The quota is dirty * q1, and q1 is set to a
// low number.
//
// The first half of the virtual dirty region is where we expect to be usually, so we have a low
// slope corresponding to a sluggish response between q1 * soft_limit and q2.
//
// In the second half, we're getting close to the hard dirty limit so we increase the slope and
// become more responsive, up to a maximum quota of qmax.
class flush_controller : public backlog_controller {
    static constexpr float hard_dirty_limit = 1.0f;
public:
    struct control_config {
        std::chrono::milliseconds interval;
        float soft_limit;
        std::function<float()> current_dirty;
    };

    using config = std::variant<float /* static_shares */, control_config>;

    flush_controller(seastar::scheduling_group sg, const ::io_priority_class& iop, config cfg)
        : backlog_controller(sg, iop, std::holds_alternative<float>(cfg)
            ? backlog_controller::config(std::get<float>(cfg))
            : backlog_controller::control_config{
                .interval = std::get<control_config>(cfg).interval,
                .control_points = {
                    {0.0, 0.0},
                    {std::get<control_config>(cfg).soft_limit, 10},
                    {std::get<control_config>(cfg).soft_limit + (hard_dirty_limit - std::get<control_config>(cfg).soft_limit) / 2, 200},
                    {hard_dirty_limit, 1000}},
                .current_backlog = std::move(std::get<control_config>(cfg).current_dirty),
            })
    {}
};

class compaction_controller : public backlog_controller {
public:
    static constexpr unsigned normalization_factor = 30;
    static constexpr float disable_backlog = std::numeric_limits<double>::infinity();
    static constexpr float backlog_disabled(float backlog) { return std::isinf(backlog); }
    compaction_controller(seastar::scheduling_group sg, const ::io_priority_class& iop, float static_shares) : backlog_controller(sg, iop, static_shares) {}
    compaction_controller(seastar::scheduling_group sg, const ::io_priority_class& iop, std::chrono::milliseconds interval, std::function<float()> current_backlog)
        : backlog_controller(sg, iop, backlog_controller::control_config{
            .interval = std::move(interval),
            .control_points = std::vector<backlog_controller::control_point>({{0.0, 50}, {1.5, 100} , {normalization_factor, 1000}}),
            .current_backlog = std::move(current_backlog),
        })
    {}
};
