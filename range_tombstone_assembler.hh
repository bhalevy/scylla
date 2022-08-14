/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <exception>
#include <seastar/core/print.hh>

#include "mutation_fragment_v2.hh"
#include "log.hh"

extern logging::logger mplog;

/// Converts a stream of range_tombstone_change fragments to an equivalent stream of range_tombstone objects.
/// The input fragments must be ordered by their position().
/// The produced range_tombstone objects are non-overlapping and ordered by their position().
///
/// on_end_of_stream() must be called after consuming all fragments to produce the final fragment.
///
/// Example usage:
///
///   range_tombstone_assembler rta;
///   if (auto rt_opt = rta.consume(range_tombstone_change(...))) {
///       produce(*rt_opt);
///   }
///   if (auto rt_opt = rta.consume(range_tombstone_change(...))) {
///       produce(*rt_opt);
///   }
///   if (auto rt_opt = rta.flush(position_in_partition(...)) {
///       produce(*rt_opt);
///   }
///   rta.on_end_of_stream();
///
class range_tombstone_assembler {
    std::optional<range_tombstone_change> _prev_rt;
    bool _end_of_stream = false;
    bool _error = false;
private:
    bool has_active_tombstone() const {
        return _prev_rt && _prev_rt->tombstone();
    }
public:
    range_tombstone_assembler() = default;
    range_tombstone_assembler(range_tombstone_assembler&& o) noexcept
        : _prev_rt(std::move(o._prev_rt))
        , _end_of_stream(std::exchange(o._end_of_stream, true))
        , _error(std::exchange(o._error, false))
    { }
    range_tombstone_assembler& operator=(range_tombstone_assembler&&) = default;

    ~range_tombstone_assembler() {
        if (!_end_of_stream && !_error) {
            std::string active_tombstone_desc = has_active_tombstone() ? format("{}", *_prev_rt) : "none";
            on_internal_error_noexcept(mplog, format("Stream ended with no end_of_stream or error. active tombstone={}", active_tombstone_desc));
        }
    }

    tombstone get_current_tombstone() const {
        return _prev_rt ? _prev_rt->tombstone() : tombstone();
    }

    std::optional<range_tombstone_change> get_range_tombstone_change() && {
        return std::move(_prev_rt);
    }

    void reset() {
        _prev_rt = std::nullopt;
    }

    std::optional<range_tombstone> consume(const schema& s, range_tombstone_change&& rt) {
        std::optional<range_tombstone> rt_opt;
        auto less = position_in_partition::less_compare(s);
        if (has_active_tombstone() && less(_prev_rt->position(), rt.position())) {
            rt_opt = range_tombstone(_prev_rt->position(), rt.position(), _prev_rt->tombstone());
        }
        _prev_rt = std::move(rt);
        return rt_opt;
    }

    void on_end_of_stream() {
        _end_of_stream = true;
        if (has_active_tombstone()) {
            throw std::logic_error(format("Stream ends with an active range tombstone: {}", *_prev_rt));
        }
    }

    void on_error() {
        _error = true;
    }

    // Returns true if and only if flush() may return something.
    // Returns false if flush() won't return anything for sure.
    bool needs_flush() const {
        return has_active_tombstone();
    }

    std::optional<range_tombstone> flush(const schema& s, position_in_partition_view pos) {
        auto less = position_in_partition::less_compare(s);
        if (has_active_tombstone() && less(_prev_rt->position(), pos)) {
            position_in_partition start = _prev_rt->position();
            _prev_rt->set_position(position_in_partition(pos));
            return range_tombstone(std::move(start), pos, _prev_rt->tombstone());
        }
        return std::nullopt;
    }

    bool discardable() const {
        return !has_active_tombstone();
    }
};
