/*
 * Copyright (C) 2020 ScyllaDB
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

#include <seastar/core/future.hh>

namespace seastar {

template <typename Object>
concept closeable = requires (Object o) {
    { o.close() } -> std::same_as<future<>>;
};

/// Template helper to close \c obj when destroyed.
///
/// \tparam Object a class exposing a \c close() method that returns a \c future<>
///         that is called when the controller is destroyed.
template <typename Object>
requires closeable<Object>
class close_controller {
    Object& _obj;
    bool _closed = false;

    void do_close() noexcept {
        if (!_closed) {
            _closed = true;
            _obj.close().get();
        }
    }
public:
    close_controller(Object& obj) noexcept : _obj(obj) {}
    ~close_controller() {
        do_close();
    }
    /// Close \c obj once now.
    void close_now() noexcept {
        assert(!_closed);
        do_close();
    }
};

/// Auto-close an object.
///
/// \param obj object to auto-close.
///
/// \return A deferred action that closes \c obj.
///
/// \note Can be used only in a seatstar thread.
template <typename Object>
requires closeable<Object>
inline auto deferred_close(Object& obj) {
    return close_controller(obj);
}

template <typename Object>
concept stoppable = requires (Object o) {
    { o.stop() } -> std::same_as<future<>>;
};

/// Template helper to stop \c obj when destroyed.
///
/// \tparam Object a class exposing a \c stop() method that returns a \c future<>
///         that is called when the controller is destroyed.
template <typename Object>
requires stoppable<Object>
class stop_controller {
    Object& _obj;
    bool _stopped = false;

    void do_stop() noexcept {
        if (!_stopped) {
            _stopped = true;
            _obj.stop().get();
        }
    }
public:
    stop_controller(Object& obj) noexcept : _obj(obj) {}
    ~stop_controller() {
        do_stop();
    }
    /// Stop \c obj once now.
    void stop_now() noexcept {
        assert(!_stopped);
        do_stop();
    }
};

/// Auto-stop an object.
///
/// \param obj object to auto-stop.
///
/// \return A deferred action that stops \c obj.
///
/// \note Can be used only in a seatstar thread.
template <typename Object>
requires stoppable<Object>
inline auto deferred_stop(Object& obj) {
    return stop_controller(obj);
}

} // namespace seastar
