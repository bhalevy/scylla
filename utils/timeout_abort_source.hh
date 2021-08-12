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

#include <seastar/core/abort_source.hh>
#include <seastar/core/timer.hh>

#include "db/timeout_clock.hh"

namespace utils {

class timeout_abort_source : public seastar::abort_source {
    seastar::timer<db::timeout_clock> _tmr;
public:
    timeout_abort_source(db::timeout_clock::time_point timeout) {
        if (timeout != db::no_timeout) {
            _tmr.set_callback([this] { this->request_abort_ex(std::make_exception_ptr(timed_out_error())); });
            _tmr.arm(timeout);
        }
    }
};

} // namespace utils
