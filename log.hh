/*
 * Copyright (C) 2014-present ScyllaDB
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

#include <seastar/util/log.hh>
#include <seastar/core/sstring.hh>

using namespace seastar;

namespace logging {

//
// Seastar changed the names of some of these types. Maintain the old names here to avoid too much churn.
//

using log_level = seastar::log_level;
using logger = seastar::logger;
using registry = seastar::logger_registry;

inline registry& logger_registry() noexcept {
    return seastar::global_logger_registry();
}

using settings = seastar::logging_settings;

inline void apply_settings(const settings& s) {
    seastar::apply_logging_settings(s);
}

using seastar::pretty_type_name;
using seastar::level_name;

} // namespace logging

template <typename... Args>
sstring log_format(logger& l, log_level level, const char* fmt, Args... args) {
    if (l.is_enabled(level)) {
        return format(fmt, std::forward<Args>(args)...);
    } else {
        return "";
    }
}

template <typename... Args>
sstring debug_format(logger& l, const char* fmt, Args... args) {
    return log_format(l, log_level::debug, fmt, std::forward<Args>(args)...);
}
