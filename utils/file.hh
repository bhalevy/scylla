/*
 * Copyright (C) 2020 ScyllaDB
 *
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

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

future<bool> same_file(sstring path1, sstring path2) noexcept;
future<> idempotent_link_file(sstring oldpath, sstring newpath) noexcept;
future<> idempotent_rename_file(sstring oldpath, sstring newpath) noexcept;
future<> idempotent_remove_file(sstring path) noexcept;
