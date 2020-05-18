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
#include <seastar/core/file.hh>

#include "seastarx.hh"

namespace utils {

/// \brief Helper for ensuring a file is closed if an exception is thrown.
///
/// The file provided by the file_fut future is passed to func.
/// * If func throws an exception E, the file is closed and we return
///   a failed future with E.
/// * If func returns a value V, the file is not closed and we return
///   a future with V.
/// Note that when an exception is not thrown, it is the
/// responsibility of func to make sure the file will be closed. It
/// can close the file itself, return it, or store it somewhere.
///
/// \param file_fut A future that produces a file
/// \param Func The type of function this wraps
/// \param func A function that uses a file
/// \return A future that passes the file produced by file_fut to func
///         and closes it if func fails
template <typename Func>
inline
auto close_file_on_failure(future<file> file_fut, Func func) {
    return file_fut.then([func = std::move(func)](file f) {
        return futurize_invoke(func, f).handle_exception([f] (std::exception_ptr e) mutable {
            return f.close().then_wrapped([f, e = std::move(e)] (future<> x) {
                using futurator = futurize<std::result_of_t<Func(file)>>;
                return futurator::make_exception_future(e);
            });
        });
    });
}

} // namespace utils

