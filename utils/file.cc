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

#include <filesystem>

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>

#include "file.hh"
#include "seastarx.hh"

namespace fs = std::filesystem;

static bool is_same_file(const stat_data& sd1, const stat_data& sd2) noexcept {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

static future<std::tuple<stat_data, stat_data>> stat_files(sstring path1, sstring path2) noexcept {
    return when_all_succeed(file_stat(std::move(path1), follow_symlink::no),
                            file_stat(std::move(path2), follow_symlink::no));
}

future<bool> same_file(sstring path1, sstring path2) noexcept {
    return stat_files(std::move(path1), std::move(path2)).then([] (std::tuple<stat_data, stat_data> r) {
        return make_ready_future<bool>(is_same_file(std::get<0>(r), std::get<1>(r)));
    });
}

future<> idempotent_link_file(sstring oldpath, sstring newpath) noexcept {
    if (oldpath.empty() || newpath.empty()) {
        return make_exception_future<>(fs::filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), make_error_code(std::errc::invalid_argument)));
    }
    return do_with(std::move(oldpath), std::move(newpath), [] (const sstring& oldpath, const sstring& newpath) {
        return link_file(oldpath, newpath).handle_exception_type([&] (const std::system_error& ex) mutable {
            if (ex.code().value() != EEXIST) {
                return make_exception_future<>(ex);
            }
            return same_file(oldpath, newpath).then_wrapped([&, eptr = std::make_exception_ptr(ex)] (future<bool> fut) mutable {
                if (fut.failed()) {
                    try {
                        std::rethrow_exception(fut.get_exception());
                    } catch (std::system_error& stat_exception) {
                        if (stat_exception.code().value() == ENOENT) {
                            // May be called from sstable::create_links concurrently to the destination file being removed.
                            // Retry using recursion, as it should either fail differently or eventually succeed.
                            return idempotent_link_file(oldpath, newpath);
                        }
                    } catch (...) {
                        /* propagate */
                    }
                    return make_exception_future<>(eptr);
                }
                auto same = fut.get0();
                if (same) {
                    return make_ready_future<>();
                } else {
                    return make_exception_future<>(eptr);
                }
            });
        });
    });
}

future<> idempotent_rename_file(sstring oldpath, sstring newpath) noexcept {
    if (oldpath.empty() || newpath.empty()) {
        return make_exception_future<>(fs::filesystem_error("rename failed", fs::path(oldpath), fs::path(newpath), make_error_code(std::errc::invalid_argument)));
    }
    return do_with(std::move(oldpath), std::move(newpath), [] (sstring& oldpath, sstring& newpath) {
        return when_all(file_stat(oldpath, follow_symlink::no),
                        file_stat(newpath, follow_symlink::no))
                    .then([&] (std::tuple<future<stat_data>, future<stat_data>> t) mutable {
            auto& f1 = std::get<0>(t);
            auto& f2 = std::get<1>(t);
            if (f1.failed()) {
                f2.ignore_ready_future();
                try {
                    std::rethrow_exception(f1.get_exception());
                } catch (const std::system_error& e) {
                    throw fs::filesystem_error("rename failed", fs::path(oldpath), fs::path(newpath), e.code());
                }
            }
            auto sd1 = f1.get0();
            if (f2.failed()) {
                try {
                    std::rethrow_exception(f2.get_exception());
                } catch (const std::system_error& e) {
                    if (e.code().value() != ENOENT) {
                        throw fs::filesystem_error("rename failed", fs::path(oldpath), fs::path(newpath), e.code());
                    }
                }
            } else {
                auto sd2 = f2.get0();
                if (is_same_file(sd1, sd2)) {
                    return idempotent_remove_file(oldpath).handle_exception_type([&] (const std::system_error& e) {
                        throw fs::filesystem_error("rename failed", fs::path(oldpath), fs::path(newpath), e.code());
                    });
                }
            }
            // This could be running in parallel, e.g. on another thread, so rename might fail to find src
            return rename_file(oldpath, newpath).handle_exception_type([&, sd1 = std::move(sd1)] (const std::system_error& e) mutable {
                if (e.code().value() != ENOENT) {
                    return make_exception_future<>(e);
                }
                return file_stat(newpath).then([sd1 = std::move(sd1), eptr = std::make_exception_ptr(e)] (stat_data sd2) {
                    if (is_same_file(sd1, sd2)) {
                        return make_ready_future<>();
                    }
                    return make_exception_future<>(eptr);
                });
            });
        });
    });
}

future<> idempotent_remove_file(sstring path) noexcept {
    return remove_file(std::move(path)).handle_exception_type([] (const std::system_error& e) {
        if (e.code().value() == ENOENT) {
            return make_ready_future<>();
        }
        return make_exception_future<>(e);
    });
}
