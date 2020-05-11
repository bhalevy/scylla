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

#include <boost/test/unit_test.hpp>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/testing/test_case.hh>
#include "seastarx.hh"

#include "test/lib/exception_utils.hh"
#include "test/lib/tmpdir.hh"
#include "utils/file.hh"

static future<> touch_file(const sstring& filename, open_flags oflags = open_flags::rw | open_flags::create) noexcept {
    return open_file_dma(filename, oflags).then([] (file f) {
        return f.close().finally([f] {});
    });
}

static future<> overwrite_link(sstring oldpath, sstring newpath) {
    return file_exists(newpath).then([oldpath, newpath] (bool exists) {
        future<> fut = make_ready_future<>();
        if (exists) {
            fut = remove_file(newpath);
        }
        return fut.then([oldpath, newpath] {
            return link_file(oldpath, newpath);
        });
    });
}

SEASTAR_TEST_CASE(test_same_file) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        auto& p = t.get_path();
        sstring f1 = (p / "testfile1.tmp").native();
        sstring f2 = (p / "testfile2.tmp").native();

        // same_file should fail when both f1 and f2 do not exist
        BOOST_REQUIRE_EXCEPTION(same_file(f1, f2).get0(), std::system_error,
                exception_predicate::message_contains("stat failed: No such file or directory"));

        // f1 is same file as itself
        touch_file(f1).get();
        BOOST_REQUIRE(same_file(f1, f1).get0());

        // same_file should fail when either f1 or f2 do not exist
        BOOST_REQUIRE_EXCEPTION(same_file(f1, f2).get0(), std::system_error,
                exception_predicate::message_contains("stat failed: No such file or directory"));
        BOOST_REQUIRE_EXCEPTION(same_file(f2, f1).get0(), std::system_error,
                exception_predicate::message_contains("stat failed: No such file or directory"));

        // f1 is not same file as a newly created f2
        touch_file(f2).get();
        BOOST_REQUIRE(!same_file(f1, f2).get0());

        // f1 and f2 refer to same file when hard-linked
        overwrite_link(f1, f2).get();
        BOOST_REQUIRE(same_file(f1, f2).get0());
    });
}

SEASTAR_TEST_CASE(test_idempotent_link_file) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        auto& p = t.get_path();
        sstring f1 = (p / "testfile1.tmp").native();
        sstring f2 = (p / "testfile2.tmp").native();

        // idempotent_link_file should fail when both f1 and f2 do not exist
        BOOST_REQUIRE_EXCEPTION(idempotent_link_file(f1, f2).get0(), std::system_error,
                exception_predicate::message_contains("link failed: No such file or directory"));

        // idempotent_link_file should succeed in the trivial case when f1 exists f2 does not exist
        touch_file(f1).get();
        BOOST_REQUIRE_NO_THROW(idempotent_link_file(f1, f2).get());
        BOOST_REQUIRE(same_file(f1, f2).get0());

        // idempotent_link_file should succeed when f2 exists and same file
        BOOST_REQUIRE_NO_THROW(idempotent_link_file(f1, f2).get0());
        BOOST_REQUIRE(same_file(f1, f2).get0());

        // idempotent_link_file should fail when f2 exists and is different from f1
        remove_file(f2).get();
        touch_file(f2).get();
        BOOST_REQUIRE(!same_file(f1, f2).get0());
        BOOST_REQUIRE_EXCEPTION(idempotent_link_file(f1, f2).get0(), std::system_error,
                exception_predicate::message_contains("link failed: File exists"));
        BOOST_REQUIRE(!same_file(f1, f2).get0());

        // idempotent_link_file should fail when f1 does not exist
        remove_file(f1).get();
        BOOST_REQUIRE_EXCEPTION(idempotent_link_file(f1, f2).get0(), std::system_error,
                exception_predicate::message_contains("link failed: No such file or directory"));

        // test idempotent_link_file EACCESS
        BOOST_REQUIRE(!file_exists(f1).get0());
        chmod(p.native(), file_permissions::user_read | file_permissions::user_execute).get();
        BOOST_REQUIRE_EXCEPTION(idempotent_link_file(f2, f1).get(), std::system_error,
                exception_predicate::message_contains("link failed: Permission denied"));
        BOOST_REQUIRE(!file_exists(f1).get0());
        BOOST_REQUIRE(file_exists(f2).get0());
        chmod(p.native(), file_permissions::default_dir_permissions).get();
    });
}

SEASTAR_TEST_CASE(test_idempotent_rename_file) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        auto& p = t.get_path();
        sstring f1 = (p / "testfile1.tmp").native();
        sstring f2 = (p / "testfile2.tmp").native();
        sstring f3 = (p / "testfile3.tmp").native();

        // idempotent_rename_file should fail when both f1 and f2 do not exist
        BOOST_REQUIRE_EXCEPTION(idempotent_rename_file(f1, f2).get0(), std::system_error,
                exception_predicate::message_contains("rename failed: No such file or directory"));

        // idempotent_rename_file should succeed in the trivial case when f1 exists and f2 does not exist
        touch_file(f1).get();
        link_file(f1, f3).get();
        BOOST_REQUIRE_NO_THROW(idempotent_rename_file(f1, f2).get());
        BOOST_REQUIRE(!file_exists(f1).get0());
        BOOST_REQUIRE(same_file(f2, f3).get0());

        // rename(2): If newpath already exists, it will be atomically replaced
        touch_file(f1).get();
        overwrite_link(f1, f3).get();
        BOOST_REQUIRE(!same_file(f1, f2).get0());
        BOOST_REQUIRE_NO_THROW(idempotent_rename_file(f1, f2).get());
        BOOST_REQUIRE(!file_exists(f1).get0());
        BOOST_REQUIRE(same_file(f2, f3).get0());

        // idempotent_rename_file should succeed if oldpath and newpath are existing hard links referring to the same file
        touch_file(f1).get();
        overwrite_link(f1, f2).get();
        overwrite_link(f2, f3).get();
        BOOST_REQUIRE_NO_THROW(idempotent_rename_file(f1, f2).get());
        BOOST_REQUIRE(!file_exists(f1).get0());
        BOOST_REQUIRE(same_file(f2, f3).get0());

        // test idempotent_rename_file EACCESS
        BOOST_REQUIRE(!file_exists(f1).get0());
        chmod(p.native(), file_permissions::user_read | file_permissions::user_execute).get();
        BOOST_REQUIRE_EXCEPTION(idempotent_rename_file(f2, f1).get(), std::system_error,
                exception_predicate::message_contains("rename failed: Permission denied"));
        BOOST_REQUIRE(!file_exists(f1).get0());
        BOOST_REQUIRE(file_exists(f2).get0());
        BOOST_REQUIRE(same_file(f2, f3).get0());
        chmod(p.native(), file_permissions::default_dir_permissions).get();
    });
}

SEASTAR_TEST_CASE(test_idempotent_remove_file) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        auto& p = t.get_path();
        sstring f1 = (p / "testfile1.tmp").native();

        touch_file(f1).get();

        // idempotent_remove_file should succeed in the trivial case when f1 exists
        touch_file(f1).get();
        BOOST_REQUIRE(file_exists(f1).get0());
        BOOST_REQUIRE_NO_THROW(idempotent_remove_file(f1).get());
        BOOST_REQUIRE(!file_exists(f1).get0());

        // remove_file should fail when f1 does not exist
        BOOST_REQUIRE_EXCEPTION(remove_file(f1).get(), std::system_error,
                exception_predicate::message_contains("remove failed: No such file or directory"));

        // idempotent_remove_file should succeed when f1 does not exist
        BOOST_REQUIRE_NO_THROW(idempotent_remove_file(f1).get());

        // test idempotent_remove_file EACCESS
        touch_file(f1).get();
        chmod(p.native(), file_permissions::user_read | file_permissions::user_execute).get();
        BOOST_REQUIRE_EXCEPTION(idempotent_remove_file(f1).get(), std::system_error,
                exception_predicate::message_contains("remove failed: Permission denied"));
        BOOST_REQUIRE(file_exists(f1).get0());
        chmod(p.native(), file_permissions::default_dir_permissions).get();
    });
}
