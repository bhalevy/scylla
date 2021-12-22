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

#include <unordered_set>
#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/tmpdir.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/make_random_string.hh"

#include "lister.hh"

class expected_exception : public std::exception {
public:
    virtual const char* what() const noexcept override {
        return "expected_exception";
    }
};

static future<> touch_file(fs::path filename, open_flags flags = open_flags::wo | open_flags::create, file_permissions create_permissions = file_permissions::default_file_permissions) {
    file_open_options opts;
    opts.create_permissions = create_permissions;
    auto f = co_await open_file_dma(filename.native(), flags, opts);
    co_await f.close();
}

SEASTAR_TEST_CASE(test_empty_queue_lister) {
    auto tmp = tmpdir();
    auto ql = queue_lister(tmp.path());
    size_t count = 0;

    while (auto de = co_await ql.get()) {
        count++;
    }

    BOOST_REQUIRE(!count);
}

static future<size_t> generate_random_content(tmpdir& tmp, std::unordered_set<std::string>& file_names, std::unordered_set<std::string>& dir_names, size_t max_count = 1000) {
    size_t count = tests::random::get_int<size_t>(0, max_count);
    for (size_t i = 0; i < count; i++) {
        auto name = tests::random::get_sstring(tests::random::get_int(1, 8));
        if (tests::random::get_bool()) {
            if (!file_names.contains(name)) {
                dir_names.insert(name);
            }
        } else {
            if (!dir_names.contains(name)) {
                file_names.insert(name);
            }
        }
    }

    for (const auto& name : file_names) {
        co_await touch_file(tmp.path() / name);
    }
    for (const auto& name : dir_names) {
        co_await touch_directory((tmp.path() / name).native());
    }

    co_return file_names.size() + dir_names.size();
}

SEASTAR_TEST_CASE(test_queue_lister) {
    auto tmp = tmpdir();

    std::unordered_set<std::string> file_names;
    std::unordered_set<std::string> dir_names;

    auto count = co_await generate_random_content(tmp, file_names, dir_names);
    BOOST_TEST_MESSAGE(fmt::format("Generated {} dir entries", count));

    std::unordered_set<std::string> found_file_names;
    std::unordered_set<std::string> found_dir_names;

    auto ql = queue_lister(tmp.path());

    while (auto de = co_await ql.get()) {
        assert(de->type);
        switch (*de->type) {
        case directory_entry_type::regular: {
            auto [it, inserted] = found_file_names.insert(de->name);
            assert(inserted);
            break;
        }
        case directory_entry_type::directory: {
            auto [it, inserted] = found_dir_names.insert(de->name);
            assert(inserted);
            break;
        }
        default:
            BOOST_FAIL(fmt::format("Unexpected directory_entry_type: {}", *de->type));
        }
    }

    BOOST_REQUIRE(found_file_names == file_names);
    BOOST_REQUIRE(found_dir_names == dir_names);
}

SEASTAR_TEST_CASE(test_queue_close) {
    auto tmp = tmpdir();

    std::unordered_set<std::string> file_names;
    std::unordered_set<std::string> dir_names;

    auto count = co_await generate_random_content(tmp, file_names, dir_names, tests::random::get_int(100, 1000));
    BOOST_TEST_MESSAGE(fmt::format("Generated {} dir entries", count));

    {
        auto ql = queue_lister(tmp.path());
        auto initial = tests::random::get_int(count);
        BOOST_TEST_MESSAGE(fmt::format("Getting {} dir entries", initial));
        for (auto i = 0; i < initial; i++) {
            auto de = co_await ql.get();
            assert(de);
        }
        BOOST_TEST_MESSAGE("Closing queue_lister");
        co_await ql.close();
    }

    {
        auto ql = queue_lister(tmp.path());
        auto initial = tests::random::get_int(count);
        BOOST_TEST_MESSAGE(fmt::format("Getting {} dir entries", initial));
        for (auto i = 0; i < initial; i++) {
            auto de = co_await ql.get();
            assert(de);
        }
        BOOST_TEST_MESSAGE("Closing queue_lister");
        co_await ql.close();
        BOOST_REQUIRE_THROW(co_await ql.get(), std::exception);
    }
}

SEASTAR_TEST_CASE(test_queue_abort) {
    auto tmp = tmpdir();

    std::unordered_set<std::string> file_names;
    std::unordered_set<std::string> dir_names;

    auto count = co_await generate_random_content(tmp, file_names, dir_names, tests::random::get_int(100, 1000));
    BOOST_TEST_MESSAGE(fmt::format("Generated {} dir entries", count));

    auto ql = queue_lister(tmp.path());

    auto initial = tests::random::get_int(count);
    BOOST_TEST_MESSAGE(fmt::format("Getting {} dir entries", initial));
    for (auto i = 0; i < initial; i++) {
        auto de = co_await ql.get();
        assert(de);
    }
    BOOST_TEST_MESSAGE("Aborting queue_lister");
    ql.abort(std::make_exception_ptr(expected_exception()));
    BOOST_REQUIRE_THROW(co_await ql.get(), expected_exception);

    BOOST_TEST_MESSAGE("Closing queue_lister");
    co_await ql.close();
}
