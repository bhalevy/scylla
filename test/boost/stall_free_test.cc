/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <seastar/testing/thread_test_case.hh>
#include "utils/stall_free.hh"

SEASTAR_THREAD_TEST_CASE(test_merge1) {
    std::list<int> l1{1, 2, 5, 8};
    std::list<int> l2{3};
    std::list<int> expected{1,2,3,5,8};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge2) {
    std::list<int> l1{1};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{1,3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge3) {
    std::list<int> l1{};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge4) {
    std::list<int> l1{1};
    std::list<int> l2{};
    std::list<int> expected{1};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_vector) {
    std::vector<int> v;
    int count = 100;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(i);
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_non_trivial_vector) {
    struct X {
        std::unique_ptr<int> v;
        X(int val) : v(std::make_unique<int>(val)) {}
    };
    std::vector<X> v;
    int count = 100;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_map) {
    std::unordered_map<int, sstring> c;
    int count = 100;

    for (int i = 0; i < count; i++) {
        c.insert(std::pair<int, sstring>(i, format("{}", i)));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_vector) {
    struct X {
        std::unique_ptr<int> v;
        X(int val) : v(std::make_unique<int>(val)) {}
    };
    std::vector<std::vector<X>> c;
    int top_count = 10;
    int count = 10;

    c.reserve(top_count);
    for (int i = 0; i < top_count; i++) {
        std::vector<X> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(X(j));
        }
        c.emplace_back(std::move(v));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_object) {
    struct X {
        std::unique_ptr<int> v;
        X(int val) : v(std::make_unique<int>(val)) {}
        future<> clear_gently() {
            v.reset();
            return make_ready_future<>();
        }
    };
    std::vector<X> v;
    int count = 100;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_unordered_map) {
    struct X {
        std::unique_ptr<int> v;
        X(int val) : v(std::make_unique<int>(val)) {}
    };
    std::unordered_map<int, std::vector<X>> c;
    int top_count = 10;
    int count = 10;

    for (int i = 0; i < top_count; i++) {
        std::vector<X> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(X(j));
        }
        c.insert(std::pair<int, std::vector<X>>(i, std::move(v)));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_container) {
    struct X {
        std::unique_ptr<int> v;
        X(int val) : v(std::make_unique<int>(val)) {}
    };
    std::list<std::vector<X>> c;
    int top_count = 10;
    int count = 10;

    for (int i = 0; i < top_count; i++) {
        std::vector<X> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(X(j));
        }
        c.push_back(std::move(v));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_multi_nesting) {
    struct X {
        std::unique_ptr<int> v;
        X(int val) : v(std::make_unique<int>(val)) {}
        future<> clear_gently() {
            v.reset();
            return make_ready_future<>();
        }
    };
    struct V {
        std::vector<X> v;
        V(int count) {
            v.reserve(count);
            for (int i = 0; i < count; i++) {
                v.emplace_back(X(i));
            }
        }
        future<> clear_gently() {
            return utils::clear_gently(v);
        }
    };
    std::vector<std::map<int, V>> c;
    int top_count = 10;
    int mid_count = 10;
    int count = 10;

    for (int i = 0; i < top_count; i++) {
        std::map<int, V> m;
        for (int j = 0; j < mid_count; j++) {
            m.insert(std::pair<int, V>(j, V(count)));
        }
        c.push_back(std::move(m));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}
