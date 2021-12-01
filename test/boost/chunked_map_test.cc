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

#include <string>

#include <seastar/testing/thread_test_case.hh>
#include "utils/chunked_map.hh"

SEASTAR_THREAD_TEST_CASE(test_empty_chunked_unordered_map) {
    utils::chunked_unordered_map<int, std::string> m;

    BOOST_REQUIRE(m.empty());
    BOOST_REQUIRE_EQUAL(m.size(), 0);

    auto it = m.find(0);
    BOOST_REQUIRE(it == m.end());
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_insert) {
    utils::chunked_unordered_map<int, std::string> m;

    std::pair<const int, std::string> item = {1, "one"};
    auto [it, inserted] = m.insert(item);
    BOOST_REQUIRE(inserted);
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    BOOST_REQUIRE_EQUAL(it->first, 1);
    BOOST_REQUIRE_EQUAL(it->second, "one");

    it = m.find(0);
    BOOST_REQUIRE(it == m.end());

    it = m.find(1);
    BOOST_REQUIRE(it != m.end());
    BOOST_REQUIRE(it->value() == item);

    auto [it1, inserted1] = m.insert(item);
    BOOST_REQUIRE(!inserted1);
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    BOOST_REQUIRE(it1 == it);

    auto [it2, inserted2] = m.insert({2, "two"});
    BOOST_REQUIRE(inserted2);
    BOOST_REQUIRE_EQUAL(m.size(), 2);
    BOOST_REQUIRE_EQUAL(it2->first, 2);
    BOOST_REQUIRE_EQUAL(it2->second, "two");
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_emplace) {
    utils::chunked_unordered_map<int, std::string> m;

    auto [it, inserted] = m.emplace(1, "one");
    BOOST_REQUIRE(inserted);
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    BOOST_REQUIRE_EQUAL(it->first, 1);
    BOOST_REQUIRE_EQUAL(it->second, "one");

    it = m.find(0);
    BOOST_REQUIRE(it == m.end());

    auto [it1, inserted1] = m.emplace(1, "noone");
    BOOST_REQUIRE(!inserted1);
    BOOST_REQUIRE_EQUAL(m.size(), 1);
    BOOST_REQUIRE_EQUAL(it1->first, 1);
    BOOST_REQUIRE_EQUAL(it1->second, "one");

    auto [it2, inserted2] = m.emplace(2, "two");
    BOOST_REQUIRE(inserted2);
    BOOST_REQUIRE_EQUAL(m.size(), 2);
    BOOST_REQUIRE_EQUAL(it2->first, 2);
    BOOST_REQUIRE_EQUAL(it2->second, "two");
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_clear) {
    utils::chunked_unordered_map<int, std::string> m;

    m.clear();
    BOOST_REQUIRE_EQUAL(m.size(), 0);

    m.insert({3, "three"});
    m.insert({1, "one"});
    m.insert({2, "two"});

    m.clear();
    BOOST_REQUIRE_EQUAL(m.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_destroy_not_empty) {
    utils::chunked_unordered_map<int, std::string> m;
    m.insert({1, "one"});
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_erase) {
    utils::chunked_unordered_map<int, std::string> m;

    m.insert({3, "three"});
    m.insert({1, "one"});
    m.insert({2, "two"});

    BOOST_REQUIRE(!m.erase(0));
    BOOST_REQUIRE_EQUAL(m.size(), 3);

    BOOST_REQUIRE(m.erase(1));
    BOOST_REQUIRE_EQUAL(m.size(), 2);

    auto it = m.find(2);
    BOOST_REQUIRE(it != m.end());
    BOOST_REQUIRE_EQUAL(it->first, 2);
    BOOST_REQUIRE_EQUAL(it->second, "two");

    m.erase(it);
    BOOST_REQUIRE(m.size() == 1);

    it = m.erase(m.begin());
    BOOST_REQUIRE(it == m.end());
    BOOST_REQUIRE(m.empty());
    BOOST_REQUIRE(m.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_extract) {
    utils::chunked_unordered_map<int, std::string> m;

    m.insert({3, "three"});
    m.insert({1, "one"});
    m.insert({2, "two"});

    auto nh = m.extract(0);
    BOOST_REQUIRE(!nh);
    BOOST_REQUIRE(nh.empty());

    auto it = m.find(1);
    nh = m.extract(it);
    BOOST_REQUIRE(nh);
    BOOST_REQUIRE(!nh.empty());
    BOOST_REQUIRE_EQUAL(nh.key(), 1);
    BOOST_REQUIRE_EQUAL(nh.mapped(), "one");

    nh = m.extract(2);
    BOOST_REQUIRE(nh);
    BOOST_REQUIRE(!nh.empty());
    BOOST_REQUIRE_EQUAL(nh.key(), 2);
    BOOST_REQUIRE_EQUAL(nh.mapped(), "two");
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_contains) {
    utils::chunked_unordered_map<int, std::string> m;

    BOOST_REQUIRE(!m.contains(0));

    m.emplace(3, "three");
    m.emplace(1, "one");
    m.emplace(2, "two");

    BOOST_REQUIRE(m.contains(1));
    BOOST_REQUIRE(m.contains(2));
    BOOST_REQUIRE(m.contains(3));
    BOOST_REQUIRE(!m.contains(0));
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_erase_if) {
    using map_type = utils::chunked_unordered_map<int, std::string>;
    map_type m;

    m.insert({3, "three"});
    m.insert({1, "one"});
    m.insert({2, "two"});

    auto erased = std::erase_if(m, [] (const map_type::value_type& value) {
        return value.first > 1;
    });
    BOOST_REQUIRE_EQUAL(erased, 2);
    BOOST_REQUIRE_EQUAL(m.size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_unordered_map_swap) {
    using map_type = utils::chunked_unordered_map<int, std::string>;
    map_type m1, m2;

    m1.insert({3, "three"});
    m2.insert({1, "one"});
    m2.insert({2, "two"});

    std::swap(m1, m2);

    BOOST_REQUIRE_EQUAL(m1.size(), 2);

    BOOST_REQUIRE(m1.contains(1));
    BOOST_REQUIRE(m1.contains(2));
    BOOST_REQUIRE(!m1.contains(3));

    BOOST_REQUIRE_EQUAL(m2.size(), 1);
    BOOST_REQUIRE(!m2.contains(1));
    BOOST_REQUIRE(!m2.contains(2));
    BOOST_REQUIRE(m2.contains(3));
}
