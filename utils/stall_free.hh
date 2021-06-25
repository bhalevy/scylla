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

#pragma once

#include <list>
#include <algorithm>
#include <seastar/core/thread.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include "utils/collection-concepts.hh"

using namespace seastar;

namespace utils {


// Similar to std::merge but it does not stall. Must run inside a seastar
// thread. It merges items from list2 into list1. Items from list2 can only be copied.
template<class T, class Compare>
requires LessComparable<T, T, Compare>
void merge_to_gently(std::list<T>& list1, const std::list<T>& list2, Compare comp) {
    auto first1 = list1.begin();
    auto first2 = list2.begin();
    auto last1 = list1.end();
    auto last2 = list2.end();
    while (first2 != last2) {
        seastar::thread::maybe_yield();
        if (first1 == last1) {
            // Copy remaining items of list2 into list1
            std::copy_if(first2, last2, std::back_inserter(list1), [] (const auto&) { return true; });
            return;
        }
        if (comp(*first2, *first1)) {
            first1 = list1.insert(first1, *first2);
            ++first2;
        } else {
            ++first1;
        }
    }
}

template <typename T>
requires requires (T o) {
    { o.clear_gently() } -> std::same_as<future<>>;
}
future<> maybe_clear_gently(T& o) {
    return o.clear_gently();
}

template <typename T>
requires std::is_trivially_destructible_v<T>
future<> maybe_clear_gently(T&) noexcept {
    return make_ready_future<>();
}

template <typename Container>
seastar::future<> clear_gently(Container& c) noexcept;

template <typename T>
requires requires (T o) {
    { o.erase(o.begin()) } -> std::same_as<typename T::iterator>;
}
future<> maybe_clear_gently(T& o) {
    return clear_gently(o);
}

template <typename T>
future<> maybe_clear_gently(T&) noexcept {
    return make_ready_future<>();
}

// Trivially destructible elements can be safely cleared in bulk
template <typename T>
requires std::is_trivially_destructible_v<T>
future<> clear_gently(std::vector<T>& v) noexcept {
    v.clear();
    return make_ready_future<>();
}

// Clear the elements gently and destroy them one-by-one
// in reverse order, to avoid copying.
template <typename T>
future<> clear_gently(std::vector<T>& v) noexcept {
    return do_until([&v] { return v.empty(); }, [&v] {
        auto it = v.end();
        --it;
        return maybe_clear_gently(*it).finally([&v, it = std::move(it)] {
            v.erase(it);
        });
    });
}

template <typename K, typename T>
future<> clear_gently(std::unordered_map<K, T>& c) noexcept {
    return do_until([&c] { return c.empty(); }, [&c] {
        auto it = c.begin();
        return maybe_clear_gently(it->second).finally([&c, it = std::move(it)] {
            c.erase(it);
        });
    });
}

template <typename Container>
seastar::future<> clear_gently(Container& c) noexcept {
    return do_until([&c] { return c.empty(); }, [&c] {
        auto it = c.begin();
        return maybe_clear_gently(*it).finally([&c, it = std::move(it)] {
            c.erase(it);
        });
    });
}

}

