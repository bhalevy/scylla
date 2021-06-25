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
concept GentlyClearable = requires (T x) {
    { x.clear_gently() } -> std::same_as<future<>>;
};

template <typename T>
concept SmartPointer = requires (T x) {
    { x.reset() } -> std::same_as<void>;
    { x.get() } -> std::same_as<typename T::element_type*>;
};

template <typename T>
concept VectorLike = requires (T x, size_t n) {
    { x.empty() } -> std::same_as<bool>;
    { x.back() } -> std::same_as<typename T::value_type&>;
    { x.pop_back() } -> std::same_as<void>;
};

template <typename T>
concept Container = !VectorLike<T> && requires (T x) {
    { x.empty() } -> std::same_as<bool>;
    { x.erase(x.begin()) } -> std::same_as<typename T::iterator>;
};

template <typename T, std::size_t N>
future<> clear_gently(std::array<T, N>&a) noexcept;

template <Container T>
future<> clear_gently(T& c) noexcept;

template <GentlyClearable T>
future<> clear_gently(T& o) noexcept(noexcept(o.clear_gently())) {
    return o.clear_gently();
}

template <typename T>
future<> clear_gently(T&) noexcept {
    return make_ready_future<>();
}

template <SmartPointer Ptr>
future<> clear_gently(Ptr& o) {
    return clear_gently(*o).finally([&o] {
        return o.reset();
    });
}

template <typename T, std::size_t N>
future<> clear_gently(std::array<T, N>&a) noexcept {
    return do_for_each(a, [] (T& o) {
        return clear_gently(o);
    });
}

// Trivially destructible elements can be safely cleared in bulk
template <VectorLike T>
requires std::is_trivially_destructible_v<typename T::value_type>
future<> clear_gently(T& v) noexcept {
    v.clear();
    return make_ready_future<>();
}

// Clear the elements gently and destroy them one-by-one
// in reverse order, to avoid copying.
template <VectorLike T>
requires (!std::is_trivially_destructible_v<typename T::value_type>)
future<> clear_gently(T& v) noexcept {
    return do_until([&v] { return v.empty(); }, [&v] {
        return clear_gently(v.back()).finally([&v] {
            v.pop_back();
        });
    });
    return make_ready_future<>();
}

template <typename K, typename T>
future<> clear_gently(std::pair<const K, T>& p) {
    return clear_gently(p.second);
}

template <Container T>
future<> clear_gently(T& c) noexcept {
    return do_until([&c] { return c.empty(); }, [&c] {
        auto it = c.begin();
        return clear_gently(*it).finally([&c, it = std::move(it)] () mutable {
            c.erase(it);
        });
    });
}

}

