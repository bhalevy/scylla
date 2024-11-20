/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <set>
#include <unordered_set>
#include <utility>

#include "utils/hashing.hh"

template<typename T>
class map_difference_reference_wrapper {
    const T* _ptr = nullptr;

public:
    map_difference_reference_wrapper() = default;
    explicit map_difference_reference_wrapper(const map_difference_reference_wrapper&) = default;
    explicit map_difference_reference_wrapper(map_difference_reference_wrapper&& o_ref) noexcept : _ptr(std::exchange(o_ref._ptr, nullptr)) {}
    explicit map_difference_reference_wrapper(const T& v) noexcept : _ptr(&v) {}

    map_difference_reference_wrapper& operator=(const map_difference_reference_wrapper&) = default;

    const T& get() const noexcept {
        return *_ptr;
    }

    operator T const&() const noexcept {
        return get();
    }

    const T& operator*() const noexcept {
        return get();
    }

    const T* operator->() const noexcept {
        return _ptr;
    }

    bool operator==(const T& o) const {
        return get() == o;
    }

    bool operator==(const map_difference_reference_wrapper& o_ref) const {
        return get() == o_ref.get();
    }

    auto operator<=>(const T& o) const {
        return get() <=> o;
    }

    auto operator<=>(const map_difference_reference_wrapper& o_ref) const {
        return get() <=> o_ref.get();
    }
};

namespace std {
template<typename T>
struct hash<map_difference_reference_wrapper<T>> : public std::hash<T> {
    size_t operator()(const map_difference_reference_wrapper<T>& x) const noexcept {
        return std::hash<T>::operator()(x.get());
    }
};
}

template <typename T>
struct fmt::formatter<map_difference_reference_wrapper<T>> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const map_difference_reference_wrapper<T>& x, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", x.get());
    }
};

template<typename Key, typename Set = std::set<map_difference_reference_wrapper<Key>>>
struct map_difference {
    using ref_type = map_difference_reference_wrapper<Key>;
    using set_type = Set;

    // Entries in left map whose keys don't exist in the right map.
    Set entries_only_on_left;

    // Entries in right map whose keys don't exist in the left map.
    Set entries_only_on_right;

    // Entries that appear in both maps with the same value.
    Set entries_in_common;

    // Entries that appear in both maps but have different values.
    Set entries_differing;

    map_difference()
        : entries_only_on_left{}
        , entries_only_on_right{}
        , entries_in_common{}
        , entries_differing{}
    { }
};

template<typename Key>
using unordered_map_difference = map_difference<Key, std::unordered_set<map_difference_reference_wrapper<Key>>>;

/**
 * Produces a map_difference between the two specified maps, with Key keys and
 * Tp values, using the provided equality function. In order to work with any
 * map type, such as std::map and std::unordered_map, Args holds the remaining
 * type parameters of the particular map type.
 */
template<template<typename...> class Map,
         typename Key,
         typename Set = std::set<map_difference_reference_wrapper<Key>>,
         typename Tp,
         typename Eq = std::equal_to<Tp>,
         typename... Args>
inline
map_difference<Key, Set>
difference(const Map<Key, Tp, Args...>& left,
           const Map<Key, Tp, Args...>& right,
           Eq equals = Eq())
{
    using map_difference_type = map_difference<Key, Set>;
    map_difference_type diff;
    for (const auto& [right_key, right_value] : right) {
        diff.entries_only_on_right.emplace(right_key);
    }
    for (const auto& [left_key, left_value] : left) {
        auto&& it = right.find(left_key);
        if (it != right.end()) {
            diff.entries_only_on_right.erase(typename map_difference_type::ref_type(left_key));
            const Tp& right_value = it->second;
            if (equals(left_value, right_value)) {
                diff.entries_in_common.emplace(left_key);
            } else {
                diff.entries_differing.emplace(left_key);
            }
        } else {
            diff.entries_only_on_left.emplace(left_key);
        }
    }
    return diff;
}

template<template<typename...> class Map,
         typename Key,
         typename UnorderedSet = std::unordered_set<map_difference_reference_wrapper<Key>>,
         typename Tp,
         typename Eq = std::equal_to<Tp>,
         typename... Args>
inline
map_difference<Key, UnorderedSet>
unordered_difference(const Map<Key, Tp, Args...>& left,
           const Map<Key, Tp, Args...>& right,
           Eq equals = Eq()) {
    return difference<Map, Key, UnorderedSet, Tp, Eq, Args...>(left, right, equals);
}
