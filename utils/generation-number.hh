/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <cstdint>
#include <compare>
#include <iostream>
#include <type_traits>

namespace utils {

template <typename Tag, typename ValueType>
requires std::is_integral_v<ValueType>
class tagged_generation_type {
public:
    using value_type = ValueType;
private:
    value_type _value;
public:
    tagged_generation_type() noexcept : _value(0) {}
    explicit tagged_generation_type(value_type v) noexcept : _value(v) {}

    tagged_generation_type& operator=(value_type v) noexcept {
        _value = v;
        return *this;
    }

    constexpr std::weak_ordering operator<=>(const tagged_generation_type& o) const = default;

    tagged_generation_type& operator++() noexcept {
        ++_value;
        return *this;
    }

    tagged_generation_type operator++(int) noexcept {
        auto ret = *this;
        ++_value;
        return ret;
    }

    explicit operator value_type() const noexcept { return _value; }
    constexpr value_type value() const noexcept { return _value; }
};

using generation_type = tagged_generation_type<struct generation_type_tag, int>;

// Based on seconds-since-epoch, which isn't a foolproof new generation
// (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
generation_type get_generation_number() noexcept;

} // namespace utils

namespace std {

template <typename Tag, typename ValueType>
struct hash<utils::tagged_generation_type<Tag, ValueType>> {
    size_t operator()(const utils::tagged_generation_type<Tag, ValueType>& x) const noexcept {
        return hash<int64_t>{}(x.value());
    }
};

template <typename Tag, typename ValueType>
struct numeric_limits<utils::tagged_generation_type<Tag, ValueType>> {
    static constexpr utils::tagged_generation_type<Tag, ValueType> min() noexcept {
        return utils::tagged_generation_type<Tag, ValueType>(numeric_limits<ValueType>::min());
    }
    static constexpr utils::tagged_generation_type<Tag, ValueType> max() noexcept {
        return utils::tagged_generation_type<Tag, ValueType>(numeric_limits<ValueType>::max());
    }
};

template <typename Tag, typename ValueType>
[[maybe_unused]] static ostream& operator<<(ostream& s, const utils::tagged_generation_type<Tag, ValueType>& x) {
    return s << x.value();
}

} // namespace std
