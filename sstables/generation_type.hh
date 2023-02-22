/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/core.h>
#include <cstdint>
#include <compare>
#include <limits>
#include <iostream>
#include <boost/range/adaptors.hpp>
#include <seastar/core/sstring.hh>

namespace sstables {

class generation_type {
public:
    using int_t = int64_t;

private:
    int_t _value;

public:
    generation_type() = delete;

    explicit constexpr generation_type(int_t value) noexcept: _value(value) {}
    constexpr int_t value() const noexcept { return _value; }

    constexpr bool operator==(const generation_type& other) const noexcept { return _value == other._value; }
    constexpr std::strong_ordering operator<=>(const generation_type& other) const noexcept { return _value <=> other._value; }
};

constexpr generation_type generation_from_value(generation_type::int_t value) {
    return generation_type{value};
}
constexpr generation_type::int_t generation_value(generation_type generation) {
    return generation.value();
}

template <std::ranges::range Range, typename Target = std::vector<sstables::generation_type>>
Target generations_from_values(const Range& values) {
    return boost::copy_range<Target>(values | boost::adaptors::transformed([] (auto value) {
        return generation_type(value);
    }));
}

template <typename Target = std::vector<sstables::generation_type>>
Target generations_from_values(std::initializer_list<generation_type::int_t> values) {
    return boost::copy_range<Target>(values | boost::adaptors::transformed([] (auto value) {
        return generation_type(value);
    }));
}

inline generation_type new_generation(std::optional<generation_type> prev = std::nullopt) {
    if (!prev) {
        return generation_type(1);
    } else {
        return generation_type(prev->value() + 1);
    }
}

class generation_factory {
    std::optional<generation_type> _next;
    std::optional<generation_type> _prev;
public:
    generation_factory(generation_type gen = new_generation()) noexcept : _next(gen) {}

    generation_factory(const generation_factory&) = delete;
    generation_factory(generation_factory&&) = delete;

    generation_type prev() const noexcept {
        return *_prev;
    }

    generation_type operator()() noexcept {
        _prev = _next;
        _next = new_generation(_next);
        return *_prev;
    }
};

} //namespace sstables

namespace std {
template <>
struct hash<sstables::generation_type> {
    size_t operator()(const sstables::generation_type& generation) const noexcept {
        return hash<sstables::generation_type::int_t>{}(generation.value());
    }
};

// for min_max_tracker
template <>
struct numeric_limits<sstables::generation_type> : public numeric_limits<sstables::generation_type::int_t> {
    static constexpr sstables::generation_type min() noexcept {
        return sstables::generation_type{numeric_limits<sstables::generation_type::int_t>::min()};
    }
    static constexpr sstables::generation_type max() noexcept {
        return sstables::generation_type{numeric_limits<sstables::generation_type::int_t>::max()};
    }
};
} //namespace std

template <>
struct fmt::formatter<sstables::generation_type> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const sstables::generation_type& generation, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", generation.value());
    }
};
