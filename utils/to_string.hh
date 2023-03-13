/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <string>

#include "seastarx.hh"

#include <boost/test/utils/basic_cstring/basic_cstring_fwd.hpp>

namespace utils {

template <typename Iterator, typename Sentinel>
requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
static inline
std::ostream& join(std::ostream& os, sstring delimiter, Iterator begin, Sentinel end) {
    while (begin != end) {
        os << *begin;
        ++begin;
        if (begin != end) {
            os << delimiter;
        }
    }
    return os;
}

template<std::ranges::range Range>
static inline
std::ostream& join(std::ostream& os, sstring delimiter, const Range& items) {
    return join(os, std::move(delimiter), items.begin(), items.end());
}

template <typename Iterator, typename Sentinel>
requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
static inline
sstring join(sstring delimiter, Iterator begin, Sentinel end) {
    std::ostringstream oss;
    join(oss, delimiter, std::move(begin), std::move(end));
    return oss.str();
}

template<typename PrintableRange>
static inline
sstring join(sstring delimiter, const PrintableRange& items) {
    std::ostringstream oss;
    join(delimiter, items);
    return oss.str();
}

template <typename Iterator, typename Sentinel>
requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
static inline
std::ostream& join(std::ostream& os, sstring open, sstring delimiter, sstring close, Iterator begin, Sentinel end) {
    os << open;
    while (begin != end) {
        os << *begin;
        ++begin;
        if (begin != end) {
            os << delimiter;
        }
    }
    os << close;
    return os;
}

template <std::ranges::range Range>
static inline
std::ostream& join(std::ostream& os, sstring open, sstring delimiter, sstring close, const Range& items) {
    return join(os, std::move(open), std::move(delimiter), std::move(close), items.begin(), items.end());
}

namespace internal {

template<bool NeedsComma, typename Printable>
struct print_with_comma {
    const Printable& v;
};

template<bool NeedsComma, typename Printable>
std::ostream& operator<<(std::ostream& os, const print_with_comma<NeedsComma, Printable>& x) {
    os << x.v;
    if (NeedsComma) {
        os << ", ";
    }
    return os;
}

} // namespace internal

} // namespace utils

namespace std {

template <std::ranges::range Range>
sstring
to_string(const Range& items) {
    return "{" + utils::join(", ", items) + "}";
}

template<typename Printable>
static inline
sstring
to_string(std::initializer_list<Printable> items) {
    return "[" + utils::join(", ", std::begin(items), std::end(items)) + "]";
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& os, const std::pair<K, V>& p) {
    os << "{" << p.first << ", " << p.second << "}";
    return os;
}

template<typename... T, size_t... I>
std::ostream& print_tuple(std::ostream& os, const std::tuple<T...>& p, std::index_sequence<I...>) {
    return ((os << "{" ) << ... << utils::internal::print_with_comma<I < sizeof...(I) - 1, T>{std::get<I>(p)}) << "}";
}

template <typename... T>
std::ostream& operator<<(std::ostream& os, const std::tuple<T...>& p) {
    return print_tuple(os, p, std::make_index_sequence<sizeof...(T)>());
}

// Exclude string-like types to avoid printing them as vector of chars
template <std::ranges::range Range>
requires (
       !std::convertible_to<Range, std::string>
    && !std::convertible_to<Range, std::string_view>
    && !std::same_as<Range, boost::unit_test::basic_cstring<const char>>
)
std::ostream& operator<<(std::ostream& os, const Range& items) {
    return utils::join(os, "{", ", ", "}", items);
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        os << "{" << *opt << "}";
    } else {
        os << "{}";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const std::strong_ordering& order);
std::ostream& operator<<(std::ostream& os, const std::weak_ordering& order);
std::ostream& operator<<(std::ostream& os, const std::partial_ordering& order);

} // namespace std
