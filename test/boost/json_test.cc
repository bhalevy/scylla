
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE json

#include <array>
#include <vector>
#include <list>
#include <deque>
#include <set>
#include <unordered_set>
#include <map>
#include <unordered_map>

#include <fmt/format.h>

#include <boost/test/unit_test.hpp>
#include <boost/range/adaptor/map.hpp>

#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>

#include "utils/rjson.hh"
#include "utils/to_string.hh"

using namespace seastar;

BOOST_AUTO_TEST_CASE(test_value_to_quoted_string) {
    std::vector<sstring> input = {
            "\"\\\b\f\n\r\t",
            sstring(1, '\0') + "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f",
            "regular string",
            "mixed\t\t\t\ba\f \007 string \002 fgh",
            "chào mọi người 123!",
            "ყველას მოგესალმებით 456?;",
            "всем привет",
            "大家好",
            ""
    };

    std::vector<sstring> expected = {
            "\"\\\"\\\\\\b\\f\\n\\r\\t\"",
            "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u000F\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017\\u0018\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E\\u001F\"",
            "\"regular string\"",
            "\"mixed\\t\\t\\t\\ba\\f \\u0007 string \\u0002 fgh\"",
            "\"chào mọi người 123!\"",
            "\"ყველას მოგესალმებით 456?;\"",
            "\"всем привет\"",
            "\"大家好\"",
            "\"\""
    };

    for (size_t i = 0; i < input.size(); ++i) {
        BOOST_CHECK_EQUAL(rjson::quote_json_string(input[i]), expected[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_parsing_map_from_null) {
    std::map<sstring, sstring> empty_map;
    auto map1 = rjson::parse_to_map<std::map<sstring, sstring>>("null");
    auto map2 = rjson::parse_to_map<std::map<sstring, sstring>>("{}");
    BOOST_REQUIRE(map1 == map2);
    BOOST_REQUIRE(map1 == empty_map);
}

namespace {

std::string_view trim(std::string_view sv) {
    auto it = sv.begin();
    auto end = sv.end();
    while (it != end && *it == ' ') {
        ++it;
    }
    return std::string_view(it, end);
}

std::string_view cmp_and_remove_prefix(std::string_view sv, std::string_view expected) {
    BOOST_TEST_MESSAGE(format("cmp_and_remove_prefix: {} expected='{}'", sv, expected));
    trim(sv);
    BOOST_REQUIRE(sv.starts_with(expected));
    auto sz = expected.size();
    sv = sv.substr(sz, sv.size() - sz);
    return trim(sv);
}

void verify_parenthesis(std::string_view sv) {
    static std::unordered_map<char, char> paren_map = {{'{', '}'}, {'[', ']'}, {'(', ')'}, {'<', '>'}};

    char open = sv[0];
    char close = sv[sv.size() - 1];
    auto it = paren_map.find(open);
    if (it == paren_map.end()) {
        BOOST_FAIL(format("Unexpected delimiters: '{}' '{}'", open, close));
    }
    BOOST_REQUIRE_EQUAL(close, it->second);
}

template <std::ranges::range Range>
void test_format_range(const char* desc, Range x, std::vector<std::string> expected_strings, std::string expected_delim = ",", bool expect_parenthesis = true) {
    auto str = seastar::format("{}", x);
    BOOST_TEST_MESSAGE(format("{}: {}", desc, str));

    auto fmt_str = fmt::format("{}", x);
    BOOST_REQUIRE_EQUAL(str, fmt_str);

    size_t num_elements = expected_strings.size();

    size_t paren_size = expect_parenthesis ? 2 : 0;
    size_t min_size = paren_size + (x.empty() ? 0 : (num_elements - 1));
    BOOST_REQUIRE_GE(str.size(), min_size);

    std::string_view sv = str;
    if (expect_parenthesis) {
        verify_parenthesis(sv);
        sv = sv.substr(1, sv.size() - 2);
    }

    bool first = true;
    while (!expected_strings.empty()) {
        sv = trim(sv);
        if (!std::exchange(first, false)) {
            sv = cmp_and_remove_prefix(sv, expected_delim);
        }

        auto s = *expected_strings.begin();
        sv = cmp_and_remove_prefix(sv, s);
        expected_strings.erase(expected_strings.begin());
    }

    BOOST_REQUIRE(sv.empty());
}

template <std::ranges::range Range>
void test_format_range(const char* desc, Range x, std::unordered_set<std::string> expected_strings, std::string expected_delim = ",", bool expect_parenthesis = true) {
    auto str = seastar::format("{}", x);
    BOOST_TEST_MESSAGE(format("{}: {}", desc, str));

    auto fmt_str = fmt::format("{}", x);
    BOOST_REQUIRE_EQUAL(str, fmt_str);

    size_t num_elements = expected_strings.size();

    size_t paren_size = expect_parenthesis ? 2 : 0;
    size_t min_size = paren_size + (x.empty() ? 0 : (num_elements - 1));
    BOOST_REQUIRE_GE(str.size(), min_size);

    std::string_view sv = str;
    if (expect_parenthesis) {
        verify_parenthesis(sv);
        sv = sv.substr(1, sv.size() - 2);
    }

    bool first = true;
    while (!expected_strings.empty()) {
        sv = trim(sv);
        if (!std::exchange(first, false)) {
            sv = cmp_and_remove_prefix(sv, expected_delim);
        }

        for (auto it = expected_strings.begin(); it != expected_strings.end(); ++it) {
            if (sv.starts_with(*it)) {
                sv = cmp_and_remove_prefix(sv, *it);
                expected_strings.erase(it);
                break;
            }
        }
    }

    BOOST_REQUIRE(sv.empty());
}

} // namespace

BOOST_AUTO_TEST_CASE(test_vector_format) {
    auto vector = std::vector<int>({1, 2, 3});
    test_format_range("vector", vector, std::vector<std::string>({"1", "2", "3"}));

    auto array = std::array<int, 3>({1, 2, 3});
    test_format_range("array", array, std::vector<std::string>({"1", "2", "3"}));

    auto list = std::list<int>({1, 2, 3});
    test_format_range("list", list, std::vector<std::string>({"1", "2", "3"}));

    auto deque = std::deque<int>({1, 2, 3});
    test_format_range("deque", deque, std::vector<std::string>({"1", "2", "3"}));

    auto set = std::set<int>({1, 2, 3});
    test_format_range("set", set, std::vector<std::string>({"1", "2", "3"}));

    auto unordered_set = std::unordered_set<int>({1, 2, 3});
    test_format_range("unordered_set", unordered_set, std::unordered_set<std::string>({"1", "2", "3"}));

    auto map = std::map<int, std::string>({{1, "one"}, {2, "two"}, {3, "three"}});
    test_format_range("map", map, std::vector<std::string>({"{1, one}", "{2, two}", "{3, three}"}));
    test_format_range("map | boost::adaptors::map_keys", map | boost::adaptors::map_keys, std::vector<std::string>({"1", "2", "3"}));
    test_format_range("map | boost::adaptors::map_values", map | boost::adaptors::map_values, std::vector<std::string>({"one", "two", "three"}));

    auto unordered_map = std::unordered_map<int, std::string>({{1, "one"}, {2, "two"}, {3, "three"}});
    // seastar has a specialized print function for unordered_map
    // See https://github.com/scylladb/seastar/issues/1544
    test_format_range("unordered_map", unordered_map, std::unordered_set<std::string>({"{1 -> one}", "{2 -> two}", "{3 -> three}"}));
    test_format_range("unordered_map | boost::adaptors::map_keys", unordered_map | boost::adaptors::map_keys, std::unordered_set<std::string>({"1", "2", "3"}));
    test_format_range("unordered_map | boost::adaptors::map_values", unordered_map | boost::adaptors::map_values, std::unordered_set<std::string>({"one", "two", "three"}));
}

BOOST_AUTO_TEST_CASE(test_optional_string_format) {
    std::optional<std::string> sopt;

    auto s = format("{}", sopt);
    BOOST_TEST_MESSAGE(format("Empty opt: {}", s));
    BOOST_REQUIRE_EQUAL(s.size(), 2);
    verify_parenthesis(s);

    sopt.emplace("foo");
    s = format("{}", sopt);
    BOOST_TEST_MESSAGE(format("Engaged opt: {}", s));
}

BOOST_AUTO_TEST_CASE(test_boost_transformed_range_format) {
    auto v= std::vector<int>({1, 2, 3});

    test_format_range("boost::adaptors::transformed", v | boost::adaptors::transformed([] (int i) { return format("{}", i * 11); }),
        std::vector<std::string>({"11", "22", "33"}));
}
