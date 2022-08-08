/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include <array>
#include <boost/program_options.hpp>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "utils/optional_reference.hh"

namespace po = boost::program_options;

struct myclass {
    int val;
};

namespace {

void verify_const(utils::optional_reference<const myclass> obj, bool expected_reference, int expected_value = 0) {
    if (expected_reference) {
        BOOST_REQUIRE(obj);
        BOOST_REQUIRE(obj.has_value());
        BOOST_REQUIRE_EQUAL(obj.value().val, expected_value);
        BOOST_REQUIRE_EQUAL(obj->val, expected_value);
        BOOST_REQUIRE_EQUAL((*obj).val, expected_value);
    } else {
        BOOST_REQUIRE(!obj);
        BOOST_REQUIRE(!obj.has_value());
    }
};

void verify_mutable(utils::optional_reference<myclass> obj, bool expected_reference, int expected_value = 0, int new_value = 0) {
    if (expected_reference) {
        BOOST_REQUIRE(obj);
        BOOST_REQUIRE(obj.has_value());
        BOOST_REQUIRE_EQUAL(obj.value().val, expected_value);
        BOOST_REQUIRE_EQUAL(obj->val, expected_value);
        BOOST_REQUIRE_EQUAL((*obj).val, expected_value);

        obj.value().val =
        obj->val =
        (*obj).val = new_value;
    } else {
        BOOST_REQUIRE(!obj);
        BOOST_REQUIRE(!obj.has_value());
    }
};

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_const) {
    verify_const(std::nullopt, false);

    myclass x0{17};
    verify_const(x0, true, 17);
}

BOOST_AUTO_TEST_CASE(test_mutable) {
    verify_mutable(std::nullopt, false);

    myclass x0{17};
    verify_mutable(x0, true, 17, 42);
    BOOST_REQUIRE_EQUAL(x0.val, 42);
}
