/*
 * Copyright (C) 2020 ScyllaDB
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

#define BOOST_TEST_MODULE versioned_shared_ptr
#include <boost/test/unit_test.hpp>

#include "utils/versioned_shared_ptr.hh"

BOOST_AUTO_TEST_CASE(test_versioned_shared_ptr_basics) {
    struct X {
        int value;
    };

    utils::versioned_shared_ptr<X> p0(make_lw_shared<X>(42));
    BOOST_REQUIRE_EQUAL(p0->value, 42);
    BOOST_REQUIRE_EQUAL((*p0).value, 42);
    BOOST_REQUIRE_EQUAL(p0.get()->value, 42);

    auto p1 = utils::make_versioned_shared_ptr<X>(42);
    BOOST_REQUIRE_EQUAL(p1->value, 42);
    BOOST_REQUIRE(p0 != p1);
    BOOST_REQUIRE(p0.uid() != p1.uid());
}

BOOST_AUTO_TEST_CASE(test_versioned_shared_ptr_assign) {
    struct X {
        int value;
    };

    utils::versioned_shared_ptr<X> p0;

    BOOST_REQUIRE_NO_THROW(p0 = utils::make_versioned_shared_ptr<X>(42));
    BOOST_REQUIRE_EQUAL(p0->value, 42);

    p0.reset();
    BOOST_REQUIRE_NO_THROW(p0 = utils::make_versioned_shared_ptr<X>(17));
    BOOST_REQUIRE_EQUAL(p0->value, 17);

    auto p1 = p0;
    p1->value++;
    BOOST_REQUIRE_NO_THROW(p0 = p1);
    BOOST_REQUIRE_EQUAL(p0->value, p1->value);
}

BOOST_AUTO_TEST_CASE(test_versioned_shared_ptr_get) {
    struct X {
        int value;
    };

    auto p0 = utils::make_versioned_shared_ptr<X>(42);
    BOOST_REQUIRE_EQUAL(p0->value, 42);

    const utils::versioned_shared_ptr<X> p1 = p0;
    BOOST_REQUIRE_EQUAL(p0.version(), p1.version());
    BOOST_REQUIRE(p0 == p1);
    BOOST_REQUIRE_EQUAL(p0->value, p1->value);

    const utils::versioned_shared_ptr<X> p2 = p1;
    BOOST_REQUIRE_EQUAL(p0.version(), p2.version());
    BOOST_REQUIRE(p0 == p2);
    BOOST_REQUIRE_EQUAL(p0->value, p2->value);
}

BOOST_AUTO_TEST_CASE(test_versioned_shared_ptr_set) {
    struct X {
        int value;
    };

    auto p0 = utils::make_versioned_shared_ptr<X>(42);
    BOOST_REQUIRE_EQUAL(p0->value, 42);

    const utils::versioned_shared_ptr<X> p1 = p0;
    BOOST_REQUIRE_EQUAL(p0.version(), p1.version());
    BOOST_REQUIRE(p0 == p1);
    BOOST_REQUIRE_EQUAL(p0->value, p1->value);

    utils::versioned_shared_ptr<X> p2 = p0;
    BOOST_REQUIRE(p0 == p1);
    BOOST_REQUIRE(p0 == p2);
    BOOST_REQUIRE(p1 == p2);
    BOOST_REQUIRE_EQUAL(p0->value, p2->value);
    p2->value = 17;
    BOOST_REQUIRE_EQUAL(p0->value, p1->value);
    BOOST_REQUIRE_EQUAL(p0->value, p2->value);

    auto p3 = p0.clone();
    BOOST_REQUIRE(p0 == p1);
    BOOST_REQUIRE(p0 != p3);
    BOOST_REQUIRE(p1 != p3);
    BOOST_REQUIRE_EQUAL(p0->value, p3->value);
    p3->value = 19;
    BOOST_REQUIRE_EQUAL(p0->value, p1->value);
    BOOST_REQUIRE_NE(p0->value, p3->value);

    p0.set(p3);
    BOOST_REQUIRE(p0 != p1);
    BOOST_REQUIRE(p0 != p2);
    BOOST_REQUIRE(p0 == p3);
    BOOST_REQUIRE(p1 == p2);
    BOOST_REQUIRE(p1 != p3);
    BOOST_REQUIRE_NE(p0->value, p1->value);
    BOOST_REQUIRE_NE(p0->value, p2->value);
    BOOST_REQUIRE_EQUAL(p0->value, p3->value);
}

BOOST_AUTO_TEST_CASE(test_versioned_shared_ptr_cmp_and_set) {
    struct X {
        int value;
    };

    auto p0 = utils::make_versioned_shared_ptr<X>(42);
    BOOST_REQUIRE_EQUAL(p0->value, 42);

    utils::versioned_shared_ptr<X> p1 = p0;
    BOOST_REQUIRE_EQUAL(p0.version(), p1.version());

    utils::versioned_shared_ptr<X> p2 = p0.clone();
    p2->value = 17;

    utils::versioned_shared_ptr<X> p3 = p0.clone();
    p3->value = 19;

    BOOST_REQUIRE_EQUAL(p2.version(), p3.version());

    BOOST_REQUIRE_NO_THROW(p0.cmp_and_set(p2));
    BOOST_REQUIRE_NE(p0.version(), p1.version());

    BOOST_REQUIRE_EQUAL(p0->value, p2->value);

    BOOST_REQUIRE_THROW(p0.cmp_and_set(p3), utils::versioned_shared_ptr_version_mismatch_error);
    BOOST_REQUIRE_EQUAL(p0->value, p2->value);
}

BOOST_AUTO_TEST_CASE(test_versioned_shared_ptr_uid) {
    struct X {
        int value;
    };

    utils::versioned_shared_ptr<X> p0;

    BOOST_REQUIRE_NO_THROW(p0 = {});
    BOOST_REQUIRE_NO_THROW(p0 = utils::make_versioned_shared_ptr<X>(42));

    BOOST_REQUIRE_THROW(p0 = {}, utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0 = utils::make_versioned_shared_ptr<X>(42), utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0.set(utils::make_versioned_shared_ptr<X>(42)), utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0.cmp_and_set(utils::make_versioned_shared_ptr<X>(42)), utils::versioned_shared_ptr_uid_mismatch_error);

    auto p1 = utils::make_versioned_shared_ptr<X>(42);
    BOOST_REQUIRE_THROW(p0 = p1, utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0.set(p1), utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0.cmp_and_set(p1), utils::versioned_shared_ptr_uid_mismatch_error);

    p1.reset();
    BOOST_REQUIRE_THROW(p0 = p1, utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0.set(p1), utils::versioned_shared_ptr_uid_mismatch_error);
    BOOST_REQUIRE_THROW(p0.cmp_and_set(p1), utils::versioned_shared_ptr_uid_mismatch_error);

    BOOST_REQUIRE_NO_THROW(p1 = utils::make_versioned_shared_ptr<X>(42));
    p0.reset();
    BOOST_REQUIRE_NO_THROW(p0 = p1);
    p0.reset();
    BOOST_REQUIRE_NO_THROW(p0.set(p1));
    p0.reset();
    BOOST_REQUIRE_NO_THROW(p0.cmp_and_set(p1));
}

BOOST_AUTO_TEST_CASE(test_versioned_shared_object) {
    struct X {
        int value;
    };

    auto so0 = utils::versioned_shared_object<X>(42);
    BOOST_REQUIRE_EQUAL(so0.get().value, 42);

    auto so1 = so0;
    BOOST_REQUIRE_EQUAL(so1.version(), so0.version());
    BOOST_REQUIRE_EQUAL(so1.get().value, 42);

    auto p0 = so0.get_shared_ptr();
    BOOST_REQUIRE_EQUAL(p0->value, 42);

    auto p1 = so1.clone_shared_ptr();
    BOOST_REQUIRE_NE(p1.version(), p0.version());
    BOOST_REQUIRE_EQUAL(p1->value, 42);
    p1->value = 17;

    BOOST_REQUIRE_EQUAL(so0.get().value, 42);
    BOOST_REQUIRE_EQUAL(so1.get().value, 42);
    BOOST_REQUIRE_EQUAL(p0->value, 42);

    BOOST_REQUIRE_NO_THROW(so1.cmp_and_set_shared_ptr(p1));
    BOOST_REQUIRE_EQUAL(so1.version(), so0.version());
    BOOST_REQUIRE_EQUAL(so0.get().value, 17);
    BOOST_REQUIRE_EQUAL(so1.get().value, 17);

    // test that the read-only snapshot wasn't changed
    // and that the version change is reflected in the base versioned_shared_object
    BOOST_REQUIRE_NE(so1.version(), p0.version());
    BOOST_REQUIRE_EQUAL(p0->value, 42);
}
