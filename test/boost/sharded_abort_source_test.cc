/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/util/closeable.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "utils/sharded_abort_source.hh"

using namespace seastar;

SEASTAR_THREAD_TEST_CASE(test_sharded_abort_source) {
    sharded_abort_source sas;
    sas.start().get();
    auto stop_sas = deferred_stop(sas);

    struct object {
        optimized_optional<abort_source::subscription> subscription;
        bool aborted = false;
    };
    sharded<object> objects;
    objects.start().get();
    auto stop_objects = deferred_stop(objects);

    objects.invoke_on_all([&] (object& o) {
        o.subscription = sas.local().subscribe([&o] () noexcept {
            o.aborted = true;
        });
    }).get();

    BOOST_REQUIRE(!sas.abort_requested());
    auto f = sas.request_abort();
    BOOST_REQUIRE(sas.abort_requested());
    f.get();

    objects.invoke_on_all([&] (object& o) {
        BOOST_REQUIRE(o.aborted);
    }).get();
}
