/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "seastar/core/with_scheduling_group.hh"
#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>
#include <seastar/core/scheduling.hh>
#include "utils/background_disposer.hh"

BOOST_AUTO_TEST_SUITE(background_disposer_test)

class clear_gently_tracker : public utils::background_disposer::basic_item {
    bool _cleared = false;
    size_t& _counter;
    scheduling_group _expected_sg;
public:
    clear_gently_tracker(size_t& counter, scheduling_group expected_sg = default_scheduling_group())
        : _counter(counter)
        , _expected_sg(expected_sg)
    {}

    clear_gently_tracker(clear_gently_tracker&& o) noexcept
        : _cleared(std::exchange(o._cleared, false))
        , _counter(o._counter)
    {
        BOOST_REQUIRE(!_cleared);
    }

    virtual future<> clear_gently() noexcept override {
        if (std::exchange(_cleared, true)) {
            BOOST_FAIL("already cleared once");
        }
        BOOST_REQUIRE(current_scheduling_group() == _expected_sg);
        ++_counter;
        return make_ready_future<>();
    }
};

SEASTAR_THREAD_TEST_CASE(background_disposer_basic_test) {
    utils::background_disposer disposer;
    auto stop_disposer = deferred_stop(disposer);
    size_t cleared_count = 0;

    auto item = std::make_unique<clear_gently_tracker>(cleared_count);
    disposer.dispose(std::move(item));
    stop_disposer.stop_now();

    BOOST_REQUIRE_EQUAL(cleared_count, 1);
}

SEASTAR_THREAD_TEST_CASE(background_disposer_default_scheduling_group_test) {
    auto sg = create_scheduling_group("test_default_dosposer_sg", 200).get();
    utils::background_disposer disposer(sg);
    auto stop_disposer = deferred_stop(disposer);
    size_t cleared_count = 0;

    auto item = std::make_unique<clear_gently_tracker>(cleared_count, sg);
    disposer.dispose(std::move(item));
    stop_disposer.stop_now();

    destroy_scheduling_group(sg).get();

    BOOST_REQUIRE_EQUAL(cleared_count, 1);
}

SEASTAR_THREAD_TEST_CASE(background_disposer_item_scheduling_group_test) {
    utils::background_disposer disposer;
    auto stop_disposer = deferred_stop(disposer);
    size_t cleared_count = 0;

    auto sg0 = create_scheduling_group("test_item_sg0", 200).get();
    auto item0 = std::make_unique<clear_gently_tracker>(cleared_count, sg0);
    with_scheduling_group(sg0, [&] {
        disposer.dispose(std::move(item0));
    }).get();

    auto sg1 = create_scheduling_group("test_item_sg1", 200).get();
    auto item1 = std::make_unique<clear_gently_tracker>(cleared_count, sg1);
    with_scheduling_group(sg1, [&] {
        disposer.dispose(std::move(item1));
    }).get();

    auto item2 = std::make_unique<clear_gently_tracker>(cleared_count);
    disposer.dispose(std::move(item2));
    stop_disposer.stop_now();

    destroy_scheduling_group(sg0).get();
    destroy_scheduling_group(sg1).get();

    BOOST_REQUIRE_EQUAL(cleared_count, 3);
}

BOOST_AUTO_TEST_SUITE_END()
