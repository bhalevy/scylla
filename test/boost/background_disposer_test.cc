/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "utils/background_disposer.hh"

BOOST_AUTO_TEST_SUITE(background_disposer_test)

class clear_gently_tracker : public utils::background_disposer::basic_item {
    bool _cleared = false;
    size_t& _counter;
public:
    clear_gently_tracker(size_t& counter)
        : _counter(counter)
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

BOOST_AUTO_TEST_SUITE_END()
