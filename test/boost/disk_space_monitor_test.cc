/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <sys/statvfs.h>

#include <seastar/util/closeable.hh>
#include <seastar/util/later.hh>

#include "seastar/core/manual_clock.hh"
#include "test/lib/log.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/tmpdir.hh"

#include "utils/disk_space_monitor.hh"

class disk_space_monitor_for_test : public utils::disk_space_monitor<manual_clock> {
    uint64_t _block_size;
    uint64_t _total_blocks;
    uint64_t _free_blocks;
public:
    disk_space_monitor_for_test(abort_source& as, disk_space_monitor::config config, uint64_t total_space = 1<<30, std::optional<uint64_t> free_space = std::nullopt)
        : disk_space_monitor(as, config)
        , _block_size(4096)
        , _total_blocks(total_space / _block_size)
        , _free_blocks(free_space.value_or(_total_blocks * _block_size) / _block_size)
    {}

    void set_utilization(float util) {
        _free_blocks = _total_blocks * (1. - util);
    }

protected:
    virtual future<struct statvfs> get_filesystem_stats() const override {
        struct statvfs ret{
            .f_bsize = _block_size,
            .f_frsize = _block_size,
            .f_blocks = _total_blocks,
            .f_bfree = _free_blocks,
            .f_bavail = _free_blocks / _block_size,
            .f_files = (_total_blocks / _block_size) / 10,
            .f_ffree = (_free_blocks / _block_size) / 10,
            .f_favail = (_free_blocks / _block_size) / 10,
            .f_fsid = 0xdeadbeef,
            .f_namemax = 4096
        };
        testlog.debug("disk_space_monitor_for_test: get_filesystem_stats: total={} free={}", _total_blocks * _block_size, _free_blocks * _block_size);
        return make_ready_future<struct statvfs>(ret);
    }
};

SEASTAR_THREAD_TEST_CASE(test_disk_space_monitor) {
    using namespace std::chrono;

    tmpdir dir;
    abort_source as;
    disk_space_monitor_for_test::config dsm_config{
        .normal_polling_interval = 10s,
        .high_polling_interval = 1s,
        .polling_interval_threshold = 0.5
    };
    disk_space_monitor_for_test dsm(as, dsm_config);

    dsm.start(dir.path().native());
    auto stop_dsm = deferred_stop(dsm);

    auto started_at = manual_clock::now();
    struct call {
        manual_clock::time_point t;
        float util;
    };
    std::vector<call> calls;
    auto sub = dsm.listen([&] (const utils::disk_space_monitor<manual_clock>& dsm) -> future<> {
        testlog.info("total_space={} free_space={} utilization={:.1f}%",
            dsm.total_space(), dsm.free_space(), dsm.disk_utilization() * 100);
        calls.emplace_back(manual_clock::now(), dsm.disk_utilization());
        return make_ready_future();
    });

    auto wait_for_call = [&] {
        auto started_waiting_at = manual_clock::now();
        auto start_size = calls.size();
        while (calls.size() == start_size && manual_clock::now() - started_waiting_at <= dsm_config.normal_polling_interval) {
            testlog.debug("{}: advancing clock", duration_cast<seconds>(manual_clock::now() - started_at).count());
            manual_clock::advance(1s);
            yield().get();
        }
        return calls.size();
    };

    BOOST_REQUIRE_EQUAL(wait_for_call(), 1);
    BOOST_REQUIRE(calls[0].t == started_at + dsm_config.normal_polling_interval);
    BOOST_REQUIRE_EQUAL(calls[0].util, 0.);

    dsm.set_utilization(dsm_config.polling_interval_threshold * .5);
    BOOST_REQUIRE_EQUAL(wait_for_call(), 2);
    BOOST_REQUIRE_EQUAL(calls[1].t.time_since_epoch().count(), (calls[0].t + dsm_config.normal_polling_interval).time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(calls[1].util, dsm_config.polling_interval_threshold * .5);

    dsm.set_utilization(dsm_config.polling_interval_threshold);
    BOOST_REQUIRE_EQUAL(wait_for_call(), 3);
    BOOST_REQUIRE_EQUAL(calls[2].t.time_since_epoch().count(), (calls[1].t + dsm_config.normal_polling_interval).time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(calls[2].util, dsm_config.polling_interval_threshold);

    BOOST_REQUIRE_EQUAL(wait_for_call(), 4);
    BOOST_REQUIRE_EQUAL(calls[3].t.time_since_epoch().count(), (calls[2].t + dsm_config.high_polling_interval).time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(calls[3].util, dsm_config.polling_interval_threshold);
}
