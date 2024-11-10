/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/util/closeable.hh>
#include <seastar/util/later.hh>

#include "test/lib/log.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/tmpdir.hh"

#include "utils/disk_space_monitor.hh"

SEASTAR_THREAD_TEST_CASE(test_disk_space_monitor) {
    tmpdir dir;
    utils::disk_space_monitor dsm;

    auto dsm_config = utils::disk_space_monitor::config{
        .normal_polling_interval = 1ms,
        .high_polling_interval = 1ms,
    };
    dsm.start(dir.path().native(), dsm_config);
    auto stop_dsm = deferred_stop(dsm);

    int called = 0;
    auto sub = dsm.subscribe([&] (const utils::disk_space_monitor& dsm) -> future<> {
        ++called;
        testlog.info("total_space={} free_space={} utilization={:.1f}%",
            dsm.total_space(), dsm.free_space(), dsm.disk_utilization() * 100);
        return make_ready_future();
    });

    while (!called) {
        yield().get();
    }
}
