/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <seastar/core/abort_source.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/on_internal_error.hh>

#include "test/lib/log.hh"

#include "database.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/schema_tables.hh"
#include "gms/feature_service.hh"
#include "locator/token_metadata.hh"
#include "service/service_ctl.hh"
#include "service/migration_manager.hh"

SEASTAR_THREAD_TEST_CASE(test_service_ctl) {
    auto ext = std::make_shared<db::extensions>();
    auto cfg = make_lw_shared<db::config>(ext);

    auto make_sched_group = [&] (sstring name, unsigned shares) {
        if (cfg->cpu_scheduler()) {
            return seastar::create_scheduling_group(name, shares).get0();
        } else {
            return seastar::scheduling_group();
        }
    };

    auto maintenance_scheduling_group = make_sched_group("streaming", 200);
    database_config dbcfg;
    dbcfg.compaction_scheduling_group = make_sched_group("compaction", 1000);
    dbcfg.memory_compaction_scheduling_group = make_sched_group("mem_compaction", 1000);
    dbcfg.streaming_scheduling_group = maintenance_scheduling_group;
    dbcfg.statement_scheduling_group = make_sched_group("statement", 1000);
    dbcfg.memtable_scheduling_group = make_sched_group("memtable", 1000);
    dbcfg.memtable_to_cache_scheduling_group = make_sched_group("memtable_to_cache", 200);
    dbcfg.gossip_scheduling_group = make_sched_group("gossip", 1000);
    dbcfg.available_memory = memory::stats().total_memory();

    service::services_controller sctl;

    service::sharded_service_ctl<seastar::abort_source> stop_signal_ctl(sctl, "stop signal", [] (auto& s) { return s.start(); });
    service::sharded_service_ctl<locator::shared_token_metadata> token_metadata_ctl(sctl, "token metadata");
    token_metadata_ctl.start_func = [] (sharded<locator::shared_token_metadata>& tm) {
        return tm.start([] () noexcept { return db::schema_tables::hold_merge_lock(); });
    };

    service::sharded_service_ctl<service::migration_notifier> mm_notifier_ctl(sctl, "migration notifier", [] (auto& s) { return s.start(); });
    gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg);
    service::sharded_service_ctl<gms::feature_service> feature_service_ctl(sctl, "feature service");
    feature_service_ctl.start_func = [fcfg] (sharded<gms::feature_service>& fs) {
        return fs.start(fcfg);
    };
    service::sharded_service_ctl<semaphore> sst_dir_semaphore_ctl(sctl, "SSTable directory semaphore");
    sst_dir_semaphore_ctl.start_func = [cfg] (sharded<semaphore>& sst_dir_semaphore) {
        return sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency());
    };

    service::sharded_service_ctl<database> db_ctl(sctl, "database");
    db_ctl.start_func = [&,
            mm_notifier = db_ctl.lookup_dep(mm_notifier_ctl),
            feature_service = db_ctl.lookup_dep(feature_service_ctl),
            token_metadata = db_ctl.lookup_dep(token_metadata_ctl),
            stop_signal = db_ctl.lookup_dep(stop_signal_ctl),
            sst_dir_semaphore = db_ctl.lookup_dep(sst_dir_semaphore_ctl)] (sharded<database>& db) {
        return db.start(std::ref(*cfg), dbcfg, mm_notifier, feature_service, token_metadata, stop_signal, sst_dir_semaphore, utils::cross_shard_barrier());
    };

    std::exception_ptr ex;
    try {
        testlog.info("Starting all services");
        sctl.start().get();

        testlog.info("Starting to serve");
        sctl.serve().get();

        testlog.info("Draining all services");
        sctl.drain().get();

        testlog.info("Shutting down all services");
        sctl.shutdown().get();
    } catch (...) {
        ex = std::current_exception();
    }

    testlog.info("Stopping all services");
    sctl.stop().get();

    BOOST_REQUIRE(!ex);
}

SEASTAR_THREAD_TEST_CASE(test_circular_dependency) {
    service::services_controller sctl;

    struct test_service {
    };
    service::sharded_service_ctl<test_service> t0(sctl, "t0");
    service::sharded_service_ctl<test_service> t1(sctl, "t1");
    service::sharded_service_ctl<test_service> t2(sctl, "t2");

    auto _ = testing::scoped_no_abort_on_internal_error();
    t1.depends_on(t0);
    BOOST_REQUIRE_THROW(t0.depends_on(t1), std::runtime_error);
    t2.depends_on(t1);
    BOOST_REQUIRE_THROW(t0.depends_on(t2), std::runtime_error);
}
