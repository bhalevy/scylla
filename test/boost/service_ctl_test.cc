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

    service::sharded_service_ctl<seastar::abort_source> stop_signal_ctl("stop signal", [] (auto& s) { return s.start(); });
    service::sharded_service_ctl<locator::shared_token_metadata> token_metadata_ctl("token metadata");
    token_metadata_ctl.start_func = [] (sharded<locator::shared_token_metadata>& tm) {
        return tm.start([] () noexcept { return db::schema_tables::hold_merge_lock(); });
    };

    service::sharded_service_ctl<service::migration_notifier> mm_notifier_ctl("migration notifier", [] (auto& s) { return s.start(); });
    gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg);
    service::sharded_service_ctl<gms::feature_service> feature_service_ctl("feature service");
    feature_service_ctl.start_func = [fcfg] (sharded<gms::feature_service>& fs) {
        return fs.start(fcfg);
    };
    service::sharded_service_ctl<semaphore> sst_dir_semaphore_ctl("SSTable directory semaphore");
    sst_dir_semaphore_ctl.start_func = [cfg] (sharded<semaphore>& sst_dir_semaphore) {
        return sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency());
    };

    service::sharded_service_ctl<database> db_ctl("database");
    db_ctl.start_func = [&] (sharded<database>& db) {
        return db.start(std::ref(*cfg), dbcfg, std::ref(mm_notifier_ctl.service()), std::ref(feature_service_ctl.service()), std::ref(token_metadata_ctl.service()),
                std::ref(stop_signal_ctl.service()), std::ref(sst_dir_semaphore_ctl.service()), utils::cross_shard_barrier());
    };
    db_ctl.depends_on(mm_notifier_ctl)
            .depends_on(feature_service_ctl)
            .depends_on(token_metadata_ctl)
            .depends_on(stop_signal_ctl)
            .depends_on(sst_dir_semaphore_ctl);

    service::systemd systemd;
    systemd.depends_on(db_ctl);

    std::exception_ptr ex;
    try {
        testlog.info("Starting all services");
        systemd.start().get();

        testlog.info("Starting to serve");
        systemd.serve(service::base_controller::service_mode::normal).get();

        testlog.info("Draining all services");
        systemd.drain().get();

        testlog.info("Shutting down all services");
        systemd.shutdown().get();
    } catch (...) {
        ex = std::current_exception();
    }

    testlog.info("Stopping all services");
    systemd.stop().get();

    BOOST_REQUIRE(!ex);
}
