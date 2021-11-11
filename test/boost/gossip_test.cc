
/*
 * Copyright (C) 2015-present ScyllaDB
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


#include <boost/test/unit_test.hpp>

#include <seastar/util/defer.hh>

#include <seastar/testing/test_case.hh>
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include <seastar/core/seastar.hh>
#include "service/storage_service.hh"
#include "service/raft/raft_group_registry.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include "cdc/generation_service.hh"
#include "repair/repair.hh"
#include "repair/row_level.hh"
#include "database.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "compaction/compaction_manager.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "db/schema_tables.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/service_ctl.hh"
#include "cql3/query_processor.hh"
#include "cql3/cql_config.hh"

using namespace std::chrono_literals;

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        service::services_controller sctl;

        database_config dbcfg;
        dbcfg.available_memory = memory::stats().total_memory();
        auto cfg = std::make_unique<db::config>();

        service::sharded_service_ctl<service::migration_notifier> mm_notif_ctl(sctl, "migration_notifier", service::default_start_tag{});
        service::sharded_service_ctl<abort_source> abort_source_ctl(sctl, "abort_source", service::default_start_tag{});
        service::sharded_service_ctl<db::system_distributed_keyspace> sys_dist_ks_ctl(sctl, "system_distributed_keyspace");
        utils::fb_utilities::set_broadcast_address(gms::inet_address("127.0.0.1"));
        service::sharded_service_ctl<repair_service> repair_ctl(sctl, "repair");
        service::sharded_service_ctl<service::endpoint_lifecycle_notifier> elc_notif_ctl(sctl, "endpoint_lifecycle_notifier", service::default_start_tag{});

        service::sharded_service_ctl<gms::feature_service> feature_service_ctl(sctl, "feature_service");
        feature_service_ctl.start_func = [&cfg] (sharded<gms::feature_service>& fs) {
            return fs.start(gms::feature_config_from_db_config(*cfg));
        };

        service::service_ctl snitch_ctl(sctl, "snitch",
            [] { return locator::i_endpoint_snitch::create_snitch("SimpleSnitch"); },
            [] { return locator::i_endpoint_snitch::stop_snitch(); });

        service::sharded_service_ctl<netw::messaging_service> messaging_ctl(sctl, "messaging");
        messaging_ctl.start_func = [] (sharded<netw::messaging_service>& ms) {
            return ms.start(gms::inet_address("127.0.0.1"), 7000);
        };
        messaging_ctl.depends_on(snitch_ctl);

        service::sharded_service_ctl<locator::shared_token_metadata> token_metadata_ctl(sctl, "token_metadata");
        token_metadata_ctl.start_func = [] (sharded<locator::shared_token_metadata>& tm) {
            return tm.start([] () noexcept { return db::schema_tables::hold_merge_lock(); });
        };

        service::sharded_service_ctl<gms::gossiper> gossiper_ctl(sctl, "gossiper");
        gossiper_ctl.start_func = [cfg = std::ref(*cfg),
                abort_source = gossiper_ctl.lookup_dep(abort_source_ctl),
                feature_service = gossiper_ctl.lookup_dep(feature_service_ctl),
                token_metadata = gossiper_ctl.lookup_dep(token_metadata_ctl),
                messaging = gossiper_ctl.lookup_dep(messaging_ctl)] (sharded<gms::gossiper>& gossiper) {
            return gossiper.start(abort_source, feature_service, token_metadata, messaging, cfg, gms::gossip_config{});
        };

        service::sharded_service_ctl<service::migration_manager> migration_manager_ctl(sctl, "migration_manager");
        migration_manager_ctl.start_func = [
                mm_notif = migration_manager_ctl.lookup_dep(mm_notif_ctl),
                feature_service = migration_manager_ctl.lookup_dep(feature_service_ctl),
                ms = migration_manager_ctl.lookup_dep(messaging_ctl),
                gossiper = migration_manager_ctl.lookup_dep(gossiper_ctl)] (sharded<service::migration_manager>& mm) {
            return mm.start(mm_notif, feature_service, ms, gossiper);
        };

        service::sharded_service_ctl<semaphore> sst_dir_semaphore_ctl(sctl, "sst_dir_semaphore", [&cfg] (sharded<semaphore>& sst_dir_semaphore) {
            return sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency());
        });

        service::sharded_service_ctl<database> db_ctl(sctl, "database");
        db_ctl.start_func = [cfg = std::ref(*cfg), &dbcfg,
                mm_notif = db_ctl.lookup_dep(mm_notif_ctl),
                feature_service = db_ctl.lookup_dep(feature_service_ctl),
                token_metadata = db_ctl.lookup_dep(token_metadata_ctl),
                abort_source = db_ctl.lookup_dep(abort_source_ctl),
                sst_dir_semaphore = db_ctl.lookup_dep(sst_dir_semaphore_ctl)] (sharded<database>& db) {
            return db.start(cfg, dbcfg, mm_notif, feature_service, token_metadata, abort_source, sst_dir_semaphore);
        };

        service::storage_proxy::config spcfg {
            .hints_directory_initializer = db::hints::directory_initializer::make_dummy(),
            .available_memory = memory::stats().total_memory(),
        };
        scheduling_group_key_config sg_conf =
                make_scheduling_group_key_config<service::storage_proxy_stats::stats>();
        scheduling_group_key stats_key = scheduling_group_key_create(sg_conf).get0();
        db::view::node_update_backlog b(smp::count, 10ms);
        service::sharded_service_ctl<service::storage_proxy> sp_ctl(sctl, "storage_proxy");
        sp_ctl.start_func = [&,
                b = std::ref(b),
                db = sp_ctl.lookup_dep(db_ctl),
                gossiper = sp_ctl.lookup_dep(gossiper_ctl),
                feature_service = sp_ctl.lookup_dep(feature_service_ctl),
                token_metadata = sp_ctl.lookup_dep(token_metadata_ctl),
                messaging = sp_ctl.lookup_dep(messaging_ctl)] (sharded<service::storage_proxy>& sp) {
            return sp.start(db, gossiper, spcfg, b, stats_key, feature_service, token_metadata, messaging);
        };

        service::sharded_service_ctl<cql3::cql_config> cql_config_ctl(sctl, "cql_config", [] (sharded<cql3::cql_config>& cql_config) {
            return cql_config.start(cql3::cql_config::default_tag{});
        });

        cql3::query_processor::memory_config qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
        service::sharded_service_ctl<cql3::query_processor> qp_ctl(sctl, "cql3::query_processor");
        qp_ctl.start_func = [&,
                sp = qp_ctl.lookup_dep(sp_ctl),
                db = qp_ctl.lookup_dep(db_ctl),
                mm_notif = qp_ctl.lookup_dep(mm_notif_ctl),
                mm = qp_ctl.lookup_dep(migration_manager_ctl),
                cql_config = qp_ctl.lookup_dep(cql_config_ctl)] (sharded<cql3::query_processor>& qp) {
            return qp.start(sp, db, mm_notif, mm, qp_mcfg, cql_config);
        };
        qp_ctl.serve_func = [] (sharded<cql3::query_processor>& qp) {
            db::system_keyspace::minimal_setup(qp);
            return make_ready_future<>();
        };

        service::sharded_service_ctl<service::raft_group_registry> raft_gr_ctl(sctl, "raft_group_registry");
        raft_gr_ctl.start_func = [
                messaging = raft_gr_ctl.lookup_dep(messaging_ctl),
                gossiper = raft_gr_ctl.lookup_dep(gossiper_ctl),
                qp = raft_gr_ctl.lookup_dep(qp_ctl)] (sharded<service::raft_group_registry>& raft_gr) {
            return raft_gr.start(messaging, gossiper, qp);
        };

        cdc::generation_service::config cdc_cfg;
        service::sharded_service_ctl<cdc::generation_service> cdc_generation_service_ctl(sctl, "cdc::generation_service");
        cdc_generation_service_ctl.start_func = [&cdc_cfg,
                gossiper = cdc_generation_service_ctl.lookup_dep(gossiper_ctl),
                sys_dist_ks = cdc_generation_service_ctl.lookup_dep(sys_dist_ks_ctl),
                abort_source = cdc_generation_service_ctl.lookup_dep(abort_source_ctl),
                token_metadata = cdc_generation_service_ctl.lookup_dep(token_metadata_ctl),
                feature_service = cdc_generation_service_ctl.lookup_dep(feature_service_ctl),
                db = cdc_generation_service_ctl.lookup_dep(db_ctl)] (sharded<cdc::generation_service>& cdc_generation_service) {
            return cdc_generation_service.start(cdc_cfg, gossiper, sys_dist_ks, abort_source, token_metadata, feature_service, db);
        };

        service::storage_service_config sscfg;
        sscfg.available_memory =  memory::stats().total_memory();

        service::sharded_service_ctl<service::storage_service> ss_ctl(sctl, "storage_service");
        ss_ctl.start_func = [sscfg = std::move(sscfg),
                abort_source = ss_ctl.lookup_dep(abort_source_ctl),
                db = ss_ctl.lookup_dep(db_ctl),
                gossiper = ss_ctl.lookup_dep(gossiper_ctl),
                sys_dist_ks = ss_ctl.lookup_dep(sys_dist_ks_ctl),
                feature_service = ss_ctl.lookup_dep(feature_service_ctl),
                migration_manager = ss_ctl.lookup_dep(migration_manager_ctl),
                token_metadata = ss_ctl.lookup_dep(token_metadata_ctl),
                messaging = ss_ctl.lookup_dep(messaging_ctl),
                cdc_generation_service = ss_ctl.lookup_dep(cdc_generation_service_ctl),
                repair = ss_ctl.lookup_dep(repair_ctl),
                raft_gr = ss_ctl.lookup_dep(raft_gr_ctl),
                elc_notif = ss_ctl.lookup_dep(elc_notif_ctl)] (sharded<service::storage_service>& ss) {
            return ss.start(abort_source, db, gossiper, sys_dist_ks, feature_service, sscfg,
                    migration_manager, token_metadata, messaging, cdc_generation_service,
                    repair, raft_gr, elc_notif);
        };

        sctl.start().get();
        sctl.stop().get();
    });
}
