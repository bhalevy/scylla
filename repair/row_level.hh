/*
 * Copyright (C) 2018-present ScyllaDB
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

#pragma once

#include <vector>
#include "gms/inet_address.hh"
#include "repair/repair.hh"
#include <seastar/core/distributed.hh>

class row_level_repair_gossip_helper;

namespace service {
class migration_manager;
}

namespace db {

class system_distributed_keyspace;

}

namespace gms {
    class gossiper;
}

class node_ops_metrics {
    tracker& _tracker;
public:
    node_ops_metrics(tracker& tracker);

    uint64_t bootstrap_total_ranges{0};
    uint64_t bootstrap_finished_ranges{0};
    uint64_t replace_total_ranges{0};
    uint64_t replace_finished_ranges{0};
    uint64_t rebuild_total_ranges{0};
    uint64_t rebuild_finished_ranges{0};
    uint64_t decommission_total_ranges{0};
    uint64_t decommission_finished_ranges{0};
    uint64_t removenode_total_ranges{0};
    uint64_t removenode_finished_ranges{0};
    uint64_t repair_total_ranges_sum{0};
    uint64_t repair_finished_ranges_sum{0};
private:
    seastar::metrics::metric_groups _metrics;
    float bootstrap_finished_percentage();
    float replace_finished_percentage();
    float rebuild_finished_percentage();
    float decommission_finished_percentage();
    float removenode_finished_percentage();
    float repair_finished_percentage();
};

class repair_service : public seastar::peering_sharded_service<repair_service> {
    distributed<gms::gossiper>& _gossiper;
    netw::messaging_service& _messaging;
    sharded<database>& _db;
    sharded<db::system_distributed_keyspace>& _sys_dist_ks;
    sharded<db::view::view_update_generator>& _view_update_generator;
    service::migration_manager& _mm;
    tracker _tracker;
    node_ops_metrics _node_ops_metrics;

    shared_ptr<row_level_repair_gossip_helper> _gossip_helper;
    bool _stopped = false;

    future<> init_ms_handlers();
    future<> uninit_ms_handlers();

public:
    repair_service(distributed<gms::gossiper>& gossiper,
            netw::messaging_service& ms,
            sharded<database>& db,
            sharded<db::system_distributed_keyspace>& sys_dist_ks,
            sharded<db::view::view_update_generator>& vug,
            service::migration_manager& mm, size_t max_repair_memory);
    ~repair_service();
    future<> start();
    future<> stop();

    // shutdown() stops all ongoing repairs started on this node (and
    // prevents any further repairs from being started). It returns a future
    // saying when all repairs have stopped, and attempts to stop them as
    // quickly as possible (we do not wait for repairs to finish but rather
    // stop them abruptly).
    future<> shutdown();

    int do_repair_start(sstring keyspace, std::unordered_map<sstring, sstring> options_map);

    // The tokens are the tokens assigned to the bootstrap node.
    future<> bootstrap_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> bootstrap_tokens);
    future<> decommission_with_repair(locator::token_metadata_ptr tmptr);
    future<> removenode_with_repair(locator::token_metadata_ptr tmptr, gms::inet_address leaving_node, shared_ptr<node_ops_info> ops);
    future<> rebuild_with_repair(locator::token_metadata_ptr tmptr, sstring source_dc);
    future<> replace_with_repair(locator::token_metadata_ptr tmptr, std::unordered_set<dht::token> replacing_tokens);
private:
    future<> do_decommission_removenode_with_repair(locator::token_metadata_ptr tmptr, gms::inet_address leaving_node, shared_ptr<node_ops_info> ops);
    future<> do_rebuild_replace_with_repair(locator::token_metadata_ptr tmptr, sstring op, sstring source_dc, streaming::stream_reason reason);

    future<> sync_data_using_repair(sstring keyspace,
            dht::token_range_vector ranges,
            std::unordered_map<dht::token_range, repair_neighbors> neighbors,
            streaming::stream_reason reason,
            std::optional<utils::UUID> ops_uuid);

    future<> do_sync_data_using_repair(sstring keyspace,
            dht::token_range_vector ranges,
            std::unordered_map<dht::token_range, repair_neighbors> neighbors,
            streaming::stream_reason reason,
            std::optional<utils::UUID> ops_uuid);

public:
    netw::messaging_service& get_messaging() noexcept { return _messaging; }
    sharded<database>& get_db() noexcept { return _db; }
    service::migration_manager& get_migration_manager() noexcept { return _mm; }
    sharded<db::system_distributed_keyspace>& get_sys_dist_ks() noexcept { return _sys_dist_ks; }
    sharded<db::view::view_update_generator>& get_view_update_generator() noexcept { return _view_update_generator; }
    gms::gossiper& get_gossiper() noexcept { return _gossiper.local(); }
    tracker& repair_tracker();
    const tracker& repair_tracker() const {
        return const_cast<repair_service*>(this)->repair_tracker();
    }

    const node_ops_metrics& get_metrics() const noexcept {
        return _node_ops_metrics;
    };
    node_ops_metrics& get_metrics() noexcept {
        return _node_ops_metrics;
    };

    // returns a vector with the ids of the active repairs
    future<std::vector<int>> get_active_repairs();

    // returns the status of repair task `id`
    future<repair_status> get_status(int id);

    // If the repair job is finished (SUCCESSFUL or FAILED), it returns immediately.
    // It blocks if the repair job is still RUNNING until timeout.
    future<repair_status> await_completion(int id, std::chrono::steady_clock::time_point timeout);

    // Abort all the repairs
    future<> abort_all();

    future<> abort_repair_node_ops(utils::UUID ops_uuid);
};

class repair_info;

future<> repair_cf_range_row_level(repair_info& ri,
        sstring cf_name, utils::UUID table_id, dht::token_range range,
        const std::vector<gms::inet_address>& all_peer_nodes);

future<> shutdown_all_row_level_repair();
