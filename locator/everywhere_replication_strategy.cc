/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#include "locator/everywhere_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "locator/token_metadata.hh"

namespace locator {

everywhere_replication_strategy_traits::everywhere_replication_strategy_traits(const replication_strategy_params&)
    : abstract_replication_strategy_traits(replication_strategy_type::everywhere_topology)
{}

everywhere_replication_strategy::everywhere_replication_strategy(replication_strategy_params params) :
        abstract_replication_strategy(everywhere_replication_strategy_traits(params), params) {
    _natural_endpoints_depend_on_token = false;
}

future<host_id_set> everywhere_replication_strategy::calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const {
    if (tm.sorted_tokens().empty()) {
        host_id_set result{host_id_vector_replica_set({host_id{}})};
        return make_ready_future<host_id_set>(std::move(result));
    }
    const auto& all_endpoints = tm.get_all_endpoints();
    return make_ready_future<host_id_set>(host_id_set(all_endpoints.begin(), all_endpoints.end()));
}

size_t everywhere_replication_strategy::get_replication_factor(const token_metadata& tm) const {
    return tm.sorted_tokens().empty() ? 1 : tm.count_normal_token_owners();
}

using registry = strategy_class_registry::registrator<everywhere_replication_strategy>;
static registry registrator("org.apache.cassandra.locator.EverywhereStrategy");
static registry registrator_short_name("EverywhereStrategy");

using traits_registry = strategy_class_traits_registry::registrator<everywhere_replication_strategy_traits>;
static traits_registry traits_registrator("org.apache.cassandra.locator.EverywhereStrategy");
static traits_registry traits_registrator_short_name("EverywhereStrategy");

}
