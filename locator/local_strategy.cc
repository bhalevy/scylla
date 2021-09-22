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

#include <algorithm>
#include "local_strategy.hh"
#include "utils/class_registrator.hh"
#include "utils/fb_utilities.hh"


namespace locator {

local_strategy::local_strategy(const shared_token_metadata& token_metadata, snitch_ptr& snitch, const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(token_metadata, snitch, config_options, replication_strategy_type::local) {}

future<inet_address_vector_replica_set> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    return make_ready_future<inet_address_vector_replica_set>(inet_address_vector_replica_set({utils::fb_utilities::get_broadcast_address()}));
}

void local_strategy::validate_options() const {
}

std::optional<std::set<sstring>> local_strategy::recognized_options() const {
    // LocalStrategy doesn't expect any options.
    return {};
}

size_t local_strategy::get_replication_factor() const {
    return 1;
}

class effective_local_strategy_impl : public effective_replication_strategy::impl {
    virtual inet_address_vector_replica_set get_natural_endpoints(const token&, const token_metadata&) const override {
        return inet_address_vector_replica_set({utils::fb_utilities::get_broadcast_address()});
    }
};

future<lw_shared_ptr<effective_replication_strategy>> local_strategy::make_effective(token_metadata_ptr tmptr) const {
    auto impl = std::make_unique<effective_local_strategy_impl>();
    auto ers = make_lw_shared<effective_replication_strategy>(*this, std::move(tmptr), std::move(impl));
    return make_ready_future<lw_shared_ptr<effective_replication_strategy>>(std::move(ers));
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, const shared_token_metadata&, snitch_ptr&, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
