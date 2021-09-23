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

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "simple_strategy.hh"
#include "utils/class_registrator.hh"
#include <boost/algorithm/string.hpp>
#include "utils/sequenced_set.hh"

namespace locator {

simple_strategy::simple_strategy(const shared_token_metadata& token_metadata, snitch_ptr& snitch, const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(token_metadata, snitch, config_options, replication_strategy_type::simple) {
    for (auto& config_pair : config_options) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        if (boost::iequals(key, "replication_factor")) {
            validate_replication_factor(val);
            _replication_factor = std::stol(val);

            break;
        }
    }
}

future<inet_address_vector_replica_set> simple_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    const std::vector<token>& tokens = tm.sorted_tokens();

    if (tokens.empty()) {
        co_return inet_address_vector_replica_set();
    }

    size_t replicas = get_replication_factor();
    utils::sequenced_set<inet_address> endpoints;
    endpoints.reserve(replicas);

    for (auto& token : tm.ring_range(t)) {
        if (endpoints.size() == replicas) {
           break;
        }

        auto ep = tm.get_endpoint(token);
        assert(ep);

        endpoints.push_back(*ep);
        co_await coroutine::maybe_yield();
    }

    co_return boost::copy_range<inet_address_vector_replica_set>(endpoints.get_vector());
}

size_t simple_strategy::get_replication_factor() const {
    return _replication_factor;
}

void simple_strategy::validate_options() const {
    auto it = _config_options.find("replication_factor");
    if (it == _config_options.end()) {
        throw exceptions::configuration_exception("SimpleStrategy requires a replication_factor strategy option.");
    }
    validate_replication_factor(it->second);
}

std::optional<std::set<sstring>>simple_strategy::recognized_options() const {
    return {{ "replication_factor" }};
}

class effective_simple_strategy_impl : public effective_replication_strategy::impl {
    replication_map _all_endpoints;
public:
    explicit effective_simple_strategy_impl(replication_map all_endpoints) noexcept
        : _all_endpoints(std::move(all_endpoints))
    {}

    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token, const token_metadata& tm) const override {
        const token& key_token = tm.first_token(search_token);
        auto res = _all_endpoints.find(key_token);
        return res->second;
    }
};

future<lw_shared_ptr<effective_replication_strategy>> simple_strategy::make_effective(token_metadata_ptr tmptr) const {
    replication_map m;

    for (const auto &t : tmptr->sorted_tokens()) {
        m[t] = co_await calculate_natural_endpoints(t, *tmptr);
    }

    auto impl = std::make_unique<effective_simple_strategy_impl>(std::move(m));
    co_return make_lw_shared<effective_replication_strategy>(*this, std::move(tmptr), std::move(impl));
}

using registry = class_registrator<abstract_replication_strategy, simple_strategy, const shared_token_metadata&, snitch_ptr&, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.SimpleStrategy");
static registry registrator_short_name("SimpleStrategy");

}
