/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc_types.hh>
#include "inet_address_vectors.hh"
#include "seastarx.hh"

#include "locator/token_metadata.hh"

#include <seastar/testing/perf_tests.hh>
#include <seastar/testing/test_runner.hh>

#include "test/lib/topology.hh"

struct sort_by_proximity_topology {
    static constexpr size_t NODES = 15;
    static constexpr size_t VNODES = 64;
    semaphore sem{1};
    std::optional<locator::shared_token_metadata> stm;
    host_id_vector_replica_set nodes;

    sort_by_proximity_topology()
    {
        nodes.reserve(NODES);
        std::generate_n(std::back_inserter(nodes), NODES, [i = 0u]() mutable {
            return locator::host_id{utils::UUID(0, ++i)};
        });

        locator::token_metadata::config tm_cfg;
        gms::inet_address my_address("localhost");
        tm_cfg.topo_cfg.this_endpoint = my_address;
        tm_cfg.topo_cfg.this_cql_address = my_address;
        tm_cfg.topo_cfg.this_host_id = nodes[0];
        tm_cfg.topo_cfg.local_dc_rack = locator::endpoint_dc_rack::default_location;

        stm.emplace([this] () noexcept { return get_units(sem, 1); }, tm_cfg);

        std::unordered_set<dht::token> random_tokens;
        while (random_tokens.size() < nodes.size() * VNODES) {
            random_tokens.insert(dht::token::get_random_token());
        }
        std::unordered_map<locator::host_id, std::unordered_set<dht::token>> endpoint_tokens;
        auto next_token_it = random_tokens.begin();
        for (auto& node : nodes) {
            for (size_t i = 0; i < VNODES; ++i) {
                endpoint_tokens[node].insert(*next_token_it);
                next_token_it++;
            }
        }

        std::unordered_map<sstring, size_t> datacenters = {
            { "rf3", 3 },
            { "rf5", 5 },
            { "rf5", 5 },
        };

        stm->mutate_token_metadata_for_test([&] (locator::token_metadata& tm) {
            tests::generate_topology(tm.get_topology(), datacenters, nodes);
        });
    }
};

PERF_TEST_F(sort_by_proximity_topology, perf_sort_by_proximity)
{
    const auto& topology = stm->get()->get_topology();

    for (size_t idx = 0; idx < NODES; ++idx) {
        topology.do_sort_by_proximity(nodes[idx], nodes);
    }

    return NODES;
}
