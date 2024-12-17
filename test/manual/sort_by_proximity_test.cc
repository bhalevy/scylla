/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>

#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc_types.hh>
#include "inet_address_vectors.hh"
#include "seastarx.hh"

#include "locator/token_metadata.hh"

#include "test/lib/log.hh"
#include "test/lib/topology.hh"

using namespace std::chrono_literals;
using clock_type = std::chrono::high_resolution_clock;

// Called in a seastar thread
void test_sort_by_proximity(clock_type::duration duration) {
    locator::token_metadata::config tm_cfg;
    auto my_address = gms::inet_address("localhost");

    constexpr size_t NODES = 15;
    constexpr size_t VNODES = 64;

    std::unordered_map<sstring, size_t> datacenters = {
                    { "rf3", 3 },
                    { "rf5", 5 },
                    { "rf5", 5 },
    };
    host_id_vector_replica_set nodes;
    nodes.reserve(NODES);
    std::generate_n(std::back_inserter(nodes), NODES, [i = 0u]() mutable {
        return locator::host_id{utils::UUID(0, ++i)};
    });

    tm_cfg.topo_cfg.this_endpoint = my_address;
    tm_cfg.topo_cfg.this_cql_address = my_address;
    tm_cfg.topo_cfg.this_host_id = nodes[0];
    tm_cfg.topo_cfg.local_dc_rack = locator::endpoint_dc_rack::default_location;

    semaphore sem(1);
    locator::shared_token_metadata stm([&sem] () noexcept { return get_units(sem, 1); }, tm_cfg);

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

    stm.mutate_token_metadata([&] (locator::token_metadata& tm) {
        tests::generate_topology(tm.get_topology(), datacenters, nodes);
        return make_ready_future();
    }).get();

    const auto& topology = stm.get()->get_topology();
    host_id_vector_replica_set sorted_nodes = nodes;

    size_t idx = 0;
    size_t count = 0;
    auto start_time = clock_type::now();
    auto end_time = start_time + duration;
    while (clock_type::now() < end_time) {
        topology.do_sort_by_proximity(nodes[idx], sorted_nodes);
        idx = (idx + 1) % NODES;
        count++;
    }
    testlog.info("Completed {} calls in {} seconds, {} nanoseconds per call",
            count,
            std::chrono::duration_cast<std::chrono::seconds>(duration).count(),
            (std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() + (count/2)) / count);
}

int main(int ac, char ** av) {
    app_template app;
    app.add_options()
        ("duration", boost::program_options::value<int>()->default_value(10), "Duration in seconds");

    return app.run_deprecated(ac, av, [&app] {
        return seastar::async([&app] {
            auto config = app.configuration();
            test_sort_by_proximity(std::chrono::seconds(config["duration"].as<int>()));
        }).finally([] {
            exit(0);
        });
    });
}
