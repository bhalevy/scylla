/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include "db/consistency_level.hh"
#include "db/read_repair_decision.hh"
#include "dht/token.hh"
#include "inet_address_vectors.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/host_id.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/token_metadata.hh"
#include "locator/types.hh"
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/defer.hh>
#include "test/lib/key_utils.hh"

using namespace locator;

BOOST_AUTO_TEST_SUITE(consistency_level_test)

// Three datacenters: dc1 (the coordinator's) and dc2 with RF=3, dc3 with
// RF=1. The nodes are listed in the order get_endpoints_for_reading()
// produces: by proximity, the coordinator first.
struct each_quorum_fixture {
    const host_id n1{utils::UUID(0, 1)};
    const host_id n2{utils::UUID(0, 2)};
    const host_id n3{utils::UUID(0, 3)};
    const host_id m1{utils::UUID(0, 4)};
    const host_id m2{utils::UUID(0, 5)};
    const host_id m3{utils::UUID(0, 6)};
    const host_id k1{utils::UUID(0, 7)};

    semaphore sem{1};
    shared_token_metadata stm;
    static_effective_replication_map_ptr erm;

    each_quorum_fixture()
        : stm([this] () noexcept { return get_units(sem, 1); }, make_config())
    {
        const std::vector<std::pair<host_id, endpoint_dc_rack>> nodes = {
            {n1, {"dc1", "r1"}}, {n2, {"dc1", "r1"}}, {n3, {"dc1", "r1"}},
            {m1, {"dc2", "r1"}}, {m2, {"dc2", "r1"}}, {m3, {"dc2", "r1"}},
            {k1, {"dc3", "r1"}},
        };
        stm.mutate_token_metadata([&] (token_metadata& tm) -> future<> {
            unsigned i = 0;
            for (const auto& [id, location] : nodes) {
                tm.get_topology().add_or_update_endpoint(id, location, node::state::normal);
                co_await tm.update_normal_tokens({dht::token(tests::d2t(double(++i) / nodes.size()))}, id);
            }
        }).get();

        std::map<sstring, replication_strategy_config_option> options = {
            {"dc1", "3"},
            {"dc2", "3"},
            {"dc3", "1"},
        };
        replication_strategy_params params(options, std::nullopt, std::nullopt);
        auto rs = abstract_replication_strategy::create_replication_strategy("NetworkTopologyStrategy", params, stm.get()->get_topology());
        erm = calculate_vnode_effective_replication_map(std::move(rs), stm.get()).get();
    }

    ~each_quorum_fixture() {
        erm = nullptr;
        stm.stop().get();
    }

    token_metadata::config make_config() const {
        token_metadata::config cfg;
        auto my_address = gms::inet_address("localhost");
        cfg.topo_cfg.this_endpoint = my_address;
        cfg.topo_cfg.this_cql_address = my_address;
        cfg.topo_cfg.this_host_id = n1;
        cfg.topo_cfg.local_dc_rack = {"dc1", "r1"};
        return cfg;
    }

    host_id_vector_replica_set filter(host_id_vector_replica_set live,
            const host_id_vector_replica_set& preferred = {},
            db::read_repair_decision read_repair = db::read_repair_decision::NONE,
            std::optional<host_id>* extra = nullptr) const {
        return db::filter_for_each_quorum(*erm, std::move(live), preferred, read_repair, extra);
    }
};

SEASTAR_THREAD_TEST_CASE(test_each_quorum_picks_a_quorum_per_datacenter) {
    each_quorum_fixture f;
    std::optional<host_id> extra;

    auto selected = f.filter({f.n1, f.n2, f.n3, f.m1, f.m2, f.m3, f.k1}, {}, db::read_repair_decision::NONE, &extra);

    // Two from each RF=3 datacenter, one from the RF=1 one, in proximity
    // order and with the coordinator first. The extra replica for
    // speculative retry comes from the local datacenter.
    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1, f.n2, f.m1, f.m2, f.k1}));
    BOOST_REQUIRE(extra == f.n3);
    BOOST_REQUIRE(db::is_sufficient_live_nodes(db::consistency_level::EACH_QUORUM, *f.erm, selected));
}

SEASTAR_THREAD_TEST_CASE(test_each_quorum_extra_falls_back_to_remote_datacenter) {
    each_quorum_fixture f;
    std::optional<host_id> extra;

    // n3 is down: the local datacenter has nothing left over, so the extra
    // replica is the closest unselected remote one.
    auto selected = f.filter({f.n1, f.n2, f.m1, f.m2, f.m3, f.k1}, {}, db::read_repair_decision::NONE, &extra);

    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1, f.n2, f.m1, f.m2, f.k1}));
    BOOST_REQUIRE(extra == f.m3);
}

SEASTAR_THREAD_TEST_CASE(test_each_quorum_no_extra_when_every_live_replica_is_needed) {
    each_quorum_fixture f;
    std::optional<host_id> extra;

    auto selected = f.filter({f.n1, f.n2, f.m1, f.m2, f.k1}, {}, db::read_repair_decision::NONE, &extra);

    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1, f.n2, f.m1, f.m2, f.k1}));
    BOOST_REQUIRE(!extra);
}

SEASTAR_THREAD_TEST_CASE(test_each_quorum_short_datacenter_returns_what_it_has) {
    each_quorum_fixture f;

    // Only one dc1 replica is live. Selection does not throw; it returns
    // what is available and leaves raising Unavailable to
    // assure_sufficient_live_nodes(), which sees the per-DC shortfall.
    auto selected = f.filter({f.n1, f.m1, f.m2, f.m3, f.k1});

    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1, f.m1, f.m2, f.k1}));
    BOOST_REQUIRE(!db::is_sufficient_live_nodes(db::consistency_level::EACH_QUORUM, *f.erm, selected));
    BOOST_REQUIRE_THROW(db::assure_sufficient_live_nodes(db::consistency_level::EACH_QUORUM, *f.erm, selected), exceptions::unavailable_exception);
}

SEASTAR_THREAD_TEST_CASE(test_each_quorum_datacenter_with_no_live_replica_is_unavailable) {
    each_quorum_fixture f;
    std::optional<host_id> extra;

    // dc2 replicates the keyspace but has no live replica at all. Selection
    // only sees the live replicas, so it picks quorums for the other
    // datacenters; assure_sufficient_live_nodes() enumerates the keyspace's
    // datacenters and raises Unavailable for dc2 with 2 required / 0 alive.
    auto selected = f.filter({f.n1, f.n2, f.n3, f.k1}, {}, db::read_repair_decision::NONE, &extra);

    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1, f.n2, f.k1}));
    BOOST_REQUIRE(extra == f.n3);
    BOOST_REQUIRE(!db::is_sufficient_live_nodes(db::consistency_level::EACH_QUORUM, *f.erm, selected));
    try {
        db::assure_sufficient_live_nodes(db::consistency_level::EACH_QUORUM, *f.erm, selected);
        BOOST_FAIL("expected unavailable_exception");
    } catch (const exceptions::unavailable_exception& e) {
        BOOST_REQUIRE_EQUAL(e.required, 2);
        BOOST_REQUIRE_EQUAL(e.alive, 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_each_quorum_prefers_preferred_endpoints_per_datacenter) {
    each_quorum_fixture f;
    std::optional<host_id> extra;

    // The replicas the previous page was served from are taken first in
    // their own datacenter, so paging stays sticky in each datacenter.
    auto selected = f.filter({f.n1, f.n2, f.n3, f.m1, f.m2, f.m3, f.k1}, {f.n3, f.m3}, db::read_repair_decision::NONE, &extra);

    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n3, f.m3, f.n1, f.m1, f.k1}));
    BOOST_REQUIRE(extra == f.n2);
}

SEASTAR_THREAD_TEST_CASE(test_each_quorum_dc_local_read_repair_takes_all_local_replicas) {
    each_quorum_fixture f;
    std::optional<host_id> extra;

    // With a DC_LOCAL read repair decision the local datacenter contributes
    // every live replica, like the bf = max(bf, local_count) rule of the
    // other consistency levels; the remote datacenters still send a quorum.
    auto selected = f.filter({f.n1, f.n2, f.n3, f.m1, f.m2, f.m3, f.k1}, {}, db::read_repair_decision::DC_LOCAL, &extra);

    BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1, f.n2, f.n3, f.m1, f.m2, f.k1}));
    BOOST_REQUIRE(extra == f.m3);
}

BOOST_AUTO_TEST_SUITE_END()
