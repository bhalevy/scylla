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
#include "gms/gossiper.hh"
#include "inet_address_vectors.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/host_id.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/token_metadata.hh"
#include "locator/types.hh"
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/defer.hh>
#include "test/lib/cql_test_env.hh"
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

    // The gossiper is only consulted for cache-hit-rate load balancing, which
    // needs a table; none is passed here, so any gossiper will do.
    host_id_vector_replica_set filter_for_query(db::consistency_level cl, const gms::gossiper& g,
            host_id_vector_replica_set live,
            const host_id_vector_replica_set& preferred = {},
            db::read_repair_decision read_repair = db::read_repair_decision::NONE,
            std::optional<host_id>* extra = nullptr) const {
        return db::filter_for_query(cl, *erm, std::move(live), preferred, read_repair, g, extra, nullptr);
    }

    host_id_vector_replica_set all_live() const {
        return {n1, n2, n3, m1, m2, m3, k1};
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

// The cases below pin the selection of the pre-existing consistency levels.
// The total replication factor is 7, so QUORUM needs 4; the local
// datacenter's is 3, so LOCAL_QUORUM needs 2.

SEASTAR_TEST_CASE(test_cluster_wide_levels_take_the_closest_replicas) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        each_quorum_fixture f;
        const auto& g = env.gossiper().local();
        using cl = db::consistency_level;

        auto check = [&] (cl level, host_id_vector_replica_set expected, std::optional<host_id> expected_extra) {
            std::optional<host_id> extra;
            BOOST_REQUIRE_EQUAL(f.filter_for_query(level, g, f.all_live(), {}, db::read_repair_decision::NONE, &extra), expected);
            BOOST_REQUIRE(extra == expected_extra);
        };

        // block_for() replicas by proximity; the next one is the extra for speculative retry.
        check(cl::ONE, {f.n1}, f.n2);
        check(cl::TWO, {f.n1, f.n2}, f.n3);
        check(cl::THREE, {f.n1, f.n2, f.n3}, f.m1);
        check(cl::QUORUM, {f.n1, f.n2, f.n3, f.m1}, f.m2);
        // Every replica is needed: nothing is left over to speculate with.
        check(cl::ALL, f.all_live(), std::nullopt);

        // The same when exactly block_for() replicas are alive.
        std::optional<host_id> extra;
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::QUORUM, g, {f.n1, f.n2, f.m1, f.k1}, {}, db::read_repair_decision::NONE, &extra),
                (host_id_vector_replica_set{f.n1, f.n2, f.m1, f.k1}));
        BOOST_REQUIRE(!extra);
    });
}

SEASTAR_TEST_CASE(test_local_levels_use_only_the_local_datacenter) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        each_quorum_fixture f;
        const auto& g = env.gossiper().local();
        using cl = db::consistency_level;
        std::optional<host_id> extra;

        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::LOCAL_ONE, g, f.all_live(), {}, db::read_repair_decision::NONE, &extra), (host_id_vector_replica_set{f.n1}));
        BOOST_REQUIRE(extra == f.n2);

        extra.reset();
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::LOCAL_QUORUM, g, f.all_live(), {}, db::read_repair_decision::NONE, &extra), (host_id_vector_replica_set{f.n1, f.n2}));
        BOOST_REQUIRE(extra == f.n3);

        // Remote replicas are never used, even when the local datacenter
        // falls short; that is reported by assure_sufficient_live_nodes().
        extra.reset();
        auto selected = f.filter_for_query(cl::LOCAL_QUORUM, g, {f.n1, f.m1, f.m2, f.m3, f.k1}, {}, db::read_repair_decision::NONE, &extra);
        BOOST_REQUIRE_EQUAL(selected, (host_id_vector_replica_set{f.n1}));
        BOOST_REQUIRE(!extra);
        BOOST_REQUIRE(!db::is_sufficient_live_nodes(cl::LOCAL_QUORUM, *f.erm, selected));
        BOOST_REQUIRE_THROW(db::assure_sufficient_live_nodes(cl::LOCAL_QUORUM, *f.erm, selected), exceptions::unavailable_exception);
    });
}

SEASTAR_TEST_CASE(test_preferred_endpoints_are_taken_first) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        each_quorum_fixture f;
        const auto& g = env.gossiper().local();
        using cl = db::consistency_level;
        std::optional<host_id> extra;

        // Fewer preferred replicas than needed: they come first, the rest is
        // filled by proximity and the extra is the next closest.
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::QUORUM, g, f.all_live(), {f.m3, f.k1}, db::read_repair_decision::NONE, &extra),
                (host_id_vector_replica_set{f.m3, f.k1, f.n1, f.n2}));
        BOOST_REQUIRE(extra == f.n3);

        // More preferred replicas than needed: the first block_for() of them,
        // and the next preferred one is the extra.
        extra.reset();
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::QUORUM, g, f.all_live(), {f.n2, f.n3, f.m1, f.m2, f.k1}, db::read_repair_decision::NONE, &extra),
                (host_id_vector_replica_set{f.n2, f.n3, f.m1, f.m2}));
        BOOST_REQUIRE(extra == f.k1);

        // Exactly as many as needed: the extra is the closest non-preferred one.
        extra.reset();
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::QUORUM, g, f.all_live(), {f.n2, f.n3, f.m1, f.m2}, db::read_repair_decision::NONE, &extra),
                (host_id_vector_replica_set{f.n2, f.n3, f.m1, f.m2}));
        BOOST_REQUIRE(extra == f.n1);

        // A preferred remote replica is ignored by a local level.
        extra.reset();
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::LOCAL_QUORUM, g, f.all_live(), {f.n3, f.m3}, db::read_repair_decision::NONE, &extra),
                (host_id_vector_replica_set{f.n3, f.n1}));
        BOOST_REQUIRE(extra == f.n2);
    });
}

SEASTAR_TEST_CASE(test_read_repair_decision_widens_the_selection) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        each_quorum_fixture f;
        const auto& g = env.gossiper().local();
        using cl = db::consistency_level;
        std::optional<host_id> extra;

        // GLOBAL: every live replica, whatever the level.
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::ONE, g, f.all_live(), {}, db::read_repair_decision::GLOBAL, &extra), f.all_live());
        BOOST_REQUIRE(!extra);

        // DC_LOCAL: at least every live replica of the local datacenter.
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::ONE, g, f.all_live(), {}, db::read_repair_decision::DC_LOCAL, &extra),
                (host_id_vector_replica_set{f.n1, f.n2, f.n3}));
        BOOST_REQUIRE(extra == f.m1);

        extra.reset();
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::QUORUM, g, f.all_live(), {}, db::read_repair_decision::DC_LOCAL, &extra),
                (host_id_vector_replica_set{f.n1, f.n2, f.n3, f.m1}));
        BOOST_REQUIRE(extra == f.m2);

        extra.reset();
        BOOST_REQUIRE_EQUAL(f.filter_for_query(cl::LOCAL_QUORUM, g, f.all_live(), {}, db::read_repair_decision::DC_LOCAL, &extra),
                (host_id_vector_replica_set{f.n1, f.n2, f.n3}));
        BOOST_REQUIRE(!extra);
    });
}

SEASTAR_THREAD_TEST_CASE(test_sufficient_live_nodes_per_level) {
    each_quorum_fixture f;
    using cl = db::consistency_level;

    auto sufficient = [&] (cl level, host_id_vector_replica_set live) {
        bool result = db::is_sufficient_live_nodes(level, *f.erm, live);
        if (result) {
            BOOST_REQUIRE_NO_THROW(db::assure_sufficient_live_nodes(level, *f.erm, live));
        } else {
            BOOST_REQUIRE_THROW(db::assure_sufficient_live_nodes(level, *f.erm, live), exceptions::unavailable_exception);
        }
        return result;
    };

    BOOST_REQUIRE(sufficient(cl::ONE, {f.k1}));
    BOOST_REQUIRE(!sufficient(cl::ONE, {}));
    BOOST_REQUIRE(sufficient(cl::LOCAL_ONE, {f.n3}));
    BOOST_REQUIRE(!sufficient(cl::LOCAL_ONE, {f.k1}));
    BOOST_REQUIRE(sufficient(cl::QUORUM, {f.n1, f.m1, f.m2, f.k1}));
    BOOST_REQUIRE(!sufficient(cl::QUORUM, {f.n1, f.n2, f.n3}));
    BOOST_REQUIRE(sufficient(cl::LOCAL_QUORUM, {f.n1, f.n2}));
    BOOST_REQUIRE(!sufficient(cl::LOCAL_QUORUM, {f.n1, f.m1, f.m2, f.m3, f.k1}));
    BOOST_REQUIRE(sufficient(cl::ALL, f.all_live()));
    BOOST_REQUIRE(!sufficient(cl::ALL, {f.n1, f.n2, f.n3, f.m1, f.m2, f.m3}));

    // The exception carries the level's own required/alive counts.
    try {
        db::assure_sufficient_live_nodes(cl::QUORUM, *f.erm, {f.n1, f.n2, f.n3});
        BOOST_FAIL("expected unavailable_exception");
    } catch (const exceptions::unavailable_exception& e) {
        BOOST_REQUIRE_EQUAL(e.required, 4);
        BOOST_REQUIRE_EQUAL(e.alive, 3);
    }
}

BOOST_AUTO_TEST_SUITE_END()
