/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>
#include <bit>

#include <seastar/core/sharded.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "locator/tablets.hh"
#include "service/tablet_allocator.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "locator/load_sketch.hh"
#include "test/lib/topology_builder.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "service/topology_mutation.hh"
#include "tools/utils.hh"

#include "test/perf/perf.hh"
#include "test/lib/log.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/key_utils.hh"

using namespace locator;
using namespace replica;
using namespace service;
using namespace tools::utils;

namespace bpo = boost::program_options;

static seastar::abort_source aborted;

static const sstring dc = "dc1";

static
cql_test_config tablet_cql_test_config() {
    cql_test_config c;
    return c;
}

static
future<table_id> add_table(cql_test_env& e, sstring test_ks_name = "") {
    auto id = table_id(utils::UUID_gen::get_time_UUID());
    co_await e.create_table([&] (std::string_view ks_name) {
        if (!test_ks_name.empty()) {
            ks_name = test_ks_name;
        }
        return *schema_builder(this_smp_shard_count(), ks_name, id.to_sstring(), id)
                .with_column("p1", utf8_type, column_kind::partition_key)
                .with_column("r1", int32_type)
                .build();
    });
    co_return id;
}

// Run in a seastar thread
static
sstring add_keyspace(cql_test_env& e, std::unordered_map<sstring, int> dc_rf, int initial_tablets = 0) {
    static std::atomic<int> ks_id = 0;
    auto ks_name = fmt::format("keyspace{}", ks_id.fetch_add(1));
    sstring rf_options;
    for (auto& [dc, rf] : dc_rf) {
        rf_options += format(", '{}': {}", dc, rf);
    }
    e.execute_cql(fmt::format("create keyspace {} with replication = {{'class': 'NetworkTopologyStrategy'{}}}"
                              " and tablets = {{'enabled': true, 'initial': {}}}",
                              ks_name, rf_options, initial_tablets)).get();
    return ks_name;
}

static
size_t get_tablet_count(const tablet_metadata& tm) {
    size_t count = 0;
    for (const auto& [table, tmap] : tm.all_tables_ungrouped()) {
        count += std::accumulate(tmap->tablets().begin(), tmap->tablets().end(), size_t(0),
                                 [] (size_t accumulator, const locator::tablet_info& info) {
                                     return accumulator + info.replicas.size();
                                 });
    }
    return count;
}

static
future<> apply_resize_plan(token_metadata& tm, const migration_plan& plan) {
    for (auto [table_id, resize_decision] : plan.resize_plan().resize) {
        co_await tm.tablets().mutate_tablet_map_async(table_id, [&resize_decision] (tablet_map& tmap) {
            resize_decision.sequence_number = tmap.resize_decision().sequence_number + 1;
            tmap.set_resize_decision(resize_decision);
            return make_ready_future();
        });
    }
}

// Reflects the plan in a given token metadata as if the migrations were fully executed.
static
future<> apply_plan(token_metadata& tm, const migration_plan& plan, locator::load_stats& load_stats) {
    for (auto&& mig : plan.migrations()) {
        co_await tm.tablets().mutate_tablet_map_async(mig.tablet.table, [&mig] (tablet_map& tmap) {
            auto tinfo = tmap.get_tablet_info(mig.tablet.tablet);
            tinfo.replicas = replace_replica(tinfo.replicas, mig.src, mig.dst);
            tmap.set_tablet(mig.tablet.tablet, tinfo);
            return make_ready_future();
        });
        // Move tablet size in load_stats to account for the migration
        if (mig.src && mig.dst && mig.src->host != mig.dst->host) {
            auto& tmap = tm.tablets().get_tablet_map(mig.tablet.table);
            const dht::token_range trange = tmap.get_token_range(mig.tablet.tablet);
            lw_shared_ptr<locator::load_stats> new_stats = load_stats.migrate_tablet_size(mig.src->host, mig.dst->host, mig.tablet, trange);

            if (new_stats) {
                load_stats = std::move(*new_stats);
            } else {
                throw std::runtime_error(format("Unable to migrate tablet size in load_stats for migration: {}", mig));
            }
        }
    }
    co_await apply_resize_plan(tm, plan);
}

using seconds_double = std::chrono::duration<double>;

struct rebalance_stats {
    seconds_double elapsed_time = seconds_double(0);
    seconds_double max_rebalance_time = seconds_double(0);
    uint64_t rebalance_count = 0;
    uint64_t splits_finalized = 0;
    uint64_t merges_finalized = 0;

    rebalance_stats& operator+=(const rebalance_stats& other) {
        elapsed_time += other.elapsed_time;
        max_rebalance_time = std::max(max_rebalance_time, other.max_rebalance_time);
        rebalance_count += other.rebalance_count;
        splits_finalized += other.splits_finalized;
        merges_finalized += other.merges_finalized;
        return *this;
    }
};

// Persists the token metadata, so that the table layer processes the changes, and
// returns a new guard so that later changes use a later timestamp.
static
group0_guard save_token_metadata(cql_test_env& e, group0_guard guard) {
    auto tm = e.local_token_metadata_ptr();

    e.get_topology_state_machine().local()._topology.version = tm->get_version();

    save_tablet_metadata(e.local_db(), tm->tablets(), guard.write_timestamp()).get();

    utils::chunked_vector<frozen_mutation> muts;
    muts.push_back(freeze(topology_mutation_builder(guard.write_timestamp())
                                  .set_version(tm->get_version())
                                  .build().to_mutation(db::system_keyspace::topology())));
    e.local_db().apply(muts, db::no_timeout).get();
    e.get_storage_service().local().update_tablet_metadata({}).get();

    release_guard(std::move(guard));
    abort_source as;
    return e.get_raft_group0_client().start_operation(as).get();
}

// Acknowledges split decisions on behalf of the replicas, which the load balancer
// waits for before finalizing a split, and lets the table layer see the decisions
// before the split is finalized, which it relies on.
static
void ack_splits(cql_test_env& e, group0_guard& guard, locator::load_stats& load_stats) {
    bool changed = false;
    for (const auto& [table, tmap] : e.local_token_metadata_ptr()->tablets().all_tables_ungrouped()) {
        const auto& decision = tmap->resize_decision();
        if (decision.is_split() && load_stats.tables[table].split_ready_seq_number != decision.sequence_number) {
            load_stats.tables[table].split_ready_seq_number = decision.sequence_number;
            changed = true;
        }
    }
    if (changed) {
        guard = save_token_metadata(e, std::move(guard));
    }
}

// Finalizes the splits and merges the plan asks for, the way the topology coordinator
// does, and carries the tablet sizes in load_stats over to the new tablets.
static
void finalize_resizes(cql_test_env& e, group0_guard& guard, const migration_plan& plan,
                      locator::load_stats& load_stats, rebalance_stats& stats) {
    const auto& tables = plan.resize_plan().finalize_resize;
    if (tables.empty()) {
        return;
    }

    auto& talloc = e.get_tablet_allocator().local();
    auto& stm = e.shared_token_metadata().local();
    auto old_tm = stm.get();

    for (auto table : tables) {
        auto tm = stm.get();
        const auto& old_tmap = tm->tablets().get_tablet_map(table);
        auto new_tmap = talloc.resize_tablets(tm, table).get();
        testlog.debug("Finalizing resize of table {}: {} => {} tablets", table, old_tmap.tablet_count(), new_tmap.tablet_count());
        if (new_tmap.tablet_count() > old_tmap.tablet_count()) {
            stats.splits_finalized++;
        } else {
            stats.merges_finalized++;
        }
        auto new_resize_decision = locator::resize_decision{};
        new_resize_decision.sequence_number = old_tmap.resize_decision().next_sequence_number();
        new_tmap.set_resize_decision(std::move(new_resize_decision));

        stm.mutate_token_metadata([table, &new_tmap] (token_metadata& tm) {
            tm.tablets().set_tablet_map(table, std::move(new_tmap));
            tm.set_version(tm.get_version() + 1);
            return make_ready_future<>();
        }).get();
    }

    // The table layer expects the tablet count to change by a factor of 2 at a time,
    // so the new maps have to be processed before the next resize.
    guard = save_token_metadata(e, std::move(guard));

    if (auto reconciled = load_stats.reconcile_tablets_resize(tables, *old_tm, *stm.get())) {
        load_stats = std::move(*reconciled);
    } else {
        testlog.warn("Unable to reconcile tablet sizes in load_stats after resizing {}", tables);
    }

    old_tm = nullptr;
    e.get_storage_service().local().local_topology_barrier().get();
}

static
rebalance_stats rebalance_tablets(cql_test_env& e, locator::load_stats& load_stats, std::unordered_set<host_id> skiplist = {}) {
    rebalance_stats stats;
    abort_source as;

    auto guard = e.get_raft_group0_client().start_operation(as).get();
    auto& talloc = e.get_tablet_allocator().local();
    auto& stm = e.shared_token_metadata().local();

    // Sanity limit to avoid infinite loops.
    // The x10 factor is arbitrary, it's there to account for more complex schedules than direct migration.
    auto max_iterations = 1 + get_tablet_count(stm.get()->tablets()) * 10;

    for (size_t i = 0; i < max_iterations; ++i) {
        auto prev_lb_stats = *talloc.stats().for_dc(dc);
        auto load_stats_p = make_lw_shared<locator::load_stats>(load_stats);
        auto start_time = std::chrono::steady_clock::now();

        auto plan = talloc.balance_tablets(stm.get(), nullptr, nullptr, load_stats_p, skiplist).get();

        auto end_time = std::chrono::steady_clock::now();
        auto lb_stats = *talloc.stats().for_dc(dc) - prev_lb_stats;

        auto elapsed = std::chrono::duration_cast<seconds_double>(end_time - start_time);
        rebalance_stats iteration_stats = {
            .elapsed_time = elapsed,
            .max_rebalance_time = elapsed,
            .rebalance_count = 1,
        };
        stats += iteration_stats;
        testlog.debug("Rebalance iteration {} took {:.3f} [s]: mig={}, bad={}, first_bad={}, eval={}, skiplist={}, skip: (load={}, rack={}, node={})",
                      i + 1, elapsed.count(),
                      lb_stats.migrations_produced,
                      lb_stats.bad_migrations,
                      lb_stats.bad_first_candidates,
                      lb_stats.candidates_evaluated,
                      lb_stats.migrations_from_skiplist,
                      lb_stats.migrations_skipped,
                      lb_stats.tablets_skipped_rack,
                      lb_stats.tablets_skipped_node);

        if (plan.empty()) {
            // We should not introduce inconsistency between on-disk state and in-memory state
            // as that may violate invariants and cause failures in later operations
            // causing test flakiness.
            save_tablet_metadata(e.local_db(), stm.get()->tablets(), guard.write_timestamp()).get();
            e.get_storage_service().local().update_tablet_metadata({}).get();

            testlog.info("Rebalance took {:.3f} [s] after {} iteration(s)", stats.elapsed_time.count(), i + 1);
            return stats;
        }
        stm.mutate_token_metadata([&] (token_metadata& tm) {
            return apply_plan(tm, plan, load_stats);
        }).get();
        ack_splits(e, guard, load_stats);
        finalize_resizes(e, guard, plan, load_stats, stats);
    }
    throw std::runtime_error("rebalance_tablets(): convergence not reached within limit");
}

struct params {
    int iterations;
    int nodes;
    std::optional<int> tablets1;
    std::optional<int> tablets2;
    int rf1;
    int rf2;
    int shards;
    int scale1 = 1;
    int scale2 = 1;
    double tablet_size_deviation_factor = 0.5;
};

struct table_balance {
    double shard_overcommit;
    double best_shard_overcommit;
    double node_overcommit;
};

constexpr auto nr_tables = 2;

struct cluster_balance {
    table_balance tables[nr_tables];
};

struct results {
    cluster_balance init;
    cluster_balance worst;
    cluster_balance last;
    rebalance_stats stats;
};

template<>
struct fmt::formatter<table_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const table_balance& b, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{shard={} (best={}), node={}}}",
                              b.shard_overcommit, b.best_shard_overcommit, b.node_overcommit);
    }
};

template<>
struct fmt::formatter<cluster_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const cluster_balance& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{table1={}, table2={}}}", r.tables[0], r.tables[1]);
    }
};

template<>
struct fmt::formatter<params> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const params& p, FormatContext& ctx) const {
        auto tablets1_per_shard = double(p.tablets1.value_or(0)) * p.rf1 / (p.nodes * p.shards);
        auto tablets2_per_shard = double(p.tablets2.value_or(0)) * p.rf2 / (p.nodes * p.shards);
        return fmt::format_to(ctx.out(), "{{iterations={}, nodes={}, tablets1={} ({:0.1f}/sh), tablets2={} ({:0.1f}/sh), rf1={}, rf2={}, shards={}, tablet_size_deviation_factor={}}}",
                         p.iterations, p.nodes,
                         p.tablets1.value_or(0), tablets1_per_shard,
                         p.tablets2.value_or(0), tablets2_per_shard,
                         p.rf1, p.rf2, p.shards, p.tablet_size_deviation_factor);
    }
};

class tablet_size_generator {
    std::default_random_engine _rnd_engine{std::random_device{}()};
    std::normal_distribution<> _dist;
public:
    explicit tablet_size_generator(double deviation_factor)
        : _dist(default_target_tablet_size, default_target_tablet_size * deviation_factor) {
    }

    uint64_t generate() {
        // We can't have a negative tablet size, which is why we need to minimize it to 0 (with std::max()).
        // One consequence of this is that the average generated tablet size will actually
        // be larger than default_target_tablet_size.
        // This will be especially pronounced as deviation_factor gets larger. For instance:
        //
        // deviation_factor | avg tablet size
        // -----------------+----------------------------------------
        //              1   | default_target_tablet_size * 1.08
        //              1.5 | default_target_tablet_size * 1.22
        //              2   | default_target_tablet_size * 1.39
        //              3   | default_target_tablet_size * 1.76
        return std::max(0.0, _dist(_rnd_engine));
    }
};

void generate_tablet_sizes(double tablet_size_deviation_factor, locator::load_stats& stats, locator::shared_token_metadata& stm) {
    tablet_size_generator tsg(tablet_size_deviation_factor);
    for (auto&& [table, tmap] : stm.get()->tablets().all_tables_ungrouped()) {
        tmap->for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
            for (const auto& replica : ti.replicas) {
                const uint64_t tablet_size = tsg.generate();
                locator::range_based_tablet_id rb_tid {table, tmap->get_token_range(tid)};
                stats.tablet_stats[replica.host].tablet_sizes[rb_tid.table][rb_tid.range] = tablet_size;
                testlog.trace("Generated tablet size {} for {}:{}", tablet_size, table, tid);
            }
            return make_ready_future<>();
        }).get();
    }
}

future<results> test_load_balancing_with_many_tables(params p, bool tablet_aware) {
    auto cfg = tablet_cql_test_config();
    results global_res;
    co_await do_with_cql_env_thread([&] (auto& e) {
        SCYLLA_ASSERT(p.nodes > 0);
        SCYLLA_ASSERT(p.rf1 > 0);
        SCYLLA_ASSERT(p.rf2 > 0);

        const size_t n_hosts = p.nodes;
        const size_t rf1 = p.rf1;
        const size_t rf2 = p.rf2;
        const shard_id shard_count = p.shards;
        const int cycles = p.iterations;
        const uint64_t shard_capacity = default_target_tablet_size * 100;

        struct host_info {
            host_id id;
            endpoint_dc_rack dc_rack;
        };

        topology_builder topo(e);
        std::vector<endpoint_dc_rack> racks;
        std::vector<host_info> hosts;
        locator::load_stats stats;

        auto populate_racks = [&] (const size_t count) {
            SCYLLA_ASSERT(count > 0);
            racks.push_back(topo.rack());
            for (size_t i = 0; i < count - 1; ++i) {
                racks.push_back(topo.start_new_rack());
            }
        };

        const sstring dc1 = topo.dc();
        populate_racks(rf1);

        // The rack for which we output stats
        sstring test_rack = racks.front().rack;

        const size_t rack_count = racks.size();
        std::unordered_map<sstring, uint64_t> rack_capacity;

        auto add_host = [&] (endpoint_dc_rack dc_rack) {
            auto host = topo.add_node(service::node_state::normal, shard_count, dc_rack);
            hosts.emplace_back(host, dc_rack);
            const uint64_t capacity = shard_capacity * shard_count;
            stats.capacity[host] = capacity;
            stats.tablet_stats[host].effective_capacity = capacity;
            rack_capacity[dc_rack.rack] += capacity;
            testlog.info("Added new node: {} / {}:{}", host, dc_rack.dc, dc_rack.rack);
        };

        for (size_t i = 0; i < n_hosts; ++i) {
            add_host(racks[i % rack_count]);
        }

        auto& stm = e.shared_token_metadata().local();

        auto bootstrap = [&] (endpoint_dc_rack dc_rack) {
            add_host(std::move(dc_rack));
            global_res.stats += rebalance_tablets(e, stats);
        };

        auto decommission = [&] (host_id host) {
            const auto it = std::ranges::find_if(hosts, [&] (const host_info& info) {
                return info.id == host;
            });
            if (it == hosts.end()) {
                throw std::runtime_error(format("No such host: {}", host));
            }
            topo.set_node_state(host, service::node_state::decommissioning);
            global_res.stats += rebalance_tablets(e, stats);
            if (stm.get()->tablets().has_replica_on(host)) {
                throw std::runtime_error(format("Host {} still has replicas!", host));
            }
            topo.set_node_state(host, service::node_state::left);
            testlog.info("Node decommissioned: {}", host);
            rack_capacity[it->dc_rack.rack] -= stats.capacity.at(host);
            hosts.erase(it);
            stats.tablet_stats.erase(host);
        };

        auto ks1 = add_keyspace(e, {{dc1, rf1}}, p.tablets1.value_or(1));
        auto ks2 = add_keyspace(e, {{dc1, rf2}}, p.tablets2.value_or(1));
        auto id1 = add_table(e, ks1).get();
        auto id2 = add_table(e, ks2).get();
        schema_ptr s1 = e.local_db().find_schema(id1);
        schema_ptr s2 = e.local_db().find_schema(id2);

        generate_tablet_sizes(p.tablet_size_deviation_factor, stats, stm);

        // Compute table size per rack, and collect all tablets per rack
        std::unordered_map<sstring, std::unordered_map<table_id, uint64_t>> table_sizes_per_rack;
        std::unordered_map<sstring, std::unordered_map<table_id, std::vector<uint64_t>>> tablet_sizes_in_rack;
        for (auto& [host, tls] : stats.tablet_stats) {
            auto host_i = std::ranges::find(hosts, host, &host_info::id);
            if (host_i == hosts.end()) {
                throw std::runtime_error(format("Host {} not found in hosts", host));
            }
            auto rack = host_i->dc_rack.rack;
            for (auto& [table, ranges] : tls.tablet_sizes) {
                for (auto& [trange, tablet_size] : ranges) {
                    table_sizes_per_rack[rack][table] += tablet_size;
                    tablet_sizes_in_rack[rack][table].push_back(tablet_size);
                }
            }
        }

        // Sort the tablet sizes per rack in descending order
        for (auto& [rack, tables] : tablet_sizes_in_rack) {
            for (auto& [table, tablets] : tables) {
                std::ranges::sort(tablets, std::greater<uint64_t>());
            }
        }

        struct node_used_size {
            host_id host;
            uint64_t used = 0;
        };

        // Compute best shard overcommit per table per rack
        std::unordered_map<sstring, std::unordered_map<table_id, double>> best_shard_overcommit_per_rack;
        auto compute_best_overcommit = [&] () {
            auto node_size_compare = [] (const node_used_size& lhs, const node_used_size& rhs) {
                return lhs.used > rhs.used;
            };

            for (auto& all_dc_rack : racks) {
                auto rack = all_dc_rack.rack;

                // Allocate tablet sizes to nodes
                for (auto& [table, tablet_sizes]: tablet_sizes_in_rack.at(rack)) {
                    load_sketch load(e.local_token_metadata_ptr(), make_lw_shared<locator::load_stats>(stats));

                    // Add nodes to load_sketch and to the nodes_used heap
                    std::vector<node_used_size> nodes_used;
                    for (const auto& [host_id, host_dc_rack] : hosts) {
                        if (rack == host_dc_rack.rack) {
                            load.ensure_node(host_id);
                            nodes_used.push_back({host_id, 0});
                        }
                    }

                    // Allocate tablets to nodes/shards
                    for (uint64_t tablet_size : tablet_sizes) {
                        std::pop_heap(nodes_used.begin(), nodes_used.end(), node_size_compare);
                        host_id add_to_host = nodes_used.back().host;
                        nodes_used.back().used += tablet_size;
                        std::push_heap(nodes_used.begin(), nodes_used.end(), node_size_compare);

                        // Add to the least loaded shard on the least loaded node
                        load.next_shard(add_to_host, 1, tablet_size);
                    }

                    // Get the best overcommit from all the nodes
                    min_max_tracker<locator::disk_usage::load_type> load_minmax;
                    for (const auto& n : nodes_used) {
                        load_minmax.update(load.get_shard_minmax(n.host));
                    }

                    const uint64_t table_size = table_sizes_per_rack.at(rack).at(table);
                    const double ideal_load = double(table_size) / rack_capacity.at(rack);
                    const double best_overcommit = load_minmax.max() / ideal_load;
                    best_shard_overcommit_per_rack[rack][table] = best_overcommit;
                }
            }
        };

        auto check_balance = [&] () -> cluster_balance {
            cluster_balance res;

            testlog.debug("tablet metadata: {}", stm.get()->tablets());

            compute_best_overcommit();

            auto load_stats_p = make_lw_shared<locator::load_stats>(stats);
            int table_index = 0;
            for (auto s : {s1, s2}) {
                auto table = s->id();
                load_sketch load(stm.get(), load_stats_p);
                load.populate(std::nullopt, table).get();

                min_max_tracker<double> shard_overcommit_minmax;
                min_max_tracker<double> node_overcommit_minmax;
                auto rack = test_rack;
                auto table_size = table_sizes_per_rack.at(rack).at(table);
                auto ideal_load = double(table_size) / rack_capacity.at(rack);
                min_max_tracker<double> shard_load_minmax;
                min_max_tracker<double> node_load_minmax;
                for (auto [h, host_dc_rack] : hosts) {
                    if (host_dc_rack.rack != rack) {
                        continue;
                    }
                    auto minmax = load.get_shard_minmax(h);
                    auto node_load = load.get_load(h);
                    auto overcommit = double(minmax.max()) / ideal_load;
                    testlog.info("Load on host {} for table {}: total={}, min={}, max={}, spread={}, ideal={}, overcommit={}",
                                h, s->cf_name(), node_load, minmax.min(), minmax.max(), minmax.max() - minmax.min(), ideal_load, overcommit);
                    node_load_minmax.update(node_load);
                    shard_load_minmax.update(minmax.max());
                }

                auto shard_overcommit = shard_load_minmax.max() / ideal_load;
                auto best_shard_overcommit = best_shard_overcommit_per_rack.at(rack).at(table);
                testlog.info("Shard overcommit: {} best: {}", shard_overcommit, best_shard_overcommit);

                auto node_imbalance = node_load_minmax.max() - node_load_minmax.min();
                auto node_overcommit = node_load_minmax.max() / ideal_load;
                testlog.info("Node imbalance in min={}, max={}, spread={}, ideal={}, overcommit={}",
                            node_load_minmax.min(), node_load_minmax.max(), node_imbalance, ideal_load, node_overcommit);

                shard_overcommit_minmax.update(shard_overcommit);
                node_overcommit_minmax.update(node_overcommit);

                res.tables[table_index++] = {
                    .shard_overcommit = shard_overcommit_minmax.max(),
                    .best_shard_overcommit = best_shard_overcommit,
                    .node_overcommit = node_overcommit_minmax.max(),
                };
            }

            for (int i = 0; i < nr_tables; i++) {
                auto t = res.tables[i];
                global_res.worst.tables[i].shard_overcommit = std::max(global_res.worst.tables[i].shard_overcommit, t.shard_overcommit);
                global_res.worst.tables[i].node_overcommit = std::max(global_res.worst.tables[i].node_overcommit, t.node_overcommit);
            }

            testlog.info("Overcommit: {}", res);
            return res;
        };

        testlog.debug("tablet metadata: {}", stm.get()->tablets());

        e.get_tablet_allocator().local().set_use_table_aware_balancing(tablet_aware);

        check_balance();

        rebalance_tablets(e, stats);

        global_res.init = global_res.worst = check_balance();

        for (int i = 0; i < cycles; i++) {
            const auto [id, dc_rack] = hosts[0];

            bootstrap(dc_rack);
            check_balance();

            decommission(id);
            global_res.last = check_balance();
        }
    }, cfg);
    co_return global_res;
}

void test_parallel_scaleout(const bpo::variables_map& opts) {
    const shard_id shard_count = opts["shards"].as<int>();
    const int nr_tables = opts["tables"].as<int>();
    const int tablets_per_table = opts["tablets_per_table"].as<int>();
    const int nr_racks = opts["racks"].as<int>();
    const int initial_nodes = nr_racks * opts["nodes-per-rack"].as<int>();
    const int extra_nodes = nr_racks * opts["extra-nodes-per-rack"].as<int>();
    const double tablet_size_deviation_factor = opts["tablet-size-deviation-factor"].as<double>();

    auto cfg = tablet_cql_test_config();
    cfg.db_config->rf_rack_valid_keyspaces(true);
    results global_res;
    do_with_cql_env_thread([&] (auto& e) {
        topology_builder topo(e);
        locator::load_stats stats;

        std::vector<endpoint_dc_rack> racks;
        racks.push_back(topo.rack());
        for (int i = 1; i < nr_racks; ++i) {
            racks.push_back(topo.start_new_rack());
        }

        auto add_host = [&] (endpoint_dc_rack rack) {
            auto host = topo.add_node(service::node_state::normal, shard_count, rack);
            const uint64_t capacity = default_target_tablet_size * shard_count * 100;
            stats.capacity[host] = capacity;
            stats.tablet_stats[host].effective_capacity = capacity;
            testlog.info("Added new node: {}", host);
        };

        auto add_hosts = [&] (int n) {
            for (int i = 0; i < n; ++i) {
                add_host(racks[i % racks.size()]);
            }
        };

        add_hosts(initial_nodes);

        testlog.info("Creating schema");
        auto ks1 = add_keyspace(e, {{topo.dc(), nr_racks}}, tablets_per_table);
        seastar::parallel_for_each(std::views::iota(0, nr_tables), [&] (int) -> future<> {
            return add_table(e, ks1).discard_result();
        }).get();

        generate_tablet_sizes(tablet_size_deviation_factor, stats, e.shared_token_metadata().local());

        testlog.info("Initial rebalancing");
        rebalance_tablets(e, stats);

        testlog.info("Scaleout");
        add_hosts(extra_nodes);
        global_res.stats += rebalance_tablets(e, stats);
    }, cfg).get();
}

// Simulates tables growing and then shrinking back to their initial size, to observe how
// tablet sizing follows: the per-shard tablet count, the average tablet size, and how many
// splits and merges it takes. Meant for evaluating capacity-relative tablet sizing and the
// per-shard tablet count budget under different settings of
// tablet_size_fraction_of_shard_capacity, tablets_per_shard_goal and
// tablets_per_shard_budget_factor.
// See docs/dev/multi-dimensional-tablet-load-balancing.md.
void test_table_growth(const bpo::variables_map& opts) {
    const shard_id shard_count = opts["shards"].as<int>();
    const int nr_tables = opts["tables"].as<int>();
    const int nr_racks = opts["racks"].as<int>();
    const int nodes_per_rack = opts["nodes-per-rack"].as<int>();
    const int steps = opts["steps"].as<int>();
    const double growth = opts["growth"].as<double>();
    const uint64_t shard_capacity = uint64_t(opts["shard-capacity-gb"].as<int>()) << 30;
    const uint64_t initial_table_size = uint64_t(opts["initial-table-size-mb"].as<int>()) << 20;
    const double size_skew = opts["size-skew"].as<double>();
    const double workload_skew = opts["workload-skew"].as<double>();

    auto cfg = tablet_cql_test_config();
    cfg.db_config->rf_rack_valid_keyspaces(true);
    // The sizing options default to db::config's defaults unless given.
    if (opts.contains("tablets-per-shard-goal")) {
        cfg.db_config->tablets_per_shard_goal(opts["tablets-per-shard-goal"].as<int>());
    }
    if (opts.contains("tablets-per-shard-budget-factor")) {
        cfg.db_config->tablets_per_shard_budget_factor(opts["tablets-per-shard-budget-factor"].as<double>());
    }
    if (opts.contains("tablet-size-fraction-of-shard-capacity")) {
        cfg.db_config->tablet_size_fraction_of_shard_capacity(opts["tablet-size-fraction-of-shard-capacity"].as<double>());
    }
    if (opts.contains("tablets-initial-scale-factor")) {
        cfg.db_config->tablets_initial_scale_factor(opts["tablets-initial-scale-factor"].as<double>());
    }
    if (opts.contains("target-tablet-size-mb")) {
        cfg.db_config->target_tablet_size_in_bytes(uint64_t(opts["target-tablet-size-mb"].as<int>()) << 20);
    }

    do_with_cql_env_thread([&] (auto& e) {
        topology_builder topo(e);
        locator::load_stats stats;
        auto& stm = e.shared_token_metadata().local();

        std::vector<endpoint_dc_rack> racks;
        racks.push_back(topo.rack());
        for (int i = 1; i < nr_racks; ++i) {
            racks.push_back(topo.start_new_rack());
        }
        for (int i = 0; i < nr_racks * nodes_per_rack; ++i) {
            auto host = topo.add_node(service::node_state::normal, shard_count, racks[i % racks.size()]);
            const uint64_t capacity = shard_capacity * shard_count;
            stats.capacity[host] = capacity;
            stats.tablet_stats[host].effective_capacity = capacity;
        }
        const size_t total_shards = size_t(nr_racks) * nodes_per_rack * shard_count;
        const size_t rf = nr_racks;

        struct table_info {
            table_id id;
            double size_weight; // Size relative to initial_table_size.
            double density;     // Workload rate per byte stored.
        };
        std::vector<table_info> tables;
        auto ks = add_keyspace(e, {{topo.dc(), nr_racks}});
        for (int i = 0; i < nr_tables; ++i) {
            tables.push_back(table_info{
                .id = add_table(e, ks).get(),
                .size_weight = std::exp2(tests::random::get_real<double>(0, size_skew, tests::random::gen())),
                .density = std::exp2(tests::random::get_real<double>(0, workload_skew, tests::random::gen())),
            });
            testlog.info("table{}: {} size weight {:.2f}, workload density {:.2f}", i, tables.back().id,
                         tables.back().size_weight, tables.back().density);
        }

        // Sets the size and workload of every table for the given growth scale, spreading
        // the size evenly over the table's current tablets.
        auto set_sizes = [&] (double scale) {
            for (auto& t : tables) {
                const uint64_t size = initial_table_size * t.size_weight * scale;
                stats.tables[t.id].size_in_bytes = size;
                stats.table_workload[t.id] = uint64_t(size * t.density);
                auto& tmap = stm.get()->tablets().get_tablet_map(t.id);
                for (auto& [host, host_stats] : stats.tablet_stats) {
                    host_stats.tablet_sizes.erase(t.id);
                }
                tmap.for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
                    for (const auto& replica : ti.replicas) {
                        stats.tablet_stats[replica.host].tablet_sizes[t.id][tmap.get_token_range(tid)] = size / tmap.tablet_count();
                    }
                    return make_ready_future<>();
                }).get();
            }
        };

        // Counts the times a table's tablet count changed direction. Growing and then shrinking
        // accounts for one per table whose count followed its size; anything beyond that is churn.
        std::vector<size_t> prev_counts(tables.size());
        std::vector<int> last_direction(tables.size());
        size_t reversals = 0;
        rebalance_stats total;

        auto run_step = [&] (int step, double scale) {
            set_sizes(scale);
            auto res = rebalance_tablets(e, stats);
            total += res;

            auto& tmeta = stm.get()->tablets();
            uint64_t total_size = 0;
            size_t total_tablets = 0;
            min_max_tracker<size_t> count_minmax;
            for (size_t i = 0; i < tables.size(); ++i) {
                const auto count = tmeta.get_tablet_map(tables[i].id).tablet_count();
                total_size += stats.tables[tables[i].id].size_in_bytes;
                total_tablets += count;
                count_minmax.update(count);
                if (prev_counts[i] && count != prev_counts[i]) {
                    int direction = count > prev_counts[i] ? 1 : -1;
                    if (last_direction[i] && direction != last_direction[i]) {
                        reversals++;
                    }
                    last_direction[i] = direction;
                }
                prev_counts[i] = count;
            }

            testlog.info("step {:3d} scale {:9.3f}: utilization {:5.1f}%, tablets/shard {:6.1f}, avg tablet size {:8.1f} MB, "
                         "tablets per table [{}, {}], splits {}, merges {}, rebalance time {:.3f} [s]",
                         step, scale,
                         100.0 * total_size * rf / (total_shards * shard_capacity),
                         double(total_tablets) * rf / total_shards,
                         double(total_size) / std::max<size_t>(total_tablets, 1) / (1 << 20),
                         count_minmax.min(), count_minmax.max(),
                         res.splits_finalized, res.merges_finalized, res.elapsed_time.count());
        };

        int step = 0;
        for (int i = 0; i <= steps; ++i) {
            run_step(step++, std::pow(growth, i));
        }
        for (int i = steps - 1; i >= 0; --i) {
            run_step(step++, std::pow(growth, i));
        }

        testlog.info("Total: splits {}, merges {}, tablet count direction reversals {}, rebalance time {:.3f} [s] (max {:.3f} [s])",
                     total.splits_finalized, total.merges_finalized, reversals,
                     total.elapsed_time.count(), total.max_rebalance_time.count());
    }, cfg).get();
}

future<> run_simulation(const params& p, const sstring& name = "") {
    testlog.info("[run {}] params: {}", name, p);

    auto total_tablet_count = p.tablets1.value_or(0) * p.rf1 + p.tablets2.value_or(0) * p.rf2;
    testlog.info("[run {}] tablet count: {}", name, total_tablet_count);
    testlog.info("[run {}] tablet count / shard: {:.3f}", name, double(total_tablet_count) / (p.nodes * p.shards));

    auto res = co_await test_load_balancing_with_many_tables(p, true);
    testlog.info("[run {}] Overcommit       : init : {}", name, res.init);
    testlog.info("[run {}] Overcommit       : worst: {}", name, res.worst);
    testlog.info("[run {}] Overcommit       : last : {}", name, res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 res.stats.elapsed_time.count(), res.stats.max_rebalance_time.count(), res.stats.rebalance_count);

    if (res.stats.elapsed_time > seconds_double(1)) {
        testlog.warn("[run {}] Scheduling took longer than 1s!", name);
    }

    auto old_res = co_await test_load_balancing_with_many_tables(p, false);
    testlog.info("[run {}] Overcommit (old) : init : {}", name, old_res.init);
    testlog.info("[run {}] Overcommit (old) : worst: {}", name, old_res.worst);
    testlog.info("[run {}] Overcommit (old) : last : {}", name, old_res.last);
    testlog.info("[run {}] Overcommit       : time : {:.3f} [s], max={:.3f} [s], count={}", name,
                 old_res.stats.elapsed_time.count(), old_res.stats.max_rebalance_time.count(), old_res.stats.rebalance_count);

    for (int i = 0; i < nr_tables; ++i) {
        if (res.worst.tables[i].shard_overcommit > old_res.worst.tables[i].shard_overcommit) {
            testlog.warn("[run {}] table{} shard overcommit worse!", name, i + 1);
        }
        auto overcommit = res.worst.tables[i].shard_overcommit;
        if (overcommit > 1.2) {
            testlog.warn("[run {}] table{} shard overcommit {:.4f} > 1.2!", name, i + 1, overcommit);
        }
    }
}

future<> run_simulations(const boost::program_options::variables_map& app_cfg) {
    for (auto i = 0; i < app_cfg["runs"].as<int>(); i++) {
        constexpr int MIN_RF = 1;
        constexpr int MAX_RF = 3;

        auto shards = 1 << tests::random::get_int(0, 8);
        auto rf1 = tests::random::get_int(MIN_RF, MAX_RF);
        // FIXME: Once we allow for RF <= #racks (and not just RF == #racks), we can randomize this RF too.
        // For now, the values must be equal.
        auto rf2 = rf1;
        auto scale1 = 1 << tests::random::get_int(0, 5);
        auto scale2 = 1 << tests::random::get_int(0, 5);
        auto nodes = tests::random::get_int(rf1 + rf2, 2 *  MAX_RF);
        // results in a deviation factor of 0.0 - 2.0
        auto tablet_size_deviation_factor = tests::random::get_int(0, 200) / 100.0;

        params p {
            .iterations = app_cfg["iterations"].as<int>(),
            .nodes = nodes,
            .tablets1 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf1) * scale1),
            .tablets2 = std::bit_ceil<size_t>(div_ceil(shards * nodes, rf2) * scale2),
            .rf1 = rf1,
            .rf2 = rf2,
            .shards = shards,
            .scale1 = scale1,
            .scale2 = scale2,
            .tablet_size_deviation_factor = tablet_size_deviation_factor
        };

        auto name = format("#{}", i);
        co_await run_simulation(p, name);
    }
}

namespace perf {

void run_add_dec(const bpo::variables_map& opts) {
    if (opts.contains("runs")) {
        run_simulations(opts).get();
    } else {
        params p {
            .iterations = opts["iterations"].as<int>(),
            .nodes = opts["nodes"].as<int>(),
            .tablets1 = opts["tablets1"].as<int>(),
            .tablets2 = opts["tablets2"].as<int>(),
            .rf1 = opts["rf1"].as<int>(),
            .rf2 = opts["rf2"].as<int>(),
            .shards = opts["shards"].as<int>(),
            .tablet_size_deviation_factor = opts["tablet-size-deviation-factor"].as<double>(),
        };
        run_simulation(p).get();
    }
}

using operation_func = std::function<void(const bpo::variables_map&)>;

const std::vector<operation_option> global_options {};
const std::vector<operation_option> global_positional_options{};

const std::map<operation, operation_func> operations_with_func{
    {
        {{"rolling-add-dec",
         "Sequence of bootstraps and decommissions with two tables",
         "",
         {
            typed_option<int>("runs", "Number of simulation runs."),
            typed_option<int>("iterations", 8, "Number of topology-changing cycles in each run."),
            typed_option<int>("tablets1", 512, "Number of tablets for the first table."),
            typed_option<int>("tablets2", 128, "Number of tablets for the second table."),
            typed_option<int>("rf1", 1, "Replication factor for the first table."),
            typed_option<int>("rf2", 1, "Replication factor for the second table."),
            typed_option<int>("nodes", 3, "Number of nodes in the cluster."),
            typed_option<int>("shards", 30, "Number of shards per node."),
            typed_option<double>("tablet-size-deviation-factor", 0.5, "Deviation factor for the tablet size random generator.")
          }
        }, &run_add_dec},

        {{"parallel-scaleout",
         "Simulates a single scale-out involving simultaneous addition of multiple nodes per rack",
         "",
         {
            typed_option<int>("tablets_per_table", 256, "Number of tablets per table."),
            typed_option<int>("tables", 70, "Table count."),
            typed_option<int>("nodes-per-rack", 5, "Number of initial nodes per rack."),
            typed_option<int>("extra-nodes-per-rack", 3, "Number of nodes to add per rack."),
            typed_option<int>("racks", 2, "Number of racks."),
            typed_option<int>("shards", 88, "Number of shards per node."),
            typed_option<double>("tablet-size-deviation-factor", 0.5, "Deviation factor for the tablet size random generator.")
          }
        }, &test_parallel_scaleout},

        {{"table-growth",
         "Simulates tables growing and shrinking back, to evaluate how tablet sizing follows",
         "",
         {
            typed_option<int>("tables", 10, "Table count."),
            typed_option<int>("racks", 3, "Number of racks, which is also the replication factor."),
            typed_option<int>("nodes-per-rack", 1, "Number of nodes per rack."),
            typed_option<int>("shards", 8, "Number of shards per node."),
            typed_option<int>("shard-capacity-gb", 500, "Disk capacity per shard, in GB."),
            typed_option<int>("initial-table-size-mb", 1024, "Initial size of the smallest table, in MB."),
            typed_option<double>("size-skew", 4, "Table sizes are spread log-uniformly over [1, 2^size-skew] times the initial size."),
            typed_option<double>("workload-skew", 10, "Workload densities are spread log-uniformly over [1, 2^workload-skew]."),
            typed_option<int>("steps", 8, "Number of growth steps, followed by as many shrink steps."),
            typed_option<double>("growth", 1.5, "Factor by which every table grows in each growth step."),
            typed_option<int>("tablets-per-shard-goal", "Value of tablets_per_shard_goal."),
            typed_option<double>("tablets-per-shard-budget-factor", "Value of tablets_per_shard_budget_factor."),
            typed_option<double>("tablet-size-fraction-of-shard-capacity", "Value of tablet_size_fraction_of_shard_capacity."),
            typed_option<double>("tablets-initial-scale-factor", "Value of tablets_initial_scale_factor."),
            typed_option<int>("target-tablet-size-mb", "Value of target_tablet_size_in_bytes, in MB."),
          }
        }, &test_table_growth},
    }
};

int scylla_tablet_load_balancing_main(int argc, char** argv) {
    const auto operations = operations_with_func | std::views::keys | std::ranges::to<std::vector>();
    tool_app_template::config app_cfg{
            .name = "perf-load-balancing",
            .description = "Tests tablet load balancer in various scenarios",
            .logger_name = testlog.name(),
            .lsa_segment_pool_backend_size_mb = 100,
            .operations = std::move(operations),
            .global_options = &global_options,
            .global_positional_options = &global_positional_options,
            .db_cfg_ext = db_config_and_extensions()
    };
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [] (const operation& operation, const bpo::variables_map& app_config) {
        auto stop_test = defer([] noexcept {
            aborted.request_abort();
        });
        try {
            operations_with_func.at(operation)(app_config);
            return 0;
        } catch (seastar::abort_requested_exception&) {
            // Ignore
        }
        return 1;
    });
}

} // namespace perf
