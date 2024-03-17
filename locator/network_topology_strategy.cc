/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <functional>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "locator/network_topology_strategy.hh"
#include "locator/load_sketch.hh"
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptors.hpp>
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"
#include "utils/hash.hh"

namespace std {
template<>
struct hash<locator::endpoint_dc_rack> {
    size_t operator()(const locator::endpoint_dc_rack& v) const {
        return utils::tuple_hash()(std::tie(v.dc, v.rack));
    }
};
}

namespace locator {

network_topology_strategy::network_topology_strategy(replication_strategy_params params) :
        abstract_replication_strategy(params,
                                      replication_strategy_type::network_topology) {
    auto opts = _config_options;
    process_tablet_options(*this, opts, params);

    size_t rep_factor = 0;
    for (auto& config_pair : opts) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        //
        // FIXME!!!
        // The first option we get at the moment is a class name. Skip it!
        //
        if (boost::iequals(key, "class")) {
            continue;
        }

        if (boost::iequals(key, "replication_factor")) {
            throw exceptions::configuration_exception(
                "replication_factor is an option for SimpleStrategy, not "
                "NetworkTopologyStrategy");
        }

        auto rf = parse_replication_factor(val);
        rep_factor += rf;
        _dc_rep_factor.emplace(key, rf);
        _datacenteres.push_back(key);
    }

    _rep_factor = rep_factor;

    rslogger.debug("Configured datacenter replicas are: {}", _dc_rep_factor);
}

using endpoint_dc_rack_set = std::unordered_set<endpoint_dc_rack>;

class natural_endpoints_tracker {
    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    struct data_center_endpoints {
        /** List accepted endpoints get pushed into. */
        host_id_set& _endpoints;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names aren't a problem.
         */
        endpoint_dc_rack_set& _racks;

        /** Number of replicas left to fill from this DC. */
        size_t _rf_left;
        ssize_t _acceptable_rack_repeats;

        data_center_endpoints(size_t rf, size_t rack_count, size_t node_count, host_id_set& endpoints, endpoint_dc_rack_set& racks)
            : _endpoints(endpoints)
            , _racks(racks)
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            , _rf_left(std::min(rf, node_count))
            // If there aren't enough racks in this DC to fill the RF, we'll still use at least one node from each rack,
            // and the difference is to be filled by the first encountered nodes.
            , _acceptable_rack_repeats(rf - rack_count)
        {}

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the endpoints set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        bool add_endpoint_and_check_if_done(const host_id& ep, const endpoint_dc_rack& location) {
            if (done()) {
                return false;
            }

            if (_racks.emplace(location).second) {
                // New rack.
                --_rf_left;
                auto added = _endpoints.insert(ep).second;
                if (!added) {
                    throw std::runtime_error(fmt::format("Topology error: found {} in more than one rack", ep));
                }
                return done();
            }

            /**
             * Ensure we don't allow too many endpoints in the same rack, i.e. we have
             * minimum current rf_left + 1 distinct racks. See above, _acceptable_rack_repeats
             * is defined as RF - rack_count, i.e. how many nodes in a single rack we are ok
             * with.
             *
             * With RF = 3 and 2 Racks in DC,
             *
             * IP1, Rack1
             * IP2, Rack1
             * IP3, Rack1,    The line _acceptable_rack_repeats <= 0 will reject IP3.
             * IP4, Rack2
             *
             */
            if (_acceptable_rack_repeats <= 0) {
                // There must be rf_left distinct racks left, do not add any more rack repeats.
                return false;
            }

            if (!_endpoints.insert(ep).second) {
                // Cannot repeat a node.
                return false;
            }

            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --_acceptable_rack_repeats;
            --_rf_left;

            return done();
        }

        bool done() const {
            return _rf_left == 0;
        }
    };

    const token_metadata& _tm;
    const topology& _tp;
    std::unordered_map<sstring, size_t> _dc_rep_factor;

    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    host_id_set _replicas;
    // tracks the racks we have already placed replicas in
    endpoint_dc_rack_set _seen_racks;

    //
    // all endpoints in each DC, so we can check when we have exhausted all
    // the members of a DC
    //
    std::unordered_map<sstring, std::unordered_set<inet_address>> _all_endpoints;

    //
    // all racks in a DC so we can check when we have exhausted all racks in a
    // DC
    //
    std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>> _racks;

    std::unordered_map<sstring_view, data_center_endpoints> _dcs;

    size_t _dcs_to_fill;

public:
    natural_endpoints_tracker(const token_metadata& tm, const std::unordered_map<sstring, size_t>& dc_rep_factor)
        : _tm(tm)
        , _tp(_tm.get_topology())
        , _dc_rep_factor(dc_rep_factor)
        , _all_endpoints(_tp.get_datacenter_endpoints())
        , _racks(_tp.get_datacenter_racks())
    {
        // not aware of any cluster members
        assert(!_all_endpoints.empty() && !_racks.empty());

        auto size_for = [](auto& map, auto& k) {
            auto i = map.find(k);
            return i != map.end() ? i->second.size() : size_t(0);
        };

        // Create a data_center_endpoints object for each non-empty DC.
        for (auto& [dc, rf] : _dc_rep_factor) {
            auto node_count = size_for(_all_endpoints, dc);

            if (rf == 0 || node_count == 0) {
                continue;
            }

            _dcs.emplace(dc, data_center_endpoints(rf, size_for(_racks, dc), node_count, _replicas, _seen_racks));
            _dcs_to_fill = _dcs.size();
        }
    }

    bool add_endpoint_and_check_if_done(host_id ep) {
        auto& loc = _tp.get_location(ep);
        auto i = _dcs.find(loc.dc);
        if (i != _dcs.end() && i->second.add_endpoint_and_check_if_done(ep, loc)) {
            --_dcs_to_fill;
        }
        return done();
    }

    bool done() const noexcept {
        return _dcs_to_fill == 0;
    }

    host_id_set& replicas() noexcept {
        return _replicas;
    }

    static void check_enough_endpoints(const token_metadata& tm, const std::unordered_map<sstring, size_t>& dc_rf) {
        const auto& dc_endpoints = tm.get_topology().get_datacenter_endpoints();
        auto endpoints_in = [&dc_endpoints](sstring dc) {
            auto i = dc_endpoints.find(dc);
            return i != dc_endpoints.end() ? i->second.size() : size_t(0);
        };
        for (const auto& [dc, rf] : dc_rf) {
            if (rf > endpoints_in(dc)) {
                throw exceptions::configuration_exception(fmt::format("Datacenter {} doesn't have enough nodes for replication_factor={}", dc, rf));
            }
        }
    }
};

future<host_id_set>
network_topology_strategy::calculate_natural_endpoints(
    const token& search_token, const token_metadata& tm) const {

    natural_endpoints_tracker tracker(tm, _dc_rep_factor);

    for (auto& next : tm.ring_range(search_token)) {
        co_await coroutine::maybe_yield();

        host_id ep = *tm.get_endpoint(next);
        if (tracker.add_endpoint_and_check_if_done(ep)) {
            break;
        }
    }

    co_return std::move(tracker.replicas());
}

void network_topology_strategy::validate_options(const gms::feature_service& fs) const {
    if(_config_options.empty()) {
        throw exceptions::configuration_exception("Configuration for at least one datacenter must be present");
    }
    validate_tablet_options(*this, fs, _config_options);
    auto tablet_opts = recognized_tablet_options();
    for (auto& c : _config_options) {
        if (tablet_opts.contains(c.first)) {
            continue;
        }
        if (c.first == sstring("replication_factor")) {
            throw exceptions::configuration_exception(
                "replication_factor is an option for simple_strategy, not "
                "network_topology_strategy");
        }
        parse_replication_factor(c.second);
    }
}

std::optional<std::unordered_set<sstring>> network_topology_strategy::recognized_options(const topology& topology) const {
    // We only allow datacenter names as options
    auto opts = topology.get_datacenters();
    opts.merge(recognized_tablet_options());
    return opts;
}

effective_replication_map_ptr network_topology_strategy::make_replication_map(table_id table, token_metadata_ptr tm) const {
    if (!uses_tablets()) {
        on_internal_error(rslogger, format("make_replication_map() called for table {} but replication strategy not configured to use tablets", table));
    }
    return do_make_replication_map(table, shared_from_this(), std::move(tm), _rep_factor);
}

//
// Try to use as many tablets initially, so that all shards in the current topology
// are covered with at least one tablet. In other words, the value is
//
//    initial_tablets = max(nr_shards_in(dc) / RF_in(dc) for dc in datacenters)
//

static unsigned calculate_initial_tablets_from_topology(const schema& s, const topology& topo, const std::unordered_map<sstring, size_t>& rf) {
    unsigned initial_tablets = std::numeric_limits<unsigned>::min();
    for (const auto& dc : topo.get_datacenter_endpoints()) {
        unsigned shards_in_dc = 0;
        unsigned rf_in_dc = 1;

        for (const auto& ep : dc.second) {
            const auto* node = topo.find_node(ep);
            if (node != nullptr) {
                shards_in_dc += node->get_shard_count();
            }
        }

        if (auto it = rf.find(dc.first); it != rf.end()) {
            rf_in_dc = it->second;
        }

        unsigned tablets_in_dc = rf_in_dc > 0 ? (shards_in_dc + rf_in_dc - 1) / rf_in_dc : 0;
        initial_tablets = std::max(initial_tablets, tablets_in_dc);
    }
    rslogger.debug("Estimated {} initial tablets for table {}.{}", initial_tablets, s.ks_name(), s.cf_name());
    return initial_tablets;
}

future<tablet_map> network_topology_strategy::allocate_tablets_for_new_table(schema_ptr s, token_metadata_ptr tm, unsigned initial_scale) const {
    auto tablet_count = get_initial_tablets();
    if (tablet_count == 0) {
        tablet_count = calculate_initial_tablets_from_topology(*s, tm->get_topology(), _dc_rep_factor) * initial_scale;
    }
    auto aligned_tablet_count = 1ul << log2ceil(tablet_count);
    if (tablet_count != aligned_tablet_count) {
        rslogger.info("Rounding up tablet count from {} to {} for table {}.{}", tablet_count, aligned_tablet_count, s->ks_name(), s->cf_name());
        tablet_count = aligned_tablet_count;
    }

    return reallocate_tablets(std::move(s), std::move(tm), tablet_count, nullptr);
}

future<tablet_map> network_topology_strategy::reallocate_tablets(schema_ptr s, token_metadata_ptr tm, size_t tablet_count, const tablet_map* cur_tablets) const {
    static thread_local std::default_random_engine rnd_engine{std::random_device{}()};

    natural_endpoints_tracker::check_enough_endpoints(*tm, _dc_rep_factor);

    auto tablets = cur_tablets ? *cur_tablets : tablet_map(tablet_count);
    load_sketch load(tm);
    co_await load.populate();

    const auto& topo = tm->get_topology();
    const auto dc_rack_nodes = topo.get_datacenter_rack_nodes();
    for (tablet_id tb : tablets.tablet_ids()) {
        tablet_replica_set replicas;
        struct rack_load {
            sstring rack_name;
            size_t load;
        };
        struct rack_load_cmp {
            bool operator()(const rack_load& a, const rack_load& b) const {
                return a.load > b.load;
            }
        };
        struct dc_rack_load {
            size_t node_count = 0;
            std::vector<rack_load> racks;
        };
        // Current load per dc/rack
        std::unordered_map<sstring, dc_rack_load> dc_rack_load_map;
        // Current replicas per dc/rack
        std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<locator::host_id>>> replicas_per_dc;
        if (cur_tablets) {
            replicas = cur_tablets->get_tablet_info(tb).replicas;
            for (const auto& tr : replicas) {
                const auto& node = topo.get_node(tr.host);
                replicas_per_dc[node.dc_rack().dc][node.dc_rack().rack].insert(tr.host);
                ++dc_rack_load_map[node.dc_rack().dc].node_count;
            }
            for (const auto& [dc, racks] : replicas_per_dc) {
                auto& dr_load = dc_rack_load_map[dc];
                dr_load.racks = boost::copy_range<std::vector<rack_load>>(racks | boost::adaptors::transformed([] (const auto& x) {
                    return rack_load(x.first, x.second.size());
                }));
            }
        } else {
            replicas.reserve(_rep_factor);
        }
        for (const auto& [dc, dc_rf] : _dc_rep_factor) {
            auto& dr_load = dc_rack_load_map[dc];
            if (dc_rf == dr_load.node_count) {
                continue;
            }

            if (dc_rf > dr_load.node_count) {
                auto cmp = [] (const rack_load& a, const rack_load& b) {
                    return a.load > b.load;
                };

                // Track new racks, with no replicas on them, in random order
                const auto& all_dc_racks = dc_rack_nodes.at(dc);
                auto rack_names = boost::copy_range<std::vector<sstring>>(all_dc_racks | boost::adaptors::map_keys);
                std::shuffle(rack_names.begin(), rack_names.end(), rnd_engine);
                std::make_heap(dr_load.racks.begin(), dr_load.racks.end(), cmp);
                for (const auto& rack : rack_names) {
                    if (!replicas_per_dc[dc].contains(rack)) {
                        dr_load.racks.emplace_back(rack, 0);
                        std::push_heap(dr_load.racks.begin(), dr_load.racks.end(), cmp);
                    }
                }
                // Track all nodes with no replicas on them, per rack.
                std::unordered_map<sstring, std::vector<locator::host_id>> candidate_nodes_per_rack;
                for (const auto& rack_name : rack_names) {
                    const auto& existing = replicas_per_dc[dc][rack_name];
                    auto& rack = candidate_nodes_per_rack[rack_name];
                    for (const auto& node : all_dc_racks.at(rack_name)) {
                        const auto& host_id = node->host_id();
                        if (!existing.contains(host_id)) {
                            rack.emplace_back(host_id);
                        }
                    }
                    std::shuffle(rack.begin(), rack.end(), rnd_engine);
                }
                size_t remaining = dc_rf - dr_load.node_count;
                if (cur_tablets) {
                    tablet_logger.debug("Allocating additional tablet replicas in dc={} allocated={} rf={}", dc, dr_load.node_count, dc_rf);
                }
                while (remaining) {
                    co_await coroutine::maybe_yield();
                    if (dr_load.racks.empty()) {
                        on_internal_error(tablet_logger, format("Cannot allocate table replica in dc={}: no available racks", dc));
                    }
                    std::pop_heap(dr_load.racks.begin(), dr_load.racks.end(), cmp);
                    auto& rl = dr_load.racks.back();
                    auto& rack = candidate_nodes_per_rack[rl.rack_name];
                    if (rack.empty()) {
                        dr_load.racks.pop_back();
                        continue;
                    }
                    const auto& host_id = rack.back();
                    replicas.emplace_back(tablet_replica{host_id, load.next_shard(host_id)});
                    ++rl.load;
                    ++dr_load.node_count;
                    --remaining;
                    rack.pop_back();
                    if (rack.empty()) {
                        // This rack is now exhausted
                        dr_load.racks.pop_back();
                    } else {
                        std::push_heap(dr_load.racks.begin(), dr_load.racks.end(), cmp);
                    }
                }
            } else {
                tablet_logger.debug("Deallocating tablet replicas in dc={} allocated={} rf={}", dc, dr_load.node_count, dc_rf);

                // Leave dc_rf replicas in dc, effectively deallocating in reverse order,
                // to maintain replica pairing between the base table and its materialized views.
                // This may leave racks unbalanced, but that's ok since the tablet load balancer
                // can fix this later.
                tablet_replica_set filtered;
                filtered.reserve(replicas.size() - dr_load.node_count + dc_rf);
                size_t nodes_in_dc = 0;
                for (const auto& tr : replicas) {
                    auto keep = topo.get_node(tr.host).dc_rack().dc != dc || ++nodes_in_dc <= dc_rf;
                    tablet_logger.debug("{} tablet replica id={} {} in dc={}", keep ? "Keeping" : "Dropping", tb.id, tr.host, topo.get_node(tr.host).dc_rack().dc);
                    if (keep) {
                        filtered.emplace_back(tr);
                    }
                }
                replicas = std::move(filtered);
            }
        }
        tablets.set_tablet(tb, tablet_info{std::move(replicas)});
    }

    tablet_logger.debug("{} tablets for {}.{} ({}): {}", cur_tablets ? "Reallocated" : "Allocated", s->ks_name(), s->cf_name(), s->id(), tablets);
    co_return tablets;
}

using registry = class_registrator<abstract_replication_strategy, network_topology_strategy, replication_strategy_params>;
static registry registrator("org.apache.cassandra.locator.NetworkTopologyStrategy");
static registry registrator_short_name("NetworkTopologyStrategy");
}
