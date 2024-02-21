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

network_topology_strategy_traits::network_topology_strategy_traits(const replication_strategy_params& params)
    : abstract_replication_strategy_traits(params.initial_tablets.has_value() ?
        abstract_replication_strategy_traits(replication_strategy_type::network_topology,
                abstract_replication_strategy_traits::local::no,
                abstract_replication_strategy_traits::per_table::yes,
                abstract_replication_strategy_traits::tablets::yes) :
        abstract_replication_strategy_traits(replication_strategy_type::network_topology)
    )
{}

network_topology_strategy::network_topology_strategy(replication_strategy_params params) :
        abstract_replication_strategy(network_topology_strategy_traits(params), params) {
    auto opts = _config_options;
    process_tablet_options(params);

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

using rack_set = std::unordered_set<const rack*>;

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
        rack_set& _racks;

        /** Number of replicas left to fill from this DC. */
        size_t _rf_left;
        ssize_t _acceptable_rack_repeats;

        data_center_endpoints(size_t rf, size_t rack_count, size_t node_count, host_id_set& endpoints, rack_set& racks)
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
        bool add_endpoint_and_check_if_done(const host_id& ep, const location& location) {
            if (done()) {
                return false;
            }

            if (_racks.emplace(location.rack).second) {
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

    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    host_id_set _replicas;
    // tracks the racks we have already placed replicas in
    rack_set _seen_racks;

    std::unordered_map<const datacenter*, data_center_endpoints> _dcs;

    size_t _dcs_to_fill;

public:
    natural_endpoints_tracker(const token_metadata& tm, const std::unordered_map<sstring, size_t>& dc_rep_factor)
        : _tm(tm)
        , _tp(_tm.get_topology())
    {
        std::unordered_set<sstring_view> processed_dcs; // Used for tracking found datacenters in log_level::debug
        for (const auto& [dc, dc_inventory] : _tp.get_inventory()) {
            auto it = dc_rep_factor.find(dc->name);
            if (it == dc_rep_factor.end()) {
                continue;
            }
            if (rslogger.is_enabled(log_level::debug)) {
                processed_dcs.insert(it->first);
            }
            auto rf = it->second;
            if (!rf) {
                continue;
            }

            size_t node_count = dc_inventory.node_count;
            size_t rack_count = dc_inventory.racks.size();
            if (!node_count) {
                rslogger.debug("datacenter {} with rf={} is empty", dc->name, rf);
                continue;
            }

            // Create a data_center_endpoints object for each non-empty DC.
            _dcs.emplace(dc, data_center_endpoints(rf, rack_count, node_count, _replicas, _seen_racks));
            _dcs_to_fill = _dcs.size();
        }

        if (rslogger.is_enabled(log_level::debug)) {
            for (const auto& [dc, rf] : dc_rep_factor) {
                if (!processed_dcs.contains(dc)) {
                    rslogger.debug("datacenter {} with rf={} not found", dc, rf);
                }
            }
        }
    }

    bool add_endpoint_and_check_if_done(host_id ep) {
        const auto* node = _tp.find_node(ep);
        if (!node) {
            on_internal_error(rslogger, format("Node {} not found", ep));
        }
        const auto& loc = node->location();
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
        for (const auto& p : dc_rf) {
            if (p.second > endpoints_in(p.first)) {
                throw exceptions::configuration_exception(fmt::format("Datacenter {} doesn't have enough nodes for replication_factor={}", p.first, p.second));
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
    auto opts = topology.get_datacenter_names();
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
    natural_endpoints_tracker::check_enough_endpoints(*tm, _dc_rep_factor);

    auto tablet_count = get_initial_tablets();
    if (tablet_count == 0) {
        tablet_count = calculate_initial_tablets_from_topology(*s, tm->get_topology(), _dc_rep_factor) * initial_scale;
    }
    auto aligned_tablet_count = 1ul << log2ceil(tablet_count);
    if (tablet_count != aligned_tablet_count) {
        rslogger.info("Rounding up tablet count from {} to {} for table {}.{}", tablet_count, aligned_tablet_count, s->ks_name(), s->cf_name());
        tablet_count = aligned_tablet_count;
    }

    tablet_map tablets(tablet_count);
    load_sketch load(tm);
    co_await load.populate();

    // FIXME: Don't use tokens to distribute nodes.
    // The following reuses the existing token-based algorithm used by NetworkTopologyStrategy.
    assert(!tm->sorted_tokens().empty());
    auto token_range = tm->ring_range(dht::token::get_random_token());

    for (tablet_id tb : tablets.tablet_ids()) {
        natural_endpoints_tracker tracker(*tm, _dc_rep_factor);

        while (true) {
            co_await coroutine::maybe_yield();
            if (token_range.begin() == token_range.end()) {
                token_range = tm->ring_range(dht::minimum_token());
            }
            locator::host_id ep = *tm->get_endpoint(*token_range.begin());
            token_range.drop_front();
            if (tracker.add_endpoint_and_check_if_done(ep)) {
                break;
            }
        }

        tablet_replica_set replicas;
        for (auto&& ep : tracker.replicas()) {
            replicas.emplace_back(tablet_replica{ep, load.next_shard(ep)});
        }

        tablets.set_tablet(tb, tablet_info{std::move(replicas)});
    }

    tablet_logger.debug("Allocated tablets for {}.{} ({}): {}", s->ks_name(), s->cf_name(), s->id(), tablets);
    co_return tablets;
}

using registry = strategy_class_registry::registrator<network_topology_strategy>;
static registry registrator("org.apache.cassandra.locator.NetworkTopologyStrategy");
static registry registrator_short_name("NetworkTopologyStrategy");

using traits_registry = strategy_class_traits_registry::registrator<network_topology_strategy_traits>;
static traits_registry traits_registrator("org.apache.cassandra.locator.NetworkTopologyStrategy");
static traits_registry traits_registrator_short_name("NetworkTopologyStrategy");

}
