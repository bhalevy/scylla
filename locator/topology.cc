/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/on_internal_error.hh>
#include <stdexcept>

#include "log.hh"
#include "locator/topology.hh"
#include "locator/production_snitch_base.hh"
#include "seastar/core/smp.hh"
#include "utils/stall_free.hh"
#include "utils/fb_utilities.hh"

sstring to_sstring(locator::node::state state) {
    switch (state) {
    case locator::node::state::none:    return "none";
    case locator::node::state::joining: return "joining";
    case locator::node::state::normal:  return "normal";
    case locator::node::state::leaving: return "leaving";
    case locator::node::state::left:    return "left";
    }
    __builtin_unreachable();
}

locator::node::state from_sstring(sstring s) {
    if (s == "none") {
        return locator::node::state::none;
    } else if (s == "joining") {
        return locator::node::state::joining;
    } else if (s == "normal") {
        return locator::node::state::normal;
    } else if (s == "leaving") {
        return locator::node::state::leaving;
    } else if (s == "left") {
        return locator::node::state::left;
    }
    throw std::invalid_argument(format("Invalid node::state \"{}\"", s));
}


namespace std {
std::ostream& operator<<(std::ostream& os, const locator::node::state& state) {
    return os << to_sstring(state);
}
}

namespace locator {

static logging::logger tlogger("topology");

thread_local const endpoint_dc_rack endpoint_dc_rack::default_location = {
    .dc = locator::production_snitch_base::default_dc,
    .rack = locator::production_snitch_base::default_rack,
};

node::node(::locator::host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, local is_local, state st)
    : _host_id(id)
    , _endpoint(endpoint)
    , _dc_rack(std::move(dc_rack))
    , _is_local(is_local)
    , _state(st)
{}

future<> node_registry::stop() noexcept {
    return clear_gently();
}

future<> node_registry::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    _datacenters.clear();
    _nodes_by_endpoint.clear();
    _nodes_by_host_id.clear();
    co_await utils::clear_gently(_all_nodes);
}

future<> node_registry::add_node_on_all_shards(host_id id, const inet_address& ep, const endpoint_dc_rack& dr, node::local is_local, node::state state) {
    // FIXME: for now we allow adding nodes with null host_id
    if (ep == inet_address{}) {
        on_internal_error(tlogger, "Node must have a valid endpoint");
    }
    if (dr.dc.empty() || dr.rack.empty()) {
        on_internal_error(tlogger, "Node must have valid dc and rack");
    }
    if (is_local && _local_node) {
        on_internal_error(tlogger, format("Local node already set: host_id={} endpoint={} dc={} rack={}: currently mapped to {}",
                id, ep, dr.dc, dr.rack, _local_node));
    }
    auto f = co_await coroutine::as_future(container().invoke_on_all([&] (node_registry& nr) {
        nr.do_add_node(make_lw_shared<locator::node>(id, ep, dr, is_local, state));
    }));
    if (f.failed()) {
        co_await container().invoke_on_all([id] (node_registry& nr) {
            nr.remove_node(id);
        });
        co_await coroutine::return_exception_ptr(f.get_exception());
    }
}

future<> node_registry::add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr, node::local is_local, node::state state) {
    // FIXME: for now we allow adding nodes with null host_id
    if (ep == inet_address{}) {
        on_internal_error(tlogger, "Node must have a valid endpoint");
    }
    if (dr.dc.empty() || dr.rack.empty()) {
        on_internal_error(tlogger, "Node must have valid dc and rack");
    }
    if (is_local && _local_node) {
        on_internal_error(tlogger, format("Local node already set: host_id={} endpoint={} dc={} rack={}: currently mapped to {}",
                id, ep, dr.dc, dr.rack, _local_node));
    }
    auto f = co_await coroutine::as_future(container().invoke_on_all([&] (node_registry& nr) {
        nr.do_add_node(make_lw_shared<locator::node>(id, ep, dr, is_local, state));
    }));
    if (f.failed()) {
        co_await container().invoke_on_all([id] (node_registry& nr) {
            nr.remove_node(id);
        });
        co_await coroutine::return_exception_ptr(f.get_exception());
    }
}

void node_registry::hash_node(const mutable_node_ptr& node) {
    tlogger.debug("add_node: node={} host_id={} endpoint={} dc={} rack={} local={} state={}", fmt::ptr(node.get()),
            node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack, node->is_local(), node->get_state());
    try {
        auto [nit, inserted_host_id] = _nodes_by_host_id.emplace(node->host_id(), node.get());
        if (!inserted_host_id) {
            on_internal_error(tlogger, format("Node already exists: host_id={} endpoint={} dc={} rack={}",
                    node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack));
        }
        _nodes_by_endpoint[node->endpoint()].insert(node);

        const auto& dc = node->dc_rack().dc;
        const auto& rack = node->dc_rack().rack;
        const auto& endpoint = node->endpoint();
        _dc_nodes[dc].insert(node);
        _dc_rack_nodes[dc][rack].insert(node);
        _dc_endpoints[dc].insert(endpoint);
        _dc_racks[dc][rack].insert(endpoint);
        _datacenters.insert(dc);

        if (node->is_local()) {
            _local_node = node;
        }
        _all_nodes.emplace(std::move(node));
    } catch (...) {
        remove_node(std::move(node));
        throw;
    }
}

void node_registry::update_node(mutable_node_ptr node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_state) {
    tlogger.debug("topology[{}]: update_node: node={} host_id={} endpoint={} dc={} rack={} state={}, at {}", fmt::ptr(this), fmt::ptr(node.get()),
            opt_id.value_or(host_id::create_null_id()), opt_ep.value_or(inet_address{}), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack,
            opt_state.value_or(node::state::none),
            current_backtrace());
    bool changed = false;
    if (opt_id) {
        if (*opt_id != node->host_id()) {
            if (node->host_id()) {
                on_internal_error(tlogger, format("Updating non-null node host_id is disallowed: host_id={} endpoint={}: new host_id={}",
                        node->host_id(), node->endpoint(), *opt_id));
            }
            if (!*opt_id) {
                on_internal_error(tlogger, format("Updating node host_id to null is disallowed: host_id={} endpoint={}: new host_id={}",
                        node->host_id(), node->endpoint(), *opt_id));
            }
            if (_nodes_by_host_id.contains(*opt_id)) {
                on_internal_error(tlogger, format("Cannot update node host_id: new host_id={} already exists: endpoint={}: ",
                        *opt_id, node->endpoint()));
            }
            changed = true;
        } else {
            opt_id.reset();
        }
    }
    if (opt_ep) {
        if (*opt_ep != node->endpoint()) {
            if (*opt_ep == inet_address{}) {
                on_internal_error(tlogger, format("Updating node endpoint to null is disallowed: host_id={} endpoint={}: new endpoint={}",
                        node->host_id(), node->endpoint(), *opt_ep));
            }
            changed = true;
        } else {
            opt_ep.reset();
        }
    }
    if (opt_dr) {
        if (opt_dr->dc.empty() || opt_dr->dc == production_snitch_base::default_dc) {
            opt_dr->dc = node->dc_rack().dc;
        }
        if (opt_dr->rack.empty() || opt_dr->rack == production_snitch_base::default_rack) {
            opt_dr->rack = node->dc_rack().rack;
        }
        if (*opt_dr != node->dc_rack()) {
            changed = true;
        } else {
            opt_dr.reset();
        }
    }

    if (!changed) {
        return;
    }

    unhash_node(node);
    if (opt_id) {
        node->_host_id = *opt_id;
    }
    if (opt_ep) {
        node->_endpoint = *opt_ep;
    }
    if (opt_dr) {
        node->_dc_rack = std::move(*opt_dr);
    }
    do_add_node(node);
}

future<> node_registry::remove_node_on_all_shards(host_id id) {
    tlogger.trace("remove_node: host_id={}", id);
    co_await container().invoke_on_all([id] (node_registry& nr) {
        auto it = nr._nodes_by_host_id.find(id);
        if (it != nr._nodes_by_host_id.end()) {
            nr.remove_node(it->second);
        }
    });
}

void node_registry::unhash_node(const node_ptr& node) {
    tlogger.debug("remove_node: node={} host_id={} endpoint={} state={}, at {}", fmt::ptr(node.get()), node->host_id(), node->endpoint(), node->get_state(), current_backtrace());
 
    const auto& dc = node->dc_rack().dc;
    const auto& rack = node->dc_rack().rack;
    if (auto dit = _dc_endpoints.find(dc); dit != _dc_endpoints.end()) {
        const auto& ep = node->endpoint();
        auto& eps = dit->second;
        eps.erase(ep);
        if (eps.empty()) {
            _dc_endpoints.erase(dit);
            _datacenters.erase(dc);
            _dc_racks.erase(dc);
        } else {
            auto& racks = _dc_racks[dc];
            if (auto rit = racks.find(rack); rit != racks.end()) {
                eps = rit->second;
                eps.erase(ep);
                if (eps.empty()) {
                    racks.erase(rit);
                }
            }
        }
    }

    _nodes_by_host_id.erase(node->host_id());
    _nodes_by_endpoint.erase(node->endpoint());
}

void node_registry::remove_node(const node_ptr& node) {
    unhash_node(node);
    if (node->is_local()) {
        _local_node = {};
    }
    _all_nodes.erase(node);
}

// Finds a node by its host_id
// Returns nullptr if not found
node_ptr node_registry::find_node(host_id id, must_exist must_exist) const noexcept {
    auto it = _nodes_by_host_id.find(id);
    if (it != _nodes_by_host_id.end()) {
        return it->second->shared_from_this();
    }
    if (must_exist) {
        on_internal_error(tlogger, format("Could not find node: host_id={}", id));
    }
    return nullptr;
}

// Finds a node by its endpoint
// Returns nullptr if not found
node_set node_registry::find_nodes(const inet_address& ep, must_exist must_exist) const noexcept {
    auto it = _nodes_by_endpoint.find(ep);
    if (it != _nodes_by_endpoint.end()) {
        return it->second;
    }
    if (must_exist) {
        on_internal_error(tlogger, format("Could not find node: endpoint={}", ep));
    }
    return {};
}

future<> topology::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    _datacenters.clear();
    _nodes_by_endpoint.clear();
    _nodes_by_host_id.clear();
    _local_node = {};
    co_await utils::clear_gently(_all_nodes);
    co_return;
}

topology::topology(node_registry& nr) noexcept
    : _node_registry(nr)
{
    tlogger.trace("topology[{}]: default-constructed", fmt::ptr(this));
}

topology::topology(node_registry& nr, config cfg)
        : _node_registry(nr)
        , _sort_by_proximity(!cfg.disable_proximity_sorting)
{
    tlogger.trace("topology[{}]: constructing using config: host_id={} endpoint={} dc={} rack={}", fmt::ptr(this),
            cfg.local_host_id, cfg.local_endpoint, cfg.local_dc_rack.dc, cfg.local_dc_rack.rack);
    if (cfg.local_host_id || cfg.local_endpoint != inet_address{}) {
        _node_registry.add_node(make_lw_shared<node>(cfg.local_host_id, cfg.local_endpoint, cfg.local_dc_rack, node::local::yes));
    }
}

topology::topology(topology&& o) noexcept
    : _dc_endpoints(std::move(o._dc_endpoints))
    , _dc_racks(std::move(o._dc_racks))
    , _sort_by_proximity(o._sort_by_proximity)
    , _datacenters(std::move(o._datacenters))
{
    tlogger.trace("topology[{}]: move from [{}]", fmt::ptr(this), fmt::ptr(&o));
}

future<topology> topology::clone_gently() const {
    topology ret;
    ret._dc_endpoints.reserve(_dc_endpoints.size());
    for (const auto& p : _dc_endpoints) {
        ret._dc_endpoints.emplace(p);
    }
    co_await coroutine::maybe_yield();
    ret._dc_racks.reserve(_dc_racks.size());
    for (const auto& [dc, rack_endpoints] : _dc_racks) {
        ret._dc_racks[dc].reserve(rack_endpoints.size());
        for (const auto& p : rack_endpoints) {
            ret._dc_racks[dc].emplace(p);
        }
    }
    co_await coroutine::maybe_yield();
    ret._datacenters = _datacenters;
    ret._sort_by_proximity = _sort_by_proximity;
    co_return ret;
}

void topology::update_endpoint(inet_address ep, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr)
{
    tlogger.trace("topology[{}]: update_endpoint: ep={} host_id={} dc={} rack={}, at {}", fmt::ptr(this),
            ep, opt_id.value_or(host_id::create_null_id()), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack,
            current_backtrace());
    node_ptr n = find_node(ep);
    if (n) {
        update_node(const_cast<node*>(n.get())->shared_from_this(), opt_id, std::nullopt, std::move(opt_dr));
    } else if (opt_id && (n = find_node(*opt_id))) {
        update_node(const_cast<node*>(n.get())->shared_from_this(), std::nullopt, ep, std::move(opt_dr));
    } else {
        add_node(opt_id.value_or(host_id::create_null_id()), ep, opt_dr.value_or(endpoint_dc_rack::default_location));
    }
}

void topology::remove_endpoint(inet_address ep)
{
    tlogger.trace("topology[{}]: remove_endpoint: endpoint={}", fmt::ptr(this), ep);
    remove_node(find_node(ep));
}

bool topology::has_node(host_id id) const noexcept {
    auto node = find_node(id);
    tlogger.trace("topology[{}]: has_node: host_id={}: node={}", fmt::ptr(this), id, fmt::ptr(node.get()));
    return bool(node);
}

bool topology::has_node(inet_address ep) const noexcept {
    auto node = find_node(ep);
    tlogger.trace("topology[{}]: has_node: endpoint={}: node={}", fmt::ptr(this), ep, fmt::ptr(node.get()));
    return bool(node);
}

bool topology::has_endpoint(inet_address ep) const
{
    return has_node(ep);
}

const endpoint_dc_rack& topology::get_location(const inet_address& ep) const {
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        return get_location();
    }
    if (auto node = find_node(ep, must_exist::no)) {
        return node->dc_rack();
    }
    // FIXME -- this shouldn't happen. After topology is stable and is
    // correctly populated with endpoints, this should be replaced with
    // on_internal_error()
    tlogger.warn("Requested location for node {} not in topology. backtrace {}", ep, current_backtrace());
    return endpoint_dc_rack::default_location;
}

void topology::sort_by_proximity(inet_address address, inet_address_vector_replica_set& addresses) const {
    if (_sort_by_proximity) {
        std::sort(addresses.begin(), addresses.end(), [this, &address](inet_address& a1, inet_address& a2) {
            return compare_endpoints(address, a1, a2) < 0;
        });
    }
}

int topology::compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const {
    //
    // if one of the Nodes IS the Node we are comparing to and the other one
    // IS NOT - then return the appropriate result.
    //
    if (address == a1 && address != a2) {
        return -1;
    }

    if (address == a2 && address != a1) {
        return 1;
    }

    // ...otherwise perform the similar check in regard to Data Center
    sstring address_datacenter = get_datacenter(address);
    sstring a1_datacenter = get_datacenter(a1);
    sstring a2_datacenter = get_datacenter(a2);

    if (address_datacenter == a1_datacenter &&
        address_datacenter != a2_datacenter) {
        return -1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter != a1_datacenter) {
        return 1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter == a1_datacenter) {
        //
        // ...otherwise (in case Nodes belong to the same Data Center) check
        // the racks they belong to.
        //
        sstring address_rack = get_rack(address);
        sstring a1_rack = get_rack(a1);
        sstring a2_rack = get_rack(a2);

        if (address_rack == a1_rack && address_rack != a2_rack) {
            return -1;
        }

        if (address_rack == a2_rack && address_rack != a1_rack) {
            return 1;
        }
    }
    //
    // We don't differentiate between Nodes if all Nodes belong to different
    // Data Centers, thus make them equal.
    //
    return 0;
}

} // namespace locator
