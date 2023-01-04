/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/on_internal_error.hh>

#include "log.hh"
#include "locator/topology.hh"
#include "locator/production_snitch_base.hh"
#include "utils/stall_free.hh"
#include "utils/fb_utilities.hh"

namespace locator {

static logging::logger tlogger("topology");

thread_local const endpoint_dc_rack endpoint_dc_rack::default_location = {
    .dc = locator::production_snitch_base::default_dc,
    .rack = locator::production_snitch_base::default_rack,
};

node::node(::locator::host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, local is_local)
    : _host_id(id)
    , _endpoint(endpoint)
    , _dc_rack(std::move(dc_rack))
    , _is_local(is_local)
{}

void node::unlink() noexcept {
    _dc_nodes_link.unlink();
    _rack_nodes_link.unlink();
    _nodes_link.unlink();
}

future<> topology::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    _datacenters.clear();
    _nodes_by_endpoint.clear();
    _nodes_by_host_id.clear();
    co_await utils::clear_gently(_all_nodes);
    co_return;
}

topology::topology() noexcept
{
    tlogger.trace("topology[{}]: default-constructed", fmt::ptr(this));
}

topology::topology(config cfg)
        : _sort_by_proximity(!cfg.disable_proximity_sorting)
{
    tlogger.trace("topology[{}]: constructing using config: host_id={} endpoint={} dc={} rack={}", fmt::ptr(this),
            cfg.local_host_id, cfg.local_endpoint, cfg.local_dc_rack.dc, cfg.local_dc_rack.rack);
    if (cfg.local_host_id || cfg.local_endpoint != inet_address{}) {
        add_node(make_lw_shared<node>(cfg.local_host_id, cfg.local_endpoint, cfg.local_dc_rack, node::local::yes));
    }
}

topology::topology(topology&& o) noexcept
    : _all_nodes(std::move(o._all_nodes))
    , _local_node(std::exchange(o._local_node, nullptr))
    , _nodes_by_host_id(std::move(o._nodes_by_host_id))
    , _nodes_by_endpoint(std::move(o._nodes_by_endpoint))
    , _dc_nodes(std::move(o._dc_nodes))
    , _dc_rack_nodes(std::move(o._dc_rack_nodes))
    , _current_nodes(std::move(o._current_nodes))
    , _dc_endpoints(std::move(o._dc_endpoints))
    , _dc_racks(std::move(o._dc_racks))
    , _sort_by_proximity(o._sort_by_proximity)
    , _datacenters(std::move(o._datacenters))
{
    tlogger.trace("topology[{}]: move from [{}]", fmt::ptr(this), fmt::ptr(&o));
}

future<topology> topology::clone_gently() const {
    topology ret;
    for (const auto& n : _all_nodes) {
        ret.add_node(make_lw_shared<node>(*n));
        co_await coroutine::maybe_yield();
    }
    // local node may be detached for _all_nodes
    // if it was decommissioned.
    if (!ret._local_node && _local_node) {
        ret._local_node = make_lw_shared<node>(*_local_node);
    }
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

node_ptr topology::add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr) {
    if (dr.dc.empty()) {
        on_internal_error(tlogger, "Node must have a valid dc");
    }
    return add_node(make_lw_shared<node>(id, ep, dr, node::local(ep == utils::fb_utilities::get_broadcast_address())));
}

node_ptr topology::add_node(lw_shared_ptr<node> node) {
    tlogger.debug("topology[{}]: add_node: node={} host_id={} endpoint={} dc={} rack={} local={}, at {}", fmt::ptr(this), fmt::ptr(node.get()),
            node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack, node->is_local(), current_backtrace());
    if (node->endpoint() == inet_address{}) {
        on_internal_error(tlogger, "Node must have a valid endpoint");
    }
    if (node->is_local() && _local_node && _local_node != node) {
        on_internal_error(tlogger, format("Local node already set: host_id={} endpoint={} dc={} rack={}: currently mapped to host_id={} endpoint={}",
                node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack,
                _local_node->host_id(), _local_node->endpoint()));
    }
    if (auto it = _all_nodes.find(node); it != _all_nodes.end()) {
        return *it;
    }
    try {
        // FIXME: for now we allow adding nodes with null host_id
        if (node->host_id()) {
            auto [nit, inserted_host_id] = _nodes_by_host_id.emplace(node->host_id(), node.get());
            if (!inserted_host_id) {
                on_internal_error(tlogger, format("Node already exists: host_id={} endpoint={} dc={} rack={}",
                        node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack));
            }
        }
        if (node->endpoint() != inet_address{}) {
            auto [eit, inserted_endpoint] = _nodes_by_endpoint.emplace(node->endpoint(), node.get());
            if (!inserted_endpoint) {
                if (node->host_id()) {
                    _nodes_by_host_id.erase(node->host_id());
                }
                on_internal_error(tlogger, format("Node endpoint already mapped: host_id={} endpoint={} dc={} rack={}: currently mapped to host_id={}",
                        node->host_id(), node->endpoint(), node->dc_rack().dc, node->dc_rack().rack,
                        eit->second->host_id()));
            }
        }

        _current_nodes.push_back(*node);

        const auto& dc = node->dc_rack().dc;
        const auto& rack = node->dc_rack().rack;
        const auto& endpoint = node->endpoint();
        _dc_nodes[dc].push_back(*node);
        _dc_rack_nodes[dc][rack].push_back(*node);
        _dc_endpoints[dc].insert(endpoint);
        _dc_racks[dc][rack].insert(endpoint);
        _datacenters.insert(dc);

        if (node->is_local()) {
            _local_node = node;
        }
        auto [it, inserted] = _all_nodes.emplace(std::move(node));
        return *it;
    } catch (...) {
        remove_node(node);
        throw;
    }
}

node_ptr topology::update_node(lw_shared_ptr<node> node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr) {
    tlogger.debug("topology[{}]: update_node: node={} host_id={} endpoint={} dc={} rack={}, at {}", fmt::ptr(this), fmt::ptr(node.get()),
            opt_id.value_or(host_id::create_null_id()), opt_ep.value_or(inet_address{}), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack,
            current_backtrace());
    bool changed = false;
    if (opt_id) {
        if (*opt_id != node->host_id()) {
            if (!*opt_id) {
                on_internal_error(tlogger, format("Updating node host_id to null is disallowed: host_id={} endpoint={}: new host_id={}",
                        node->host_id(), node->endpoint(), *opt_id));
            }
            if (_nodes_by_host_id.contains(*opt_id)) {
                on_internal_error(tlogger, format("Cannot update node host_id: new host_id={} already exists: endpoint={}: ",
                        *opt_id, node->endpoint()));
            }
            // FIXME: for now, allow updating existing host_id since
            // this is still requried by replace_node using the same ip address
            //if (node->host_id()) {
            //    on_internal_error(tlogger, format("Updating non-null node host_id is disallowed: host_id={} endpoint={}: new host_id={}",
            //            node->host_id(), node->endpoint(), *opt_id));
            //}
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
        return node;
    }

    remove_node(node);
    if (opt_id) {
        node->_host_id = *opt_id;
    }
    if (opt_ep) {
        node->_endpoint = *opt_ep;
    }
    if (opt_dr) {
        node->_dc_rack = std::move(*opt_dr);
    }
    return add_node(node);
}

void topology::remove_node(host_id id, must_exist require_exist) {
    if (id == _local_node->host_id()) {
        on_internal_error(tlogger, format("Cannot remove the local node: host_id={} endpoint={}",
                _local_node->host_id(), _local_node->endpoint()));
    }
    tlogger.trace("topology[{}]: remove_node: host_id={}", fmt::ptr(this), id);
    auto it = _nodes_by_host_id.find(id);
    if (it != _nodes_by_host_id.end()) {
        remove_node(it->second);
    } else if (require_exist) {
        on_internal_error(tlogger, format("Node not found: host_id={}", id));
    }
}

void topology::remove_node(node* np) {
    if (np) {
        do_remove_node(np->shared_from_this());
    }
}

void topology::remove_node(node_ptr np) {
    if (np) {
        do_remove_node(const_cast<node*>(np.get())->shared_from_this());
    }
}

void topology::do_remove_node(lw_shared_ptr<node> node) {
    tlogger.debug("remove_node: node={} host_id={} endpoint={}, at {}", fmt::ptr(node.get()), node->host_id(), node->endpoint(), current_backtrace());
 
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
    node->unlink();
    _all_nodes.erase(node);
}

// Finds a node by its host_id
// Returns nullptr if not found
node_ptr topology::find_node(host_id id, must_exist must_exist) const noexcept {
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
node_ptr topology::find_node(const inet_address& ep, must_exist must_exist) const noexcept {
    auto it = _nodes_by_endpoint.find(ep);
    if (it != _nodes_by_endpoint.end()) {
        return it->second->shared_from_this();
    }
    if (must_exist) {
        on_internal_error(tlogger, format("Could not find node: endpoint={}", ep));
    }
    return nullptr;
}

node_ptr topology::update_endpoint(inet_address ep, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr)
{
    tlogger.trace("topology[{}]: update_endpoint: ep={} host_id={} dc={} rack={}, at {}", fmt::ptr(this),
            ep, opt_id.value_or(host_id::create_null_id()), opt_dr.value_or(endpoint_dc_rack{}).dc, opt_dr.value_or(endpoint_dc_rack{}).rack,
            current_backtrace());
    node_ptr n = find_node(ep);
    if (n) {
        return update_node(const_cast<node*>(n.get())->shared_from_this(), opt_id, std::nullopt, std::move(opt_dr));
    } else if (opt_id && (n = find_node(*opt_id))) {
        return update_node(const_cast<node*>(n.get())->shared_from_this(), std::nullopt, ep, std::move(opt_dr));
    } else {
        return add_node(opt_id.value_or(host_id::create_null_id()), ep, opt_dr.value_or(endpoint_dc_rack::default_location));
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
