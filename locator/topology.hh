/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <unordered_set>
#include <unordered_map>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

#include "locator/types.hh"
#include "inet_address_vectors.hh"

using namespace seastar;

namespace locator {

class node_registry;

class node : public enable_lw_shared_from_this<node> {
public:
    using local = bool_class<struct local_tag>;

    enum class state {
        none,
        joining,
        normal,
        leaving,
        left
    };

private:
    host_id _host_id;
    inet_address _endpoint;
    endpoint_dc_rack _dc_rack;

    // Private state of this instance.
    // May change across topology generations
    local _is_local;
    state _state;
public:
    node(host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, local is_local, state st);

    const host_id& host_id() const noexcept {
        return _host_id;
    }

    const inet_address& endpoint() const noexcept {
        return _endpoint;
    }

    const endpoint_dc_rack& dc_rack() const noexcept {
        return _dc_rack;
    }

    local is_local() const noexcept { return _is_local; }

    state get_state() const noexcept { return _state; }

private:
    void set_state(state st) noexcept { _state = st; }

    friend class node_registry;
};

using must_exist = bool_class<struct must_exist_tag>;

class node_registry : public peering_sharded_service<node_registry> {
    std::unordered_set<mutable_node_ptr> _all_nodes;
    node_ptr _local_node;
    std::unordered_map<host_id, node_ptr> _nodes_by_host_id;
    std::unordered_map<inet_address, node_set> _nodes_by_endpoint;
    std::unordered_map<sstring, node_set> _dc_nodes;
    std::unordered_map<sstring, std::unordered_map<sstring, node_set>> _dc_rack_nodes;
    std::unordered_set<sstring> _datacenters;

    // legacy maps
    std::unordered_map<sstring, std::unordered_set<inet_address>> _dc_endpoints;
    std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>> _dc_racks;

public:
    future<> stop() noexcept;

    future<> clear_gently() noexcept;

    node_ptr local_node() const noexcept {
        return _local_node;
    }

    // Adds a node with given host_id, endpoint, DC/rack, and state on all shards.
    future<> add_node_on_all_shards(host_id id, const inet_address& ep, const endpoint_dc_rack& dr, node::local is_local, node::state state = node::state::none);

    // Optionally updates node's current host_id, endpoint, or DC/rack.
    void update_node(lw_shared_ptr<node> node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr, std::optional<node::state> opt_state);

    // Removes a node using its host_id
    //
    // If host_id is not found and must_exist is true:
    //   the function throws internal error.
    future<> remove_node_on_all_shards(host_id id);

    // Finds a node by its host_id
    //
    // If host_id is not found:
    //   the function throws internal error if must_exist is true,
    //   or returns nullptr otherwise.
    node_ptr find_node(host_id id, must_exist must_exist = must_exist::no) const noexcept;

    // Finds a node by its endpoint
    //
    // If host_id is not found,
    //   the function throws internal error if must_exist is true,
    //   or returns nullptr otherwise.
    node_set find_nodes(const inet_address& ep, must_exist must_exist = must_exist::no) const noexcept;

private:
    void hash_node(const mutable_node_ptr& node);
    void unhash_node(const node_ptr& node);
    void remove_node(const node_ptr& node);
};

class topology {
public:
    struct config {
        host_id local_host_id;
        inet_address local_endpoint;
        endpoint_dc_rack local_dc_rack;
        bool disable_proximity_sorting = false;
    };
    topology(sharded<node_registry>& nr, config cfg);
    topology(topology&&) noexcept;

    topology& operator=(topology&&) = default;

    future<topology> clone_gently() const;
    future<> clear_gently() noexcept;

public:
    // Returns true if a node with given host_id is found
    bool has_node(host_id id) const noexcept;
    bool has_node(inet_address id) const noexcept;

    /**
     * Stores current DC/rack assignment for ep
     *
     * Adds or updates a node with given endpoint
     */
    void update_endpoint(inet_address ep, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr);

    // Legacy entry point from token_metadata::update_topology
    void update_endpoint(inet_address ep, endpoint_dc_rack dr) {
        update_endpoint(ep, std::nullopt, std::move(dr));
    }
    void update_endpoint(inet_address ep, host_id id) {
        update_endpoint(ep, id, std::nullopt);
    }

    /**
     * Removes current DC/rack assignment for ep
     */
    void remove_endpoint(inet_address ep);

    /**
     * Returns true iff contains given endpoint.
     */
    bool has_endpoint(inet_address) const;

    const std::unordered_map<sstring,
                           std::unordered_set<inet_address>>&
    get_datacenter_endpoints() const {
        return _dc_endpoints;
    }

    const std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>&
    get_datacenter_racks() const {
        return _dc_racks;
    }

    const std::unordered_set<sstring>& get_datacenters() const noexcept {
        return _datacenters;
    }

    // Get dc/rack location of the local node
    const endpoint_dc_rack& get_location() const noexcept{
        return _local_node->dc_rack();
    }
    // Get dc/rack location of a node identified by host_id
    const endpoint_dc_rack& get_location(host_id id) const {
        return find_node(id, must_exist::yes)->dc_rack();
    }
    // Get dc/rack location of a node identified by endpoint
    const endpoint_dc_rack& get_location(const inet_address& ep) const;

    // Get rack of the local node
    const sstring& get_rack() const noexcept {
        return get_location().rack;
    }
    // Get rack of a node identified by host_id
    const sstring& get_rack(host_id id) const {
        return get_location(id).rack;
    }
    // Get rack of a node identified by endpoint
    const sstring& get_rack(inet_address ep) const {
        return get_location(ep).rack;
    }

    // Get datacenter of the local node
    const sstring& get_datacenter() const noexcept {
        return get_location().dc;
    }
    // Get datacenter of a node identified by host_id
    const sstring& get_datacenter(host_id id) const {
        return get_location(id).dc;
    }
    // Get datacenter of a node identified by endpoint
    const sstring& get_datacenter(inet_address ep) const {
        return get_location(ep).dc;
    }

    auto get_local_dc_filter() const noexcept {
        return [ this, local_dc = get_datacenter() ] (inet_address ep) {
            return get_datacenter(ep) == local_dc;
        };
    };

    template <std::ranges::range Range>
    inline size_t count_local_endpoints(const Range& endpoints) const {
        return std::count_if(endpoints.begin(), endpoints.end(), get_local_dc_filter());
    }

    /**
     * This method will sort the <tt>List</tt> by proximity to the given
     * address.
     */
    void sort_by_proximity(inet_address address, inet_address_vector_replica_set& addresses) const;

private:
    // constructor for cloning purposes
    topology(node_registry& nr) noexcept;

    node& local_node() noexcept {
        return *_local_node;
    }

    void add_node(mutable_node_ptr node);
    void remove_node(node_ptr node);
    void do_remove_node(node_ptr node);

    /**
     * compares two endpoints in relation to the target endpoint, returning as
     * Comparator.compare would
     */
    int compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const;

    void calculate_datacenters();

    node_registry& _node_registry;
    bool _sort_by_proximity = true;
};

} // namespace locator

sstring to_sstring(locator::node::state);
locator::node::state from_sstring(sstring);

namespace std {
std::ostream& operator<<(std::ostream& os, const locator::node::state& state);
}
