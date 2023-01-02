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
#include <seastar/util/bool_class.hh>

#include "locator/types.hh"
#include "inet_address_vectors.hh"

using namespace seastar;

namespace locator {

class topology {
public:
    struct config {
        host_id local_host_id;
        inet_address local_endpoint;
        endpoint_dc_rack local_dc_rack;
        bool disable_proximity_sorting = false;
    };
    topology(config cfg);
    topology(topology&&) noexcept;

    topology& operator=(topology&&) = default;

    future<topology> clone_gently() const;
    future<> clear_gently() noexcept;

    class node : public enable_lw_shared_from_this<node> {
    public:
        using local = bool_class<struct local_tag>;

    private:
        locator::host_id _host_id;
        inet_address _endpoint;
        endpoint_dc_rack _dc_rack;

        // Private state of this instance.
        // May change across topology generations
        local _is_local;

        using node_member_hook = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

        node_member_hook _nodes_link;
        node_member_hook _dc_nodes_link;
        node_member_hook _rack_nodes_link;

        friend class topology;
    public:
        node(host_id id, inet_address endpoint, endpoint_dc_rack dc_rack, local is_local);

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

    private:
        void unlink() noexcept;

        friend class topology;
    };

public:
    using node_ptr = lw_shared_ptr<const node>;
    using node_ptr_replica_set = utils::small_vector<node_ptr, 3>;
    using node_ptr_topology_change = utils::small_vector<node_ptr, 1>;

    node_ptr local_node() const noexcept {
        return _local_node;
    }

    // Adds a node with given host_id, endpoint, and DC/rack.
    void add_node(host_id id, const inet_address& ep, const endpoint_dc_rack& dr);

    // Optionally updates node's current host_id, endpoint, or DC/rack.
    void update_node(lw_shared_ptr<node> node, std::optional<host_id> opt_id, std::optional<inet_address> opt_ep, std::optional<endpoint_dc_rack> opt_dr);

    using must_exist = bool_class<struct must_exist_tag>;

    // Removes a node using its host_id
    //
    // If host_id is not found and must_exist is true:
    //   the function throws internal error.
    void remove_node(host_id id, must_exist must_exist = must_exist::no);

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
    node_ptr find_node(const inet_address& ep, must_exist must_exist = must_exist::no) const noexcept;

    // Returns true if a node with given host_id is found
    bool has_node(host_id id) const noexcept;
    bool has_node(inet_address id) const noexcept;

    /**
     * Stores current DC/rack assignment for ep
     *
     * Adds a node with given endpoint with must_exist::no
     */
    void update_endpoint(inet_address ep, std::optional<host_id> opt_id, std::optional<endpoint_dc_rack> opt_dr, must_exist must_exist);

    // Legacy entry point from token_metadata::update_topology
    void update_endpoint(inet_address ep, endpoint_dc_rack dr) {
        update_endpoint(ep, std::nullopt, std::move(dr), must_exist::no);
    }
    void update_endpoint(inet_address ep, host_id id) {
        update_endpoint(ep, id, std::nullopt, must_exist::yes);
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
    // default constructor for cloning purposes
    topology() noexcept;

    node& local_node() noexcept {
        return *_local_node;
    }

    void add_node(lw_shared_ptr<node> node);
    void remove_node(node* np);
    void remove_node(node_ptr node);
    void do_remove_node(lw_shared_ptr<node> node);

    /**
     * compares two endpoints in relation to the target endpoint, returning as
     * Comparator.compare would
     */
    int compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const;

    std::unordered_set<lw_shared_ptr<node>> _all_nodes;
    lw_shared_ptr<node> _local_node;
    std::unordered_map<host_id, node*> _nodes_by_host_id;
    std::unordered_map<inet_address, node*> _nodes_by_endpoint;

    std::unordered_map<sstring,
        boost::intrusive::list<node
            , boost::intrusive::member_hook<node, node::node_member_hook, &node::_dc_nodes_link>
            , boost::intrusive::constant_time_size<false>
        >
    > _dc_nodes;
    std::unordered_map<sstring,
        std::unordered_map<sstring,
            boost::intrusive::list<node
                , boost::intrusive::member_hook<node, node::node_member_hook, &node::_rack_nodes_link>
                , boost::intrusive::constant_time_size<false>
            >
        >
    > _dc_rack_nodes;

    boost::intrusive::list<node
        , boost::intrusive::member_hook<node, node::node_member_hook, &node::_nodes_link>
        , boost::intrusive::constant_time_size<false>
    > _current_nodes;

    /** multi-map: DC -> endpoints in that DC */
    std::unordered_map<sstring,
                       std::unordered_set<inet_address>>
        _dc_endpoints;

    /** map: DC -> (multi-map: rack -> endpoints in that rack) */
    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>
        _dc_racks;

    bool _sort_by_proximity = true;

    // pre-calculated
    std::unordered_set<sstring> _datacenters;

    void calculate_datacenters();
};

} // namespace locator
