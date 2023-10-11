/*

 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "utils/chunked_vector.hh"

namespace gms {

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
class gossip_digest_ack {
private:
    using inet_address = gms::inet_address;
    utils::chunked_vector<gossip_digest> _digests;
    std::unordered_map<inet_address, endpoint_state> _map;
    // Optional mapping of all inet_address -> host_id in _map
    std::unordered_map<inet_address, locator::host_id> _address_map;
public:
    gossip_digest_ack() {
    }

    gossip_digest_ack(utils::chunked_vector<gossip_digest> d, std::unordered_map<inet_address, endpoint_state> m, std::unordered_map<inet_address, locator::host_id> address_map)
        : _digests(std::move(d))
        , _map(std::move(m))
        , _address_map(std::move(address_map))
    {}

    const utils::chunked_vector<gossip_digest>& get_gossip_digest_list() const {
        return _digests;
    }

    std::unordered_map<inet_address, endpoint_state>& get_endpoint_state_map() {
        return _map;
    }

    const std::unordered_map<inet_address, endpoint_state>& get_endpoint_state_map() const {
        return _map;
    }

    const std::unordered_map<inet_address, locator::host_id>& get_address_map() const {
        return _address_map;
    }

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_ack& ack);
};

}
