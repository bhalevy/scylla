/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "utils/serialization.hh"
#include "gms/inet_address.hh"
#include "gms/generation-number.hh"
#include "gms/version_generator.hh"
#include "locator/host_id.hh"

namespace gms {

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
class gossip_digest { // implements Comparable<GossipDigest>
private:
    using inet_address = gms::inet_address;
    inet_address _endpoint;
    generation_type _generation;
    version_type _max_version;
    // Optional host_id of _endpoint
    locator::host_id _host_id;
public:
    gossip_digest() = default;

    explicit gossip_digest(inet_address ep, generation_type gen, version_type version, locator::host_id host_id) noexcept
        : _endpoint(ep)
        , _generation(gen)
        , _max_version(version)
        , _host_id(std::move(host_id))
    {}

    inet_address get_endpoint() const {
        return _endpoint;
    }

    void set_endpoint(inet_address ep) {
        _endpoint = ep;
    }

    generation_type get_generation() const {
        return _generation;
    }

    version_type get_max_version() const {
        return _max_version;
    }

    // note: host_id may be null when digest is sent by peers running old versions
    locator::host_id get_host_id() const {
        return _host_id;
    }

    void set_host_id(locator::host_id host_id) {
        _host_id = host_id;
    }

    friend bool operator<(const gossip_digest& x, const gossip_digest& y) {
        if (x._generation != y._generation) {
            return x._generation < y._generation;
        }
        return x._max_version <  y._max_version;
    }

    friend inline std::ostream& operator<<(std::ostream& os, const gossip_digest& d) {
        fmt::print(os, "{}/{}:{}:{}", d._host_id, d._endpoint, d._generation, d._max_version);
        return os;
    }
}; // class gossip_digest

} // namespace gms
