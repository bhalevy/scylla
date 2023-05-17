/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/range/adaptors.hpp>

#include <seastar/core/coroutine.hh>

#include "gms/application_state.hh"
#include "gms/endpoint_state.hh"
#include "gms/versioned_value.hh"
#include "seastar/core/on_internal_error.hh"
#include <optional>
#include <ostream>

namespace gms {

extern logging::logger logger;

static_assert(std::is_default_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_copy_constructible_v<heart_beat_state>);
static_assert(std::is_nothrow_move_constructible_v<heart_beat_state>);

static_assert(std::is_nothrow_default_constructible_v<std::map<application_state, versioned_value>>);

// Note: although std::map::find is not guaranteed to be noexcept
// it depends on the comperator used and in this case comparing application_state
// is noexcept.  Therefore, we can safely mark this method noexcept.
const versioned_value* endpoint_state::get_application_state_ptr(application_state key) const noexcept {
    auto it = _application_state.find(key);
    if (it == _application_state.end()) {
        return nullptr;
    } else {
        return &it->second;
    }
}

std::ostream& operator<<(std::ostream& os, const endpoint_state& x) {
    os << "HeartBeatState = " << x._heart_beat_state << ", AppStateMap =";
    for (auto&entry : x._application_state) {
        const application_state& state = entry.first;
        const versioned_value& value = entry.second;
        os << " { " << state << " : " << value << " } ";
    }
    return os;
}

bool endpoint_state::is_cql_ready() const noexcept {
    auto* app_state = get_application_state_ptr(application_state::RPC_READY);
    if (!app_state) {
        return false;
    }
    try {
        return boost::lexical_cast<int>(app_state->value());
    } catch (...) {
        return false;
    }
}

std::vector<inet_address> endpoint_state_map::get_endpoints() const {
    return boost::copy_range<std::vector<inet_address>>(_state_by_address | boost::adaptors::map_keys);
}

const endpoint_state* endpoint_state_map::get_ptr(const endpoint_id& ep) const noexcept {
    return get_ptr(ep.addr);
}
const endpoint_state* endpoint_state_map::get_ptr(inet_address addr) const noexcept {
    auto it = _state_by_address.find(addr);
    if (it != _state_by_address.end()) {
        return it->second.get();
    }
    return nullptr;
}

endpoint_state* endpoint_state_map::get_ptr(const endpoint_id& ep) noexcept {
    return get_ptr(ep.addr);
}
endpoint_state* endpoint_state_map::get_ptr(inet_address addr) noexcept {
    auto it = _state_by_address.find(addr);
    if (it != _state_by_address.end()) {
        return it->second.get();
    }
    return nullptr;
}

const endpoint_state& endpoint_state_map::at(const endpoint_id& ep) const {
    return at(ep.addr);
}
const endpoint_state& endpoint_state_map::at(inet_address addr) const {
    auto it = _state_by_address.find(addr);
    if (it != _state_by_address.end()) {
        return *it->second;
    }
    throw std::out_of_range(format("endpoint state not found for address={}", addr));
}

endpoint_state& endpoint_state_map::at(const endpoint_id& ep) {
    return at(ep.addr);
}
endpoint_state& endpoint_state_map::at(inet_address addr) {
    auto it = _state_by_address.find(addr);
    if (it != _state_by_address.end()) {
        return *it->second;
    }
    throw std::out_of_range(format("endpoint state not found for address={}", addr));
}

endpoint_state& endpoint_state_map::get_or_create(const endpoint_id& node) {
    if (!node.host_id || node.addr == inet_address()) {
        on_internal_error(logger, format("endpoint_state_map::get_or_create: invalid endpoint {}", node));
    }
    if (node.addr == inet_address()) {
        on_internal_error(logger, format("Cannot get_or_create state for endpoint {} with null address", node.host_id));
    }
    auto it = _state_by_address.find(node.addr);
    if (it == _state_by_address.end()) {
        auto epsp = make_lw_shared<endpoint_state>();
        auto& eps = *epsp;
        eps.add_application_state(application_state::HOST_ID, versioned_value::host_id(node.host_id));
        eps.add_application_state(application_state::RPC_ADDRESS, versioned_value::rpcaddress(node.addr));
        it = _state_by_address.emplace(node.addr, std::move(epsp)).first;
    }
    return *it->second;
}

endpoint_state& endpoint_state_map::set(const endpoint_id& node, endpoint_state&& eps) {
    if (!node.host_id || node.addr == inet_address()) {
        on_internal_error(logger, format("endpoint_state_map::get_or_create: invalid endpoint {}", node));
    }
    if (auto host_id_state = eps.get_application_state_ptr(application_state::HOST_ID)) {
        auto state_host_id = locator::host_id(utils::UUID(host_id_state->value()));
        if (state_host_id != node.host_id) {
            on_internal_error(logger, format("endpoint_state_map::set: node {} has miamatching host_id={} in endpoint_state", node, state_host_id));
        }
    } else {
        eps.add_application_state(application_state::HOST_ID, versioned_value::host_id(node.host_id));
    }
    if (auto addr_state = eps.get_application_state_ptr(application_state::RPC_ADDRESS)) {
        auto state_addr = inet_address(addr_state->value());
        if (state_addr != node.addr) {
            on_internal_error(logger, format("endpoint_state_map::set: node {} has miamatching address={} in endpoint_state", node, state_addr));
        }
    } else {
        eps.add_application_state(application_state::RPC_ADDRESS, versioned_value::rpcaddress(node.addr));
    }
    auto epsp = make_lw_shared<endpoint_state>(std::move(eps));
    auto it = _state_by_address.find(node.addr);
    if (it == _state_by_address.end()) {
        it = _state_by_address.emplace(node.addr, std::move(epsp)).first;
    } else {
        it->second = std::move(epsp);
    }
    return *it->second;
}

bool endpoint_state_map::erase(const endpoint_id& ep) {
    return erase(ep.addr);
}
bool endpoint_state_map::erase(inet_address addr) {
    return _state_by_address.erase(addr);
}

future<endpoint_state_map::endpoint_permit> endpoint_state_map::lock_endpoint(endpoint_id ep) {
    endpoint_permit permit;

    auto it = _address_locks.find(ep.addr);
    if (it == _address_locks.end()) {
        it = _address_locks.emplace(ep.addr, 1).first;
    }
    permit.address_lock_holder = co_await get_units(it->second, 1);
    if (!ep.host_id) {
        if (auto eps = get_ptr(ep.addr)) {
            ep.host_id = eps->get_host_id();
        }
    }
    auto hit = _host_id_locks.find(ep.host_id);
    if (hit == _host_id_locks.end()) {
        hit = _host_id_locks.emplace(ep.host_id, 1).first;
    }
    permit.host_id_lock_holder = co_await get_units(hit->second, 1);

    co_return permit;
}

} // namespace gms
