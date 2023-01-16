/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "gms/inet_address.hh"
#include <cstdint>
#include "locator/host_id.hh"

namespace netw {

struct msg_addr {
    locator::host_id host_id;
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const msg_addr& x, const msg_addr& y) noexcept;
    friend bool operator<(const msg_addr& x, const msg_addr& y) noexcept;
    explicit msg_addr(gms::inet_address ip) noexcept : host_id(locator::host_id::create_null_id()), addr(ip), cpu_id(0) { }
    msg_addr(gms::inet_address ip, uint32_t cpu) noexcept : host_id(locator::host_id::create_null_id()), addr(ip), cpu_id(cpu) { }
    msg_addr(const locator::host_id& id, gms::inet_address ip, uint32_t cpu = 0) noexcept : host_id(id), addr(ip), cpu_id(cpu) { }
};

} // namespace netw

namespace std {

template<>
struct hash<netw::msg_addr> {
    size_t operator()(const netw::msg_addr& id) const noexcept {
        return std::hash<bytes_view>()(id.addr.bytes());
    }
};

inline std::ostream& operator<<(std::ostream& os, const netw::msg_addr& id) {
    return os << id.host_id << '/' << id.addr << ':' << id.cpu_id;
}

} // namespace std
