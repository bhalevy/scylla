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
    gms::inet_address addr;
    uint32_t cpu_id;
    locator::host_id host_id;
    msg_addr(gms::inet_address ip, uint32_t cpu = 0, std::optional<locator::host_id> opt_id = std::nullopt) noexcept
        : addr(ip)
        , cpu_id(cpu)
        , host_id(opt_id.value_or(locator::host_id::create_null_id()))
    {}

    friend bool operator==(const msg_addr& x, const msg_addr& y) noexcept;
    std::strong_ordering operator<=>(const msg_addr& o) const noexcept;
};

} // namespace netw

namespace fmt {

template <>
struct formatter<netw::msg_addr> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const netw::msg_addr& id, FormatContext& ctx) const {
        return format_to(ctx.out(), "{}/{}.{}", id.host_id, id.addr, id.cpu_id);
    }
};

} // namespace fmt

namespace std {

template <>
struct hash<netw::msg_addr> {
    size_t operator()(const netw::msg_addr& id) const noexcept {
        // Ignore cpu id for now since we do not really support // shard to shard connections
        // Ignore host_id for now since msg_addr with null host_id is equivalent to msg_addr with engaged host_id
        // and they must be hashed to the same bucket.
        return std::hash<bytes_view>()(id.addr.bytes());
    }
};

} // namespace std
