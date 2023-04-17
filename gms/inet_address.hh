/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/net/ipv4_address.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <iosfwd>
#include <optional>
#include <functional>

#include "bytes.hh"
#include "seastarx.hh"

namespace gms {

class inet_address {
private:
    net::inet_address _addr;
public:
    inet_address() = default;
    inet_address(int32_t ip) noexcept
        : inet_address(uint32_t(ip)) {
    }
    explicit inet_address(uint32_t ip) noexcept
        : _addr(net::ipv4_address(ip)) {
    }
    inet_address(const net::inet_address& addr) noexcept : _addr(addr) {}
    inet_address(const socket_address& sa) noexcept
        : inet_address(sa.addr())
    {}
    const net::inet_address& addr() const noexcept {
        return _addr;
    }

    inet_address(const inet_address&) = default;

    operator const seastar::net::inet_address&() const noexcept {
        return _addr;
    }

    // throws std::invalid_argument if sstring is invalid
    inet_address(const sstring& addr) {
        // FIXME: We need a real DNS resolver
        if (addr == "localhost") {
            _addr = net::ipv4_address("127.0.0.1");
        } else {
            _addr = net::inet_address(addr);
        }
    }
    bytes_view bytes() const noexcept {
        return bytes_view(reinterpret_cast<const int8_t*>(_addr.data()), _addr.size());
    }
    // TODO remove
    uint32_t raw_addr() const {
        return addr().as_ipv4_address().ip;
    }
    sstring to_sstring() const;
    bool operator==(const inet_address& x) const = default;
    auto operator<=>(const inet_address& x) const noexcept {
        return bytes() <=> x.bytes();
    }
    friend struct std::hash<inet_address>;

    using opt_family = std::optional<net::inet_address::family>;

    static future<inet_address> lookup(sstring, opt_family family = {}, opt_family preferred = {});
};

std::ostream& operator<<(std::ostream& os, const inet_address& x);

}

namespace std {
template<>
struct hash<gms::inet_address> {
    size_t operator()(gms::inet_address a) const noexcept { return std::hash<net::inet_address>()(a._addr); }
};
}

template <>
struct fmt::formatter<gms::inet_address> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const ::gms::inet_address& x, FormatContext& ctx) const {
        if (x.addr().is_ipv4()) {
            return fmt::format_to(ctx.out(), "{}", x.addr());
        }
        // print 2 bytes in a group, and use ':' as the delimeter
        fmt::format_to(ctx.out(), "{:2:}", fmt_hex(x.bytes()));
        if (x.addr().scope() != seastar::net::inet_address::invalid_scope) {
            return fmt::format_to(ctx.out(), "%{}", x.addr().scope());
        }
        return ctx.out();
    }
};
