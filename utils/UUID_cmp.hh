/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

namespace utils {

inline std::strong_ordering uint64_t_tri_compare(uint64_t a, uint64_t b) noexcept {
    return a <=> b;
}

// Read 8 most significant bytes of timeuuid from serialized bytes
inline uint64_t timeuuid_read_msb(const int8_t *b) noexcept {
    // cast to unsigned to avoid sign-compliment during shift.
    auto u64 = [](uint8_t i) -> uint64_t { return i; };
    // Scylla and Cassandra use a standard UUID memory layout for MSB:
    // 4 bytes    2 bytes    2 bytes
    // time_low - time_mid - time_hi_and_version
    //
    // The storage format uses network byte order.
    // Reorder bytes to allow for an integer compare.
    return u64(b[6] & 0xf) << 56 | u64(b[7]) << 48 |
           u64(b[4]) << 40 | u64(b[5]) << 32 |
           u64(b[0]) << 24 | u64(b[1]) << 16 |
           u64(b[2]) << 8  | u64(b[3]);
}

inline uint64_t uuid_read_lsb(const int8_t *b) noexcept {
    auto u64 = [](uint8_t i) -> uint64_t { return i; };
    return u64(b[8]) << 56 | u64(b[9]) << 48 |
           u64(b[10]) << 40 | u64(b[11]) << 32 |
           u64(b[12]) << 24 | u64(b[13]) << 16 |
           u64(b[14]) << 8  | u64(b[15]);
}

// Compare two values of timeuuid type.
// Cassandra legacy requires:
// - using signed compare for least significant bits.
// - masking off UUID version during compare, to
// treat possible non-version-1 UUID the same way as UUID.
//
// To avoid breaking ordering in existing sstables, Scylla preserves
// Cassandra compare order.
//
inline std::strong_ordering timeuuid_tri_compare(bytes_view o1, bytes_view o2) noexcept {
    auto timeuuid_read_lsb = [](bytes_view o) -> uint64_t {
        return uuid_read_lsb(o.begin()) ^ 0x8080808080808080;
    };
    auto res = uint64_t_tri_compare(timeuuid_read_msb(o1.begin()), timeuuid_read_msb(o2.begin()));
    if (res == 0) {
        res = uint64_t_tri_compare(timeuuid_read_lsb(o1), timeuuid_read_lsb(o2));
    }
    return res;
}

// Compare two values of UUID type, if they happen to be
// both of Version 1 (timeuuids).
//
// This function uses memory order for least significant bits,
// which is both faster and monotonic, so should be preferred
// to @timeuuid_tri_compare() used for all new features.
//
inline std::strong_ordering uuid_tri_compare_timeuuid(bytes_view o1, bytes_view o2) noexcept {
    auto res = uint64_t_tri_compare(timeuuid_read_msb(o1.begin()), timeuuid_read_msb(o2.begin()));
    if (res == 0) {
        res = uint64_t_tri_compare(uuid_read_lsb(o1.begin()), uuid_read_lsb(o2.begin()));
    }
    return res;
}

} // namespace utils
