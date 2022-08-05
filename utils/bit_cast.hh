/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <bit>
#include <cstdint>
#include <cstring>
#include <cstddef>
#include <type_traits>

template <class T> concept Trivial = std::is_trivial_v<T>;
template <class T> concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

template <TriviallyCopyable To>
To read_unaligned(const void* src) {
    uint8_t dst[sizeof(To)];
    std::memcpy(&dst, src, sizeof(To));
    return *reinterpret_cast<To*>(dst);
}

template <TriviallyCopyable From>
void write_unaligned(void* dst, const From& src) {
    std::memcpy(dst, &src, sizeof(From));
}
