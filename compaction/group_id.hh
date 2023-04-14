/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/format.h>

namespace compaction {

struct group_id {
    unsigned idx;
    unsigned total;

    group_id() noexcept : idx(0), total(0) {}
    group_id(unsigned idx, unsigned total) noexcept : idx(idx), total(total) {}
    explicit group_id(unsigned total) noexcept : idx(0), total(total) {}

    group_id& operator++() noexcept {
        ++idx;
        return *this;
    }

    group_id operator++(int) noexcept {
        auto ret = *this;
        ++idx;
        return ret;
    }

    group_id& operator--() noexcept {
        --idx;
        return *this;
    }

    group_id operator--(int) noexcept {
        auto ret = *this;
        --idx;
        return ret;
    }
};

} // namespace compaction

namespace fmt {

template <>
struct formatter<compaction::group_id> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const compaction::group_id& gid, FormatContext& ctx) const {
        return format_to(ctx.out(), "{}/{}", gid.idx, gid.total);
    }
};

} // namespace fmt
