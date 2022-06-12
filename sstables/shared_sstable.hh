/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <utility>
#include <functional>
#include <unordered_set>
#include <seastar/core/shared_ptr.hh>

namespace sstables {

class sstable;

};

// Customize deleter so that lw_shared_ptr can work with an incomplete sstable class
namespace seastar {

template <>
struct lw_shared_ptr_deleter<sstables::sstable> {
    static void dispose(sstables::sstable* sst);
};

}

namespace sstables {

using shared_sstable = seastar::lw_shared_ptr<sstable>;
using sstable_list = std::unordered_set<shared_sstable>;

struct sstable_gen_hash final {
    size_t operator()(const shared_sstable &) const noexcept;
};

struct sstable_gen_equal_to final : public std::binary_function<shared_sstable, shared_sstable, bool> {
    bool operator()(const shared_sstable& lhs, const shared_sstable& rhs) const noexcept;
};

using unique_genration_sstable_set = std::unordered_set<shared_sstable, sstable_gen_hash, sstable_gen_equal_to>;

}


