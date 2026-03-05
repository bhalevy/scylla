/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "readers/mutation_reader.hh"
#include "compaction/compaction_fwd.hh"

namespace mutation_writer {

using owned_ranges_ptr = compaction::owned_ranges_ptr;

// Given a producer that may contain data for all shards, consume it in a per-vnode manner. 
// Similar to segregate_by_shard, but segregates by vnode token ranges. This is used for migration of vnode-based tables to tablets.
future<> segregate_by_vnode(mutation_reader producer, mutation_reader_consumer consumer, owned_ranges_ptr owned_ranges);

} // namespace mutation_writer
