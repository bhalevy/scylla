/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation_partition.hh"
#include "mutation.hh"
#include "canonical_mutation.hh"

//
// Applies p to current object.
//
// Commutative when this_schema == p_schema. If schemas differ, data in p which
// is not representable in this_schema is dropped, thus apply() loses commutativity.
//
// Weak exception guarantees.
// Assumes target and p are not owned by a cache_tracker.
future<> apply_gently(mutation_partition& target, const schema& target_schema, const mutation_partition& p, const schema& p_schema,
        mutation_application_stats& app_stats);
future<> apply_gently(mutation_partition& target, const schema& target_schema, mutation_partition_view p, const schema& p_schema,
        mutation_application_stats& app_stats);
// Use in case this instance and p share the same schema.
// Same guarantees and constraints as for other variants of apply_gently().
future<> apply_gently(mutation_partition& target, const schema& target_schema, mutation_partition&& p, mutation_application_stats& app_stats);

// The apply_gently entry points may yield while applying
// changes to a mutation partition, therefore they should not
// be used when atomic application is requried, such as when
// applying changes to memtable, which is done synchronously.
future<> apply_gently(mutation& target, mutation&& m);
future<> apply_gently(mutation& target, const mutation& m);

future<mutation> to_mutation_gently(const canonical_mutation& cm, schema_ptr s);
