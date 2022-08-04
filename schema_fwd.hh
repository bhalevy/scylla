/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "utils/UUID.hh"

using column_count_type = uint32_t;

// Column ID, unique within column_kind
using column_id = column_count_type;

class schema;
class schema_extension;

using schema_ptr = seastar::lw_shared_ptr<const schema>;

class table_id : public utils::UUID_class<table_id> {
public:
    table_id() = default;
    explicit table_id(utils::UUID uuid) noexcept : utils::UUID_class<table_id>(uuid) {}
};

namespace std {
template<> struct hash<table_id> : public hash<utils::UUID_class<table_id>> {};
}

// Cluster-wide identifier of schema version of particular table.
//
// The version changes the value not only on structural changes but also
// temporal. For example, schemas with the same set of columns but created at
// different times should have different versions. This allows nodes to detect
// if the version they see was already synchronized with or not even if it has
// the same structure as the past versions.
//
// Schema changes merged in any order should result in the same final version.
//
// When table_schema_version changes, schema_tables::calculate_schema_digest() should
// also change when schema mutations are applied.
class table_schema_version : public utils::UUID_class<table_schema_version> {
public:
    table_schema_version() = default;
    explicit table_schema_version(utils::UUID uuid) noexcept : utils::UUID_class<table_schema_version>(uuid) {}

    table_schema_version reversed() const noexcept;
};

namespace std {
template<> struct hash<table_schema_version> : public hash<utils::UUID_class<table_schema_version>> {};
}
