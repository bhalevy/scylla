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
