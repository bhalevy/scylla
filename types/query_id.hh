/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"
#include "utils/hash.hh"

class query_id : public utils::UUID_class<query_id> {
public:
    query_id() = default;
    explicit query_id(utils::UUID uuid) noexcept : utils::UUID_class<query_id>(uuid) {}

    explicit operator bool() const noexcept {
        return !to_uuid().is_null();
    }

    static query_id make_null() noexcept {
        return query_id(utils::null_uuid());
    }

    static query_id make_random() noexcept {
        return query_id(utils::make_random_uuid());
    }
};

namespace std {
template<> struct hash<query_id> : public hash<utils::UUID_class<query_id>> {};
}

