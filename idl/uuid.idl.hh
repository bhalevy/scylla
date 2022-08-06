/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

include "utils/UUID.hh"
include "schema_basic_types.hh"
include "query-request.hh"

namespace utils {
class UUID final {
    int64_t get_most_significant_bits();
    int64_t get_least_significant_bits();
};
}

class table_id final {
    utils::UUID uuid();
};

class table_schema_version final {
    utils::UUID uuid();
};

class query_id final {
    utils::UUID uuid();
};
