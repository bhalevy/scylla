/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

include "utils/UUID.hh"
include "counters.hh"
include "schema_fwd.hh"
include "types/query_id.hh"

namespace utils {
class UUID final {
    int64_t get_most_significant_bits();
    int64_t get_least_significant_bits();
};
}

class counter_id final {
    utils::UUID to_uuid();
};

class table_id final {
    utils::UUID to_uuid();
};

class table_schema_version final {
    utils::UUID to_uuid();
};

class query_id final {
    utils::UUID to_uuid();
};
