/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "test/lib/reader_permit.hh"
#include "query_class_config.hh"
#include "test/lib/simple_schema.hh"
#include "reader_concurrency_semaphore.hh"

class reader_concurrency_semaphore_for_tests {
    reader_concurrency_semaphore _semaphore;
public:
    reader_concurrency_semaphore_for_tests(sstring name = "test");

    simple_schema make_simple_schema() {
        return simple_schema(_semaphore);
    }

    simple_schema make_simple_schema(simple_schema::with_static ws) {
        return simple_schema(_semaphore, ws);
    }

    global_simple_schema make_global_simple_schema() {
        auto s = make_simple_schema();
        return global_simple_schema(s, _semaphore);
    }

    reader_permit make_permit() {
        return tests::make_permit(_semaphore);
    }

    reader_permit make_permit(const schema* const schema, const char* const op_name) {
        return _semaphore.make_permit(schema, op_name);
    }

    query::query_class_config make_query_class_config() {
        return tests::make_query_class_config(_semaphore);
    }
};
