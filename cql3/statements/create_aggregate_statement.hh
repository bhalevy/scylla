/*
 * Copyright (C) 2019 ScyllaDB
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

#include "cql3/functions/user_aggregate.hh"
#include "cql3/statements/function_statement.hh"
#include "cql3/term.hh"

namespace cql3 {
namespace statements {
class create_aggregate_statement final : public create_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(service::storage_proxy& proxy, bool is_local_only) const override;
    virtual void create(service::storage_proxy& proxy, functions::function* old) const override;

    sstring _s_func;
    shared_ptr<cql3_type::raw> _s_type;
    sstring _final_func;
    shared_ptr<term::raw> _initial_value;

    mutable shared_ptr<functions::user_aggregate> _agg{};

public:
    static shared_ptr<functions::user_aggregate> create(database& db, sstring keyspace, sstring name,
            std::vector<data_type> arg_types, sstring s_func, data_type s_type, sstring final_func,
            shared_ptr<term::raw> initial_value);

    create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring s_func, shared_ptr<cql3_type::raw> s_type, sstring final_func, shared_ptr<term::raw> initial_value,
            bool or_replace, bool if_not_exists);
};
}
}
