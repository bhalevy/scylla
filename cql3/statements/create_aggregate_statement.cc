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

#include "cql3/functions/functions.hh"
#include "cql3/statements/create_aggregate_statement.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"

namespace cql3 {

namespace statements {

shared_ptr<functions::user_aggregate> create_aggregate_statement::create(database& db, sstring keyspace, sstring name,
        std::vector<data_type> arg_types, sstring s_func_name, data_type s_type, sstring final_func_name,
        shared_ptr<term::raw> raw_initial_value) {
    functions::function_name q_name{keyspace, name};

    std::vector<data_type> s_func_types = {s_type};
    s_func_types.insert(s_func_types.end(), arg_types.begin(), arg_types.end());

    functions::function_name s_func_q_name{keyspace, s_func_name};
    auto s_func =
            dynamic_pointer_cast<functions::user_function>(functions::functions::find(s_func_q_name, s_func_types));
    if (!s_func) {
        throw exceptions::invalid_request_exception(format("SFUNC {}({}) was not found", s_func_q_name, s_func_types));
    }

    auto s_type_cql = s_type->as_cql3_type();

    shared_ptr<functions::user_function> final_func;
    if (!final_func_name.empty()) {
        functions::function_name final_func_q_name(keyspace, final_func_name);
        final_func =
                dynamic_pointer_cast<functions::user_function>(functions::functions::find(final_func_q_name, {s_type}));
        if (!final_func) {
            throw exceptions::invalid_request_exception(
                    format("FINALFUNC {}({}) was not found", final_func_q_name, s_type_cql));
        }
    }

    if (s_func->return_type() != s_type) {
        throw exceptions::invalid_request_exception(
                format("SFUNC '{}' should return S_TYPE({})", s_func, s_type_cql));
    }

    data_value initial_value_dv = data_value::make_null(s_type);
    if (raw_initial_value) {
        // FIXME: do we really need the dummy column?
        auto dummy_column =
                make_lw_shared<column_specification>(keyspace, "foo", make_shared<column_identifier>("bar", true), s_type);
        ::shared_ptr<term> initial_value = raw_initial_value->prepare(db, keyspace, dummy_column);
        bytes b = to_bytes(initial_value->bind_and_get(query_options::DEFAULT));
        initial_value_dv = s_type->deserialize(b);
    } else if (!s_func->called_on_null_input()) {
        throw exceptions::invalid_request_exception(
                format("INITCOND is required since '{}' cannot be called on null", s_func));
    }

    return ::make_shared<functions::user_aggregate>(std::move(q_name), std::move(arg_types), std::move(s_func),
            std::move(final_func), std::move(initial_value_dv));
}

void create_aggregate_statement::create(service::storage_proxy& proxy, functions::function* old) const {
    if (old && !dynamic_cast<functions::user_aggregate*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined aggregate", *old));
    }
    auto&& db = proxy.get_db().local();
    const sstring& keyspace = _name.keyspace;
    data_type s_type = _s_type->prepare(db, keyspace).get_type();
    _agg = create(db, keyspace, _name.name, _arg_types, _s_func, s_type, _final_func, _initial_value);
    return;
}

std::unique_ptr<prepared_statement> create_aggregate_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_aggregate_statement>(*this));
}

future<shared_ptr<cql_transport::event::schema_change>> create_aggregate_statement::announce_migration(
        service::storage_proxy& proxy, bool is_local_only) const {
    if (!_agg) {
        return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>();
    }
    return service::get_local_migration_manager().announce_new_aggregate(_agg, is_local_only).then([this] {
        return create_schema_change(*_agg, true);
    });
}

create_aggregate_statement::create_aggregate_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, sstring s_func, shared_ptr<cql3_type::raw> s_type,
        sstring final_func, shared_ptr<term::raw> initial_value, bool or_replace, bool if_not_exists)
    : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists),
      _s_func(std::move(s_func)), _s_type(std::move(s_type)), _final_func(std::move(final_func)),
      _initial_value(std::move(initial_value)) {}
}
}
