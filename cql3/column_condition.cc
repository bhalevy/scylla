/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/column_condition.hh"
#include "statements/request_validations.hh"
#include "unimplemented.hh"
#include "lists.hh"
#include "maps.hh"
#include <boost/range/algorithm_ext/push_back.hpp>
#include "types/map.hh"
#include "types/list.hh"
#include "utils/like_matcher.hh"
#include "expr/expression.hh"

namespace cql3 {

static
expr::expression
update_for_lwt_null_equality_rules(const expr::expression& e) {
    using namespace expr;

    return search_and_replace(e, [] (const expression& e) -> expression {
        if (auto* binop = as_if<binary_operator>(&e)) {
            auto new_binop = *binop;
            new_binop.null_handling = expr::null_handling_style::lwt_nulls;
            return new_binop;
        }
        return e;
    });
}

column_condition::column_condition(expr::expression expr)
        : _expr(std::move(expr))
{
    // If a collection is multi-cell and not frozen, it is returned as a map even if the
    // underlying data type is "set" or "list". This is controlled by
    // partition_slice::collections_as_maps enum, which is set when preparing a read command
    // object. Representing a list as a map<timeuuid, listval> is necessary to identify the list field
    // being updated, e.g. in case of UPDATE t SET list[3] = null WHERE a = 1 IF list[3]
    // = 'key'
    //
    // We adjust for it by reinterpreting the returned value as a list, since the map
    // representation is not needed here.
    _expr = expr::adjust_for_collection_as_maps(_expr);

    _expr = expr::optimize_like(_expr);

    _expr = update_for_lwt_null_equality_rules(_expr);
}

void column_condition::collect_marker_specificaton(prepare_context& ctx) {
    expr::fill_prepare_context(_expr, ctx);
}

bool column_condition::applies_to(const expr::evaluation_inputs& inputs) const {
    static auto true_value = raw_value::make_value(data_value(true).serialize());
    return expr::evaluate(_expr, inputs) == true_value;
}

lw_shared_ptr<column_condition>
column_condition::prepare(const expr::expression& expr, data_dictionary::database db, const sstring& keyspace, const schema& schema){
    auto prepared = expr::prepare_expression(expr, db, keyspace, &schema, make_lw_shared<column_specification>("", "", make_shared<column_identifier>("IF condition", true), boolean_type));

    expr::for_each_expression<expr::column_value>(prepared, [] (const expr::column_value& cval) {
      auto def = cval.col;
      if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(format("PRIMARY KEY column '{}' cannot have IF conditions", def->name_as_text()));
      }
    });
    return make_lw_shared<column_condition>(std::move(prepared));
}

} // end of namespace cql3
