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

#include "user_aggregate.hh"

namespace cql3 {
namespace functions {

user_aggregate::user_aggregate(function_name name, std::vector<data_type> arg_types,
    shared_ptr<functions::user_function> state_update_func,
    shared_ptr<functions::user_function> final_func, data_value initial_value)
    : abstract_function(std::move(name), std::move(arg_types),
          final_func ? final_func->return_type() : initial_value.type()),
      _state_update_func(std::move(state_update_func)), _final_func(std::move(final_func)),
      _initial_value(std::move(initial_value)) {}

bool user_aggregate::is_pure() const { return true; }
bool user_aggregate::is_native() const { return false; }
bool user_aggregate::is_aggregate() const { return true; }
std::unique_ptr<user_aggregate::aggregate> user_aggregate::new_aggregate() { return nullptr; }
}
}
