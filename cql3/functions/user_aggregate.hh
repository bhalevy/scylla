/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "user_function.hh"
#include "aggregate_function.hh"

namespace cql3 {
namespace functions {

class user_aggregate final : public abstract_function, public aggregate_function {
    shared_ptr<user_function> _state_update_func;
    shared_ptr<user_function> _final_func;
    data_value _initial_value;

public:
    user_aggregate(function_name name, std::vector<data_type> arg_types, shared_ptr<user_function> state_update_func,
            shared_ptr<user_function> final_func, data_value initial_value);
    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool is_aggregate() const override;
    virtual std::unique_ptr<aggregate> new_aggregate() override;

    const shared_ptr<user_function>& state_update_func() const { return _state_update_func; }
    const data_type& state_type() const { return _initial_value.type(); }
    const shared_ptr<user_function>& final_func() const { return _final_func; }
    const data_value& initial_value() const { return _initial_value; }
};

}
}
