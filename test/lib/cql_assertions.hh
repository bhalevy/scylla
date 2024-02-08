
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message_base.hh"
#include "bytes.hh"
#include <source_location>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>

class rows_assertions {
    shared_ptr<cql_transport::messages::result_message::rows> _rows;
public:
    rows_assertions(shared_ptr<cql_transport::messages::result_message::rows> rows);
    rows_assertions with_size(size_t size);
    rows_assertions is_empty();
    rows_assertions is_not_empty();
    rows_assertions with_column_types(std::initializer_list<data_type> column_types);
    rows_assertions with_row(std::initializer_list<bytes_opt> values);

    // Verifies that the result has the following rows and only that rows, in that order.
    rows_assertions with_rows(std::vector<std::vector<bytes_opt>> rows);
    // Verifies that the result has the following rows and only those rows.
    rows_assertions with_rows_ignore_order(std::vector<std::vector<bytes_opt>> rows);
    rows_assertions with_serialized_columns_count(size_t columns_count);

    rows_assertions is_null();
    rows_assertions is_not_null();
};

class result_msg_assertions {
    shared_ptr<cql_transport::messages::result_message> _msg;
public:
    result_msg_assertions(shared_ptr<cql_transport::messages::result_message> msg);
    rows_assertions is_rows();
};

result_msg_assertions assert_that(shared_ptr<cql_transport::messages::result_message> msg);

template<typename... T>
void assert_that_failed(future<T...>& f)
{
    try {
        f.get();
        assert(f.failed());
    }
    catch (...) {
    }
}

template<typename... T>
void assert_that_failed(future<T...>&& f)
{
    try {
        f.get();
        assert(f.failed());
    }
    catch (...) {
    }
}

/// Invokes env.execute_cql(query), awaits its result, and returns it.  If an exception is thrown,
/// invokes BOOST_FAIL with useful diagnostics.
///
/// \note Should be called from a seastar::thread context, as it awaits the CQL result.
shared_ptr<cql_transport::messages::result_message> cquery_nofail(
        cql_test_env& env,
        sstring_view query,
        std::unique_ptr<cql3::query_options>&& qo = nullptr,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Asserts that cquery_nofail(e, qstr) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  sstring_view qstr,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Like require_rows, but wraps assertions in \c eventually.
void eventually_require_rows(
        cql_test_env& e, sstring_view qstr, const std::vector<std::vector<bytes_opt>>& expected,
        const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Asserts that e.execute_prepared(id, values) contains expected rows, in any order.
void require_rows(cql_test_env& e,
                  cql3::prepared_cache_key_type id,
                  const std::vector<cql3::raw_value>& values,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const seastar::compat::source_location& loc = seastar::compat::source_location::current());

/// Asserts that a cell at the given table.partition.row.column position contains expected data
future<> require_column_has_value(cql_test_env&, const sstring& table_name,
        std::vector<data_value> pk, std::vector<data_value> ck, const sstring& column_name, data_value expected);

/// Helper for simple pk and ck native types
template <typename P, typename C, typename V>
future<> require_column_has_value(cql_test_env& env, const sstring& table_name, std::initializer_list<P> pk, std::initializer_list<C> ck, const sstring& column_name, V expected) {
    auto pk_data = boost::copy_range<std::vector<data_value>>(pk | boost::adaptors::transformed([] (const P& x) { return data_value(x); }));
    auto ck_data = boost::copy_range<std::vector<data_value>>(ck | boost::adaptors::transformed([] (const C& x) { return data_value(x); }));
    return require_column_has_value(env, table_name, std::move(pk_data), std::move(ck_data), column_name, data_value(expected));
}

// Helper for empty ck
template <typename P, typename V>
future<> require_column_has_value(cql_test_env& env, const sstring& table_name, std::initializer_list<P> pk, std::vector<data_value> ck, const sstring& column_name, V expected) {
    auto pk_data = boost::copy_range<std::vector<data_value>>(pk | boost::adaptors::transformed([] (const P& x) { return data_value(x); }));
    return require_column_has_value(env, table_name, std::move(pk_data), std::move(ck), column_name, data_value(expected));
}
