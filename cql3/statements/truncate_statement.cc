/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "gc_clock.hh"
#include "utils/assert.hh"
#include "cql3/statements/raw/truncate_statement.hh"
#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include <optional>
#include "validation.hh"

namespace cql3 {

namespace statements {

namespace raw {

truncate_statement::truncate_statement(cf_name name, std::unique_ptr<attributes::raw> attrs)
        : cf_statement(std::move(name))
        , _attrs(std::move(attrs))
{
    // Validate the attributes.
    // Currently, TRUNCATE supports only USING TIMEOUT and USING TIMESTAMP
    SCYLLA_ASSERT(!_attrs->time_to_live.has_value());
}

std::unique_ptr<prepared_statement> truncate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    auto ctx = get_prepare_context();
    prepared_attributes->fill_prepare_context(ctx);
    auto stmt = ::make_shared<cql3::statements::truncate_statement>(std::move(schema), std::move(prepared_attributes));
    return std::make_unique<prepared_statement>(std::move(stmt));
}

} // namespace raw

truncate_statement::truncate_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs)
    : schema_altering_statement(&timeout_config::truncate_timeout)
    , _schema{std::move(schema)}
    , _attrs(std::move(prepared_attrs))
{
}

truncate_statement::truncate_statement(const truncate_statement& ts)
    : schema_altering_statement(ts)
    , _schema(ts._schema)
    , _attrs(std::make_unique<attributes>(*ts._attrs))
{ }

uint32_t truncate_statement::get_bound_terms() const
{
    return 0;
}

bool truncate_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return false;
}

future<> truncate_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_column_family_access(keyspace(), column_family(), auth::permission::MODIFY);
}

std::unique_ptr<cql3::statements::prepared_statement> truncate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    utils::on_internal_error("truncate_statement cannot be prepared.");
}

future<::shared_ptr<cql_transport::messages::result_message>>
truncate_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const
{
    auto table = validation::validate_column_family(qp.db(), keyspace(), column_family());
    if (table->is_view()) {
        throw exceptions::invalid_request_exception("Cannot TRUNCATE materialized view directly; must truncate base table instead");
    }
    return schema_altering_statement::execute(qp, state, options, std::move(guard));
}

db::timeout_clock::duration truncate_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().truncate_timeout;
}

std::pair<schema_ptr, std::vector<view_ptr>> truncate_statement::prepare_schema_update(data_dictionary::database db, const query_options& options, api::timestamp_type ts) const {
    auto s = validation::validate_column_family(db, keyspace(), column_family());
    if (s->is_view()) {
        throw exceptions::invalid_request_exception("Cannot use TRUNCATE TABLE on a Materialized View");
    }

    tombstone truncate_tombstone(_attrs->get_timestamp(ts, options), gc_clock::now());
    auto cfm = schema_builder(s).with_truncate_tombstone(truncate_tombstone).build();

    auto cf = db.find_column_family(s);
    std::vector<view_ptr> view_updates;
    view_updates.reserve(cf.views().size());

    for (const auto& view : cf.views()) {
        view_updates.emplace_back(schema_builder(view).with_truncate_tombstone(truncate_tombstone).build());
    }

    return make_pair(std::move(cfm), std::move(view_updates));
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> truncate_statement::prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type ts) const {
    data_dictionary::database db = qp.db();
    auto [cfm, view_updates] = prepare_schema_update(db, options, ts);
    auto m = co_await service::prepare_column_family_update_announcement(qp.proxy(), std::move(cfm), std::move(view_updates), ts);

    using namespace cql_transport;
    auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            column_family());

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

}

}