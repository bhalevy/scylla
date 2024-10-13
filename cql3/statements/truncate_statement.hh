/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"
#include "schema/schema_builder.hh"

namespace cql3 {

class query_processor;

namespace statements {

class truncate_statement : public schema_altering_statement {
public:
    schema_ptr _schema;
    const std::unique_ptr<attributes> _attrs;
public:
    truncate_statement(schema_ptr schema, std::unique_ptr<attributes> prepared_attrs);
    truncate_statement(const truncate_statement&);

    virtual uint32_t get_bound_terms() const override;

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;
private:
    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;
    std::pair<schema_ptr, std::vector<view_ptr>> prepare_schema_update(data_dictionary::database db, const query_options& options, api::timestamp_type timestamp) const;
};

}

}
