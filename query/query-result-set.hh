/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once


#include <seastar/core/shared_ptr.hh>
#include <fmt/ostream.h>
#include "types/types.hh"
#include "schema/schema.hh"

#include <optional>
#include <stdexcept>

class mutation;

namespace query {

class result;

class no_value : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

class non_null_data_value {
    data_value _v;

public:
    explicit non_null_data_value(data_value&& v);
    operator const data_value&() const {
        return _v;
    }
};

inline bool operator==(const non_null_data_value& x, const non_null_data_value& y) {
    return static_cast<const data_value&>(x) == static_cast<const data_value&>(y);
}

// Result set row is a set of cells that are associated with a row
// including regular column cells, partition keys, as well as static values.
class result_set_row {
public:
    using map_type = std::unordered_map<const column_definition*, non_null_data_value>;
private:
    schema_ptr _schema;
    map_type _cells;
public:
    result_set_row(schema_ptr schema, map_type&& cells)
        : _schema{schema}
        , _cells{std::move(cells)}
    { }
    result_set_row(result_set_row&&) = default;
    result_set_row(const result_set_row&) = delete;
    result_set_row& operator=(result_set_row&&) = default;
    result_set_row& operator=(const result_set_row&) = delete;
    result_set_row copy() const {
        return {_schema, map_type{cells()}};
    }
    const schema& schema() const noexcept {
        return *_schema;
    }
    const column_definition* find_column(const sstring& name) const {
        return _schema->get_column_definition(to_bytes(name));
    }
    const column_definition& get_column(const sstring& name) const {
        if (auto cdef = find_column(name)) {
            return *cdef;
        }
        throw std::out_of_range(fmt::format("Row is missing column definition for '{}' in {}.{}", name, _schema->ks_name(), _schema->cf_name()));
    }
    // Look up a deserialized row cell value by column name
    const data_value*
    get_data_value(const sstring& column_name) const {
        return get_data_value(get_column(column_name));
    }
    const data_value*
    get_data_value(const column_definition& col) const {
        auto it = cells().find(&col);
        if (it == cells().end()) {
            return nullptr;
        }
        return &static_cast<const data_value&>(it->second);
    }
    // Look up a deserialized row cell value by column name
    template<typename T>
    std::optional<T>
    get(const sstring& column_name) const {
        return get<T>(get_column(column_name));
    }
    // Look up a deserialized row cell value by column definition
    template<typename T>
    std::optional<T>
    get(const column_definition& col) const {
        if (const auto *value = get_ptr<T>(col)) {
            return std::optional(*value);
        }
        return std::nullopt;
    }
    template<typename T>
    const T*
    get_ptr(const column_definition& col) const {
        const auto *value = get_data_value(col);
        if (value == nullptr) {
            return nullptr;
        }
        return &value_cast<T>(*value);
    }
    // throws no_value on error
    template<typename T>
    const T& get_nonnull(const sstring& column_name) const {
        return get_nonnull<T>(get_column(column_name));
    }
    template<typename T>
    const T& get_nonnull(const column_definition& col) const {
        auto v = get_ptr<std::remove_reference_t<T>>(col);
        if (v) {
            return *v;
        }
        throw no_value(col.name_as_text());
    }
    const map_type& cells() const { return _cells; }
    friend inline bool operator==(const result_set_row& x, const result_set_row& y) = default;
    friend std::ostream& operator<<(std::ostream& out, const result_set_row& row);
};

// Result set is an in-memory representation of query results in
// deserialized format. To obtain a result set, use the result_set_builder
// class as a visitor to query_result::consume() function.
class result_set {
public:
    using rows_type = utils::chunked_vector<result_set_row>;
private:
    schema_ptr _schema;
    rows_type _rows;
public:
    static result_set from_raw_result(schema_ptr, const partition_slice&, const result&);
    result_set(schema_ptr s, rows_type&& rows)
        : _schema(std::move(s)), _rows{std::move(rows)}
    { }
    explicit result_set(const mutation&);
    bool empty() const {
        return _rows.empty();
    }
    // throws std::out_of_range on error
    const result_set_row& row(size_t idx) const {
        if (idx >= _rows.size()) {
            throw std::out_of_range("no such row in result set: " + std::to_string(idx));
        }
        return _rows[idx];
    }
    const rows_type& rows() const {
        return _rows;
    }
    const schema_ptr& schema() const {
        return _schema;
    }
    friend inline bool operator==(const result_set& x, const result_set& y);
    friend std::ostream& operator<<(std::ostream& out, const result_set& rs);
};

inline bool operator==(const result_set& x, const result_set& y) {
    return x._rows == y._rows;
}

}

template <> struct fmt::formatter<query::result_set> : fmt::ostream_formatter {};
template <> struct fmt::formatter<query::result_set_row> : fmt::ostream_formatter {};
