/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "db/timeout_clock.hh"

namespace cql3 {

class query_options;
class prepare_context;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
class attributes final {
private:
    std::optional<cql3::expr::expression> _timestamp;
    std::optional<cql3::expr::expression> _time_to_live;
    std::optional<cql3::expr::expression> _timeout;
public:
    static std::unique_ptr<attributes> none();
private:
    attributes(std::optional<cql3::expr::expression>&& timestamp,
               std::optional<cql3::expr::expression>&& time_to_live,
               std::optional<cql3::expr::expression>&& timeout);
public:
    enum class mask : unsigned {
        timestamp = 1 << 0,
        time_to_live = 1 << 1,
        timeout = 1 << 2,
    };

    // Get the set attributes as a bit mask
    unsigned get_set() const noexcept;

    bool is_timestamp_set() const;

    bool is_time_to_live_set() const;

    bool is_timeout_set() const;

    int64_t get_timestamp(int64_t now, const query_options& options);

    int32_t get_time_to_live(const query_options& options);

    db::timeout_clock::duration get_timeout(const query_options& options) const;

    void fill_prepare_context(prepare_context& ctx);

    class raw final {
    public:
        std::optional<cql3::expr::expression> timestamp;
        std::optional<cql3::expr::expression> time_to_live;
        std::optional<cql3::expr::expression> timeout;

        std::unique_ptr<attributes> prepare(data_dictionary::database db, const sstring& ks_name, const sstring& cf_name) const;

        // Get the set attributes as a bit mask
        unsigned get_set() const noexcept;
    private:
        lw_shared_ptr<column_specification> timestamp_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> time_to_live_receiver(const sstring& ks_name, const sstring& cf_name) const;

        lw_shared_ptr<column_specification> timeout_receiver(const sstring& ks_name, const sstring& cf_name) const;
    };
};

inline unsigned operator|(attributes::mask l, attributes::mask r) {
    return static_cast<unsigned>(l) | static_cast<unsigned>(r);
}

inline unsigned operator&(attributes::mask l, attributes::mask r) {
    return static_cast<unsigned>(l) & static_cast<unsigned>(r);
}

inline unsigned operator~(attributes::mask m) {
    return ~static_cast<unsigned>(m);
}

}
