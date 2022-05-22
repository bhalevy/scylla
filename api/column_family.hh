/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/range/adaptor/map.hpp>

#include "api.hh"
#include "api/api-doc/column_family.json.hh"
#include "replica/database.hh"
#include <seastar/core/future-util.hh>
#include <seastar/core/coroutine.hh>
#include <any>

namespace api {

void set_column_family(http_context& ctx, routes& r);

const utils::UUID& get_uuid(const sstring& name, const replica::database& db);
future<> foreach_column_family(http_context& ctx, const sstring& name, std::function<void(const replica::column_family&)> f);


template<class Mapper, class I, class Reducer>
requires std::same_as<futurize_t<std::invoke_result_t<Mapper, const replica::table&>>, future<I>> && std::invocable<Reducer, I, I>
future<I> map_reduce_cf_raw(http_context& ctx, const sstring& name, I&& init,
        Mapper&& mapper, Reducer&& reducer) {
    auto uuid = get_uuid(name, ctx.db.local());
    using mapper_type = std::function<future<std::unique_ptr<std::any>>(replica::database&)>;
    using reducer_type = std::function<std::unique_ptr<std::any>(std::unique_ptr<std::any>, std::unique_ptr<std::any>)>;
    auto&& r = co_await ctx.db.map_reduce0(mapper_type([mapper = std::move(mapper), uuid](replica::database& db) -> future<std::unique_ptr<std::any>> {
        co_return std::make_unique<std::any>(I(co_await futurize_invoke(mapper, db.find_column_family(uuid))));
    }), std::make_unique<std::any>(std::move(init)), reducer_type([reducer = std::move(reducer)] (std::unique_ptr<std::any> a, std::unique_ptr<std::any> b) mutable {
        return std::make_unique<std::any>(I(reducer(std::any_cast<I>(std::move(*a)), std::any_cast<I>(std::move(*b)))));
    }));
    co_return std::any_cast<I>(std::move(*r));
}


template<class Mapper, class I, class Reducer>
requires std::same_as<futurize_t<std::invoke_result_t<Mapper, const replica::table&>>, future<I>> && std::invocable<Reducer, I, I>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I&& init,
        Mapper&& mapper, Reducer&& reducer) {
    auto&& res = co_await map_reduce_cf_raw(ctx, name, std::forward<I>(init), std::forward<Mapper>(mapper), std::forward<Reducer>(reducer));
    co_return json::json_return_type(std::move(res));
}

template<class Mapper, class I, class Reducer, class Result>
requires std::same_as<futurize_t<std::invoke_result_t<Mapper, const replica::table&>>, future<I>> && std::invocable<Reducer, I, I> && std::is_assignable_v<Result, I>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I&& init,
        Mapper&& mapper, Reducer&& reducer, Result&& result_) {
    auto result = std::move(result_);
    auto&& res = co_await map_reduce_cf_raw(ctx, name, std::forward<I>(init), std::forward<Mapper>(mapper), std::forward<Reducer>(reducer));
    result = res;
    co_return json::json_return_type(std::move(result));
}

future<json::json_return_type> map_reduce_cf_time_histogram(http_context& ctx, const sstring& name, std::function<utils::time_estimated_histogram(const replica::column_family&)> f);

struct map_reduce_column_families_locally {
    std::any init;
    std::function<future<std::unique_ptr<std::any>>(const replica::column_family&)> mapper;
    std::function<std::unique_ptr<std::any>(std::unique_ptr<std::any>, std::unique_ptr<std::any>)> reducer;
    future<std::unique_ptr<std::any>> operator()(replica::database& db) const {
        auto res = std::make_unique<std::any>(init);
        for (const lw_shared_ptr<replica::table>& i : db.get_column_families() | boost::adaptors::map_values) {
            res = reducer(std::move(res), co_await mapper(*i));
        }
        co_return res;
    }
};

template<class Mapper, class I, class Reducer>
requires std::same_as<futurize_t<std::invoke_result_t<Mapper, const replica::table&>>, future<I>> && std::invocable<Reducer, I, I>
future<I> map_reduce_cf_raw(http_context& ctx, I&& init,
        Mapper&& mapper, Reducer&& reducer) {
    using mapper_type = std::function<future<std::unique_ptr<std::any>>(const replica::column_family&)>;
    using reducer_type = std::function<std::unique_ptr<std::any>(std::unique_ptr<std::any>, std::unique_ptr<std::any>)>;
    auto wrapped_mapper = mapper_type([mapper = std::move(mapper)] (const replica::column_family& cf) mutable -> future<std::unique_ptr<std::any>> {
        co_return std::make_unique<std::any>(I(co_await futurize_invoke(mapper, cf)));
    });
    auto wrapped_reducer = reducer_type([reducer = std::move(reducer)] (std::unique_ptr<std::any> a, std::unique_ptr<std::any> b) mutable {
        return std::make_unique<std::any>(I(reducer(std::any_cast<I>(std::move(*a)), std::any_cast<I>(std::move(*b)))));
    });
    auto&& res = co_await ctx.db.map_reduce0(map_reduce_column_families_locally{std::move(init),
            std::move(wrapped_mapper), wrapped_reducer}, std::make_unique<std::any>(init), wrapped_reducer);
    co_return std::any_cast<I>(std::move(*res));
}


template<class Mapper, class I, class Reducer>
requires std::same_as<futurize_t<std::invoke_result_t<Mapper, const replica::table&>>, future<I>> && std::invocable<Reducer, I, I>
future<json::json_return_type> map_reduce_cf(http_context& ctx, I&& init,
        Mapper&& mapper, Reducer&& reducer) {
    auto&& res = co_await map_reduce_cf_raw(ctx, std::forward<I>(init), std::forward<Mapper>(mapper), std::forward<Reducer>(reducer));
    co_return json::json_return_type(std::move(res));
}

future<json::json_return_type>  get_cf_stats(http_context& ctx, const sstring& name,
        int64_t replica::column_family_stats::*f);

future<json::json_return_type>  get_cf_stats(http_context& ctx,
        int64_t replica::column_family_stats::*f);


std::tuple<sstring, sstring> parse_fully_qualified_cf_name(sstring name);

}
