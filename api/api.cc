/*
 * Copyright 2015-present ScyllaDB
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

#include "api.hh"
#include <seastar/http/file_handler.hh>
#include <seastar/http/transformers.hh>
#include <seastar/http/api_docs.hh>
#include "storage_service.hh"
#include "commitlog.hh"
#include "gossiper.hh"
#include "failure_detector.hh"
#include "column_family.hh"
#include "lsa.hh"
#include "messaging_service.hh"
#include "storage_proxy.hh"
#include "cache_service.hh"
#include "collectd.hh"
#include "endpoint_snitch.hh"
#include "compaction_manager.hh"
#include "hinted_handoff.hh"
#include "error_injection.hh"
#include <seastar/http/exception.hh>
#include "stream_manager.hh"
#include "system.hh"
#include "api/config.hh"
#include "api/api_job.hh"
#include "utils/UUID_gen.hh"
#include <seastar/core/sleep.hh>

logging::logger apilog("api");

namespace api {

static std::unique_ptr<reply> exception_reply(std::exception_ptr eptr) {
    try {
        std::rethrow_exception(eptr);
    } catch (const no_such_keyspace& ex) {
        throw bad_param_exception(ex.what());
    }
    // We never going to get here
    throw std::runtime_error("exception_reply");
}

future<> set_server_init(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);
    auto rb02 = std::make_shared < api_registry_builder20 > (ctx.api_doc, "/v2");

    return ctx.http_server.set_routes([rb, &ctx, rb02](routes& r) {
        r.register_exeption_handler(exception_reply);
        r.put(GET, "/ui", new httpd::file_handler(ctx.api_dir + "/index.html",
                new content_replace("html")));
        r.add(GET, url("/ui").remainder("path"), new httpd::directory_handler(ctx.api_dir,
                new content_replace("html")));
        rb->set_api_doc(r);
        rb02->set_api_doc(r);
        rb02->register_api_file(r, "swagger20_header");
        rb->register_function(r, "system",
                "The system related API");
        set_system(ctx, r);
    });
}

future<> set_server_config(http_context& ctx, const db::config& cfg) {
    auto rb02 = std::make_shared < api_registry_builder20 > (ctx.api_doc, "/v2");
    return ctx.http_server.set_routes([&ctx, &cfg, rb02](routes& r) {
        set_config(rb02, ctx, r, cfg);
    });
}

static future<> register_api(http_context& ctx, const sstring& api_name,
        const sstring api_desc,
        std::function<void(http_context& ctx, routes& r)> f) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx, api_name, api_desc, f](routes& r) {
        rb->register_function(r, api_name, api_desc);
        f(ctx,r);
    });
}

future<> set_transport_controller(http_context& ctx, cql_transport::controller& ctl) {
    return ctx.http_server.set_routes([&ctx, &ctl] (routes& r) { set_transport_controller(ctx, r, ctl); });
}

future<> unset_transport_controller(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_transport_controller(ctx, r); });
}

future<> set_rpc_controller(http_context& ctx, thrift_controller& ctl) {
    return ctx.http_server.set_routes([&ctx, &ctl] (routes& r) { set_rpc_controller(ctx, r, ctl); });
}

future<> unset_rpc_controller(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_rpc_controller(ctx, r); });
}

future<> set_server_storage_service(http_context& ctx, sharded<service::storage_service>& ss, sharded<gms::gossiper>& g, sharded<cdc::generation_service>& cdc_gs) {
    return register_api(ctx, "storage_service", "The storage service API", [&ss, &g, &cdc_gs] (http_context& ctx, routes& r) {
            set_storage_service(ctx, r, ss, g.local(), cdc_gs);
        });
}

future<> set_server_sstables_loader(http_context& ctx, sharded<sstables_loader>& sst_loader) {
    return ctx.http_server.set_routes([&ctx, &sst_loader] (routes& r) { set_sstables_loader(ctx, r, sst_loader); });
}

future<> unset_server_sstables_loader(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_sstables_loader(ctx, r); });
}

future<> set_server_view_builder(http_context& ctx, sharded<db::view::view_builder>& vb) {
    return ctx.http_server.set_routes([&ctx, &vb] (routes& r) { set_view_builder(ctx, r, vb); });
}

future<> unset_server_view_builder(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_view_builder(ctx, r); });
}

future<> set_server_repair(http_context& ctx, sharded<repair_service>& repair) {
    return ctx.http_server.set_routes([&ctx, &repair] (routes& r) { set_repair(ctx, r, repair); });
}

future<> unset_server_repair(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_repair(ctx, r); });
}

future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl) {
    return ctx.http_server.set_routes([&ctx, &snap_ctl] (routes& r) { set_snapshot(ctx, r, snap_ctl); });
}

future<> unset_server_snapshot(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_snapshot(ctx, r); });
}

future<> set_server_snitch(http_context& ctx) {
    return register_api(ctx, "endpoint_snitch_info", "The endpoint snitch info API", set_endpoint_snitch);
}

future<> set_server_gossip(http_context& ctx, sharded<gms::gossiper>& g) {
    return register_api(ctx, "gossiper",
                "The gossiper API", [&g] (http_context& ctx, routes& r) {
                    set_gossiper(ctx, r, g.local());
                });
}

future<> set_server_load_sstable(http_context& ctx) {
    return register_api(ctx, "column_family",
                "The column family API", set_column_family);
}

future<> set_server_messaging_service(http_context& ctx, sharded<netw::messaging_service>& ms) {
    return register_api(ctx, "messaging_service",
                "The messaging service API", [&ms] (http_context& ctx, routes& r) {
                    set_messaging_service(ctx, r, ms);
                });
}
future<> unset_server_messaging_service(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_messaging_service(ctx, r); });
}

future<> set_server_storage_proxy(http_context& ctx, sharded<service::storage_service>& ss) {
    return register_api(ctx, "storage_proxy",
                "The storage proxy API", [&ss] (http_context& ctx, routes& r) {
                    set_storage_proxy(ctx, r, ss);
                });
}

future<> set_server_stream_manager(http_context& ctx, sharded<streaming::stream_manager>& sm) {
    return register_api(ctx, "stream_manager",
                "The stream manager API", [&sm] (http_context& ctx, routes& r) {
                    set_stream_manager(ctx, r, sm);
                });
}

future<> unset_server_stream_manager(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_stream_manager(ctx, r); });
}

future<> set_server_cache(http_context& ctx) {
    return register_api(ctx, "cache_service",
            "The cache service API", set_cache_service);
}

future<> set_hinted_handoff(http_context& ctx, sharded<gms::gossiper>& g) {
    return register_api(ctx, "hinted_handoff",
                "The hinted handoff API", [&g] (http_context& ctx, routes& r) {
                    set_hinted_handoff(ctx, r, g.local());
                });
}

future<> unset_hinted_handoff(http_context& ctx) {
    return ctx.http_server.set_routes([&ctx] (routes& r) { unset_hinted_handoff(ctx, r); });
}

future<> set_server_gossip_settle(http_context& ctx, sharded<gms::gossiper>& g) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx, &g](routes& r) {
        rb->register_function(r, "failure_detector",
                "The failure detector API");
        set_failure_detector(ctx, r, g.local());
    });
}

future<> set_server_compaction_manager(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        rb->register_function(r, "compaction_manager",
                "The Compaction manager API");
        set_compaction_manager(ctx, r);
    });
}

future<> set_server_done(http_context& ctx) {
    auto rb = std::make_shared < api_registry_builder > (ctx.api_doc);

    return ctx.http_server.set_routes([rb, &ctx](routes& r) {
        rb->register_function(r, "lsa", "Log-structured allocator API");
        set_lsa(ctx, r);

        rb->register_function(r, "commitlog",
                "The commit log API");
        set_commitlog(ctx,r);
        rb->register_function(r, "collectd",
                "The collectd API");
        set_collectd(ctx, r);
        rb->register_function(r, "error_injection",
                "The error injection API");
        set_error_injection(ctx, r);
    });
}

api_job::api_job(http_context& ctx, sstring category, sstring name, sstring keyspace, sstring table, std::unordered_map<sstring, sstring> params, utils::UUID parent_uuid, utils::UUID uuid, clock::duration ttl)
    : _ctx(ctx)
    , _category(category)
    , _name(std::move(name))
    , _keyspace(std::move(keyspace))
    , _table(std::move(table))
    , _uuid(uuid != utils::UUID() ? uuid : utils::UUID_gen::get_time_UUID())
    , _parent_uuid(parent_uuid)
    , _params(std::move(params))
    , _ttl(ttl)
{
}

void api_job::set_action(future<json::json_return_type> fut) {
    auto me = shared_from_this();
    auto [it, inserted] = _ctx.api_jobs.emplace(std::make_pair(_uuid, me));
    if (!inserted) {
        on_internal_error(apilog, fmt::format("api_job: job UUID {} already exists", _uuid));
    }
    // Run the action in the background
    // and pass its status to the shared promise when done.
    // Then, set the timer to remove it from the tracker.
    _start_time = clock::now();
    (void)std::move(fut).then_wrapped([this] (future<json::json_return_type> fut) {
        if (!fut.failed()) {
            succeeded(std::move(fut).get0());
        } else {
            auto ex = fut.get_exception();
            if (_as.abort_requested()) {
                ex = std::make_exception_ptr(abort_requested_exception());
            }
            failed(std::move(ex));
        }
        _completion_time = clock::now();
        if (_killed) {
            return make_ready_future<>();
        }
        _as = abort_source();
        return sleep_abortable(_ttl, _as);
    }).finally([this, me = std::move(me)] {
        _ctx.api_jobs.erase(_uuid);
    });
}

future<> http_context::stop() noexcept {
    return parallel_for_each(api_jobs, [] (std::pair<const utils::UUID, lw_shared_ptr<api_job>>& p) {
        auto& job = p.second;
        job->kill();
        return job->wait().discard_result();
    }).finally([this] {
        assert(api_jobs.empty());
    });
}

} // namespace api
