/*
 * Copyright 2021-present ScyllaDB
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

#include <exception>
#include <optional>
#include <unordered_map>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/json/json_elements.hh>
#include "seastarx.hh"

#include "utils/UUID.hh"
#include "db_clock.hh"

namespace api {

using clock = db_clock;
using namespace std::chrono_literals;

class http_context;

class api_job : public enable_lw_shared_from_this<api_job> {
public:
    enum class completion_status {
        none,       // still running
        succeeded,  // ran to completion with no error
        failed,     // completed with an error
        aborted,    // without completing
    };

private:
    http_context& _ctx;
    sstring _category;
    sstring _name;
    sstring _keyspace;
    sstring _table;
    utils::UUID _uuid;
    utils::UUID _parent_uuid;
    std::unordered_map<sstring, sstring> _params;
    clock::duration _ttl;   // time to live after done
    completion_status _status = completion_status::none;
    abort_source _as;
    bool _killed = false;
    shared_promise<json::json_return_type> _promise;
    clock::time_point _start_time = {};
    clock::time_point _completion_time = {};

public:
    api_job(http_context& ctx, sstring category, sstring name, sstring keyspace = {}, sstring table = {}, std::unordered_map<sstring, sstring> params = {}, utils::UUID parent_uuid = {}, utils::UUID uuid = {}, clock::duration ttl = 60s);

    const auto& name() const noexcept {
        return _name;
    }

    const auto& uuid() const noexcept {
        return _uuid;
    }

    const auto& parent_uuid() const noexcept {
        return _parent_uuid;
    }

    const auto& params() const noexcept {
        return _params;
    };

    completion_status status() const noexcept {
        return _status;
    }

    const abort_source& get_abort_source() const noexcept {
        return _as;
    }

    abort_source& get_abort_source() noexcept {
        return _as;
    }

    clock::time_point start_time() const noexcept {
        return _start_time;
    }

    clock::time_point completion_time() const noexcept {
        return _completion_time;
    }

    bool is_running() const noexcept {
        return _status == completion_status::none;
    }

    bool is_done() const noexcept {
        return !is_running();
    }

    void abort(bool killed = false) noexcept {
        if (is_running()) {
            _status = completion_status::aborted;
        }
        _killed = killed;
        _as.request_abort();
    }

    void kill() noexcept {
        return abort(true);
    }

    future<json::json_return_type> wait() const noexcept {
        return _promise.get_shared_future();
    }

    void set_action(future<json::json_return_type> fut);

private:
    void succeeded(json::json_return_type ret) noexcept {
        _status = completion_status::succeeded;
        _promise.set_value(std::move(ret));
    }

    void failed(std::exception_ptr ex) noexcept {
        _status = completion_status::failed;
        _promise.set_exception(std::move(ex));
    }
};

} // namespace api
