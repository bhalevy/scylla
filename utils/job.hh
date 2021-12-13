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
#include <unordered_map>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/abort_source.hh>
#include "seastarx.hh"

#include "utils/UUID.hh"
#include "db_clock.hh"

namespace utils {

using clock = db_clock;
using namespace std::chrono_literals;

class job_registry;

class job : public enable_lw_shared_from_this<job> {
public:
    enum class completion_status {
        none,       // still running
        succeeded,  // ran to completion with no error
        failed,     // completed with an error
        aborted,    // without completing
    };

    using job_number_type = int64_t;

    struct config {
        sstring module;
        sstring name;
        sstring keyspace;
        sstring table;
        job_number_type job_number = 0;  // unique in module, auto-assigned if 0
        UUID uuid = {};
        UUID parent_uuid = {};
        std::unordered_map<sstring, sstring> params;
        bool abortable = true;
        clock::duration retention = 60s;  // how long to keep the job listed post completion
    };

private:
    job_registry& _registry;
    config _config;
    completion_status _status = completion_status::none;
    abort_source _as;
    bool _killed = false;
    shared_promise<> _promise;
    clock::time_point _start_time = {};
    clock::time_point _completion_time = {};

public:
    job(job_registry& registry, config cfg, future<> action);

    const config& get_config() const noexcept {
        return _config;
    }

    const auto& name() const noexcept {
        return _config.name;
    }

    const auto& uuid() const noexcept {
        return _config.uuid;
    }

    const auto& parent_uuid() const noexcept {
        return _config.parent_uuid;
    }

    const auto& params() const noexcept {
        return _config.params;
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
        abort(true);
    }

    future<> wait() const noexcept {
        return _promise.get_shared_future();
    }

private:
    config& get_config() noexcept {
        return _config;
    }

    void make_as_succeeded() noexcept {
        _status = completion_status::succeeded;
        _promise.set_value();
    }

    void mark_as_failed(std::exception_ptr ex) noexcept {
        _status = completion_status::failed;
        _promise.set_exception(std::move(ex));
    }

    friend class job_registry;
};

class job_registry {
    std::unordered_map<UUID, lw_shared_ptr<job>> _jobs;
    std::unordered_map<sstring, job::job_number_type> _module_job_number;
    std::unordered_map<sstring, std::unordered_map<job::job_number_type, lw_shared_ptr<job>>> _jobs_by_module;

public:
    job_registry() = default;
    ~job_registry();

    const std::unordered_map<UUID, lw_shared_ptr<job>>& get_jobs() const noexcept {
        return _jobs;
    }

    const std::unordered_map<sstring, std::unordered_map<job::job_number_type, lw_shared_ptr<job>>>& get_jobs_by_module() const noexcept {
        return _jobs_by_module;
    }

    lw_shared_ptr<job> find(UUID uuid);

    lw_shared_ptr<job> find(sstring module, job::job_number_type job_number);

    future<> stop() noexcept;

private:
    void register_job(lw_shared_ptr<job> job);

    // return true iff found;
    bool unregister_job(lw_shared_ptr<job> job);

    friend class job;
};

} // namespace utils
