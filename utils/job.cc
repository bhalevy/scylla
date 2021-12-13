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

#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sleep.hh>

#include "utils/job.hh"
#include "utils/UUID_gen.hh"
#include "log.hh"

namespace utils {

logging::logger jlogger("jobs");

static thread_local std::unordered_map<sstring, job::job_number_type> _module_job_number;

job::job(job_registry& registry, config cfg, future<> action)
    : _registry(registry)
    , _config(std::move(cfg))
    , _start_time(clock::now())
{
    auto me = shared_from_this();

    _registry.register_job(me);

    // run in background
    (void)std::move(action).then_wrapped([this, me] (future<> f) {
        _completion_time = clock::now();
        if (f.failed()) {
            mark_as_failed(f.get_exception());
        } else {
            make_as_succeeded();
        }
        if (_config.retention.count()) {
            _as = abort_source();
            return sleep(_config.retention);
        }
        return make_ready_future<>();
    }).finally([this, me = std::move(me)] {
        _registry.unregister_job(me);
    });
}

job_registry::~job_registry() {
    if (!_jobs.empty() || !_jobs_by_module.empty()) {
        on_internal_error_noexcept(jlogger, fmt::format("job_registry: jobs are still present when destroyed: jobs={} jobs_by_module={}", _jobs.size(), _jobs_by_module.size()));
    }
}

lw_shared_ptr<job> job_registry::find(UUID uuid) {
    auto it = _jobs.find(uuid);
    if (it != _jobs.end()) {
        return it->second;
    }
    return {};
}

lw_shared_ptr<job> job_registry::find(sstring module, job::job_number_type job_number) {
    // do not create _jobs_by_module[module] inadevertently
    auto jobs_it = _jobs_by_module.find(module);
    if (jobs_it != _jobs_by_module.end()) {
        auto it = jobs_it->second.find(job_number);
        if (it != jobs_it->second.end()) {
            return it->second;
        }
    }
    return {};
}

void job_registry::register_job(lw_shared_ptr<job> job) {
    auto& config = job->get_config();
    if (config.uuid == UUID()) {
        config.uuid = UUID_gen::get_time_UUID();
    }
    auto jobs_res = _jobs.emplace(config.uuid, job);
    if (!jobs_res.second) {
        on_internal_error(jlogger, fmt::format("UUID={} already registered", config.uuid));
    }

    if (!config.job_number) {
        config.job_number = ++_module_job_number[config.module];
    }

    auto jobs_bu_module_res = _jobs_by_module[config.module].emplace(config.job_number, job);
    if (!jobs_bu_module_res.second) {
        _jobs.erase(jobs_res.first);
        on_internal_error(jlogger, fmt::format("module={} job_number={} already registered", config.module, config.job_number));
    }
}

bool job_registry::unregister_job(lw_shared_ptr<job> job) {
    const auto& config = job->get_config();
    auto found = _jobs.erase(config.uuid);
    _jobs_by_module[config.module].erase(config.job_number);
    return found;
}

future<> job_registry::stop() noexcept {
    while (!_jobs.empty()) {
        auto job = std::move(_jobs.begin()->second);
        job->kill();
        co_await job->wait();
    }
    if (!_jobs_by_module.empty()) {
        on_internal_error_noexcept(jlogger, fmt::format("job_registry: jobs_by_module still present when stopped: jobs_by_module={}", _jobs_by_module.size()));
    }
}

} // namespace utils