/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/abort_source.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/lister.hh"
#include "utils/s3/client.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "schema/schema_fwd.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstables.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/component_type.hh"
#include "utils/error_injection.hh"

extern logging::logger snap_log;

namespace db::snapshot {

backup_task_impl::backup_task_impl(tasks::task_manager::module_ptr module,
                                   snapshot_ctl& ctl,
                                   sharded<sstables::storage_manager>& sstm,
                                   sstring endpoint,
                                   sstring bucket,
                                   sstring prefix,
                                   sstring ks,
                                   std::filesystem::path snapshot_dir,
                                   table_id tid,
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _sstm(sstm)
    , _endpoint(std::move(endpoint))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
    , _table_id(tid)
    , _remove_on_uploaded(move_files) {
    _status.progress_units = "bytes";
}

std::string backup_task_impl::type() const {
    return "backup";
}

tasks::is_internal backup_task_impl::is_internal() const noexcept {
    return tasks::is_internal::no;
}

tasks::is_abortable backup_task_impl::is_abortable() const noexcept {
    return tasks::is_abortable::yes;
}

future<tasks::task_manager::task::progress> backup_task_impl::get_progress() const {
    auto ret = _progress;

    if (_sharded_sstables_manager.local_is_initialized()) {
        ret.completed = co_await _sharded_sstables_manager.map_reduce0([](const auto& m) {
            return m.progress.uploaded;
        }, size_t(0), std::plus<size_t>());
    }

    co_return ret;
}

tasks::is_user_task backup_task_impl::is_user_task() const noexcept {
    return tasks::is_user_task::yes;
}

future<> backup_task_impl::sstables_manager_for_table::upload_component(sstring name) {
    auto component_name = _task._snapshot_dir / name;
    auto destination = fmt::format("/{}/{}/{}", _task._bucket, _task._prefix, name);
    snap_log.trace("Upload {} to {}", component_name.native(), destination);

    // Start uploading in the background. The caller waits for these fibers
    // with the uploads gate.
    // Parallelism is implicitly controlled in two ways:
    //  - s3::client::claim_memory semaphore
    //  - http::client::max_connections limitation
    try {
        co_await _client->upload_file(component_name, destination, progress, &_abort);
    } catch (const abort_requested_exception&) {
        snap_log.info("Upload aborted per requested: {}", component_name.native());
        co_return;
    } catch (...) {
        snap_log.error("Error uploading {}: {}", component_name.native(), std::current_exception());
        throw;
    }

    if (!_task._remove_on_uploaded) {
        co_return;
    }

    // Delete the uploaded component to:
    // 1. Free up disk space immediately
    // 2. Avoid costly S3 existence checks on future backup attempts
    try {
        co_await remove_file(component_name.native());
    } catch (...) {
        // If deletion of an uploaded file fails, the backup process will continue.
        // While this doesn't halt the backup, it may indicate filesystem permissions
        // issues or system constraints that should be investigated.
        snap_log.warn("Failed to remove {}: {}", component_name, std::current_exception());
    }

    co_return;
}

future<> backup_task_impl::process_snapshot_dir() {
    auto snapshot_dir_lister = directory_lister(_snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());

    try {
        snap_log.debug("backup_task: listing {}", _snapshot_dir.native());
        while (auto component_ent = co_await snapshot_dir_lister.get()) {
            const auto& name = component_ent->name;
            auto file_path = _snapshot_dir / name;
            try {
                auto st = co_await file_stat(file_path.native());
                _progress.total += st.size;

                auto desc = sstables::parse_path(file_path, "", "");
                _sstable_comps[desc.generation].emplace_back(name);
                _sstables_in_snapshot.insert(desc.generation);
                ++_num_sstable_comps;

                if (desc.component == sstables::component_type::Data) {
                    // If the sstable is already unlinked after the snapshot was taken
                    // track its generation in the unlinked_sstables list
                    // so it can be prioritized for backup
                    if (st.number_of_links == 1) {
                        snap_log.trace("do_backup: sstable with gen={} is already unlinked", desc.generation);
                        _unlinked_sstables.push_back(desc.generation);
                    }
                }
            } catch (const sstables::malformed_sstable_exception&) {
                _queue.emplace_back(name);
            }
        }
        snap_log.debug("backup_task: found {} SSTables consisting of {} component files, and {} non-sstable files",
            _sstable_comps.size(), _num_sstable_comps, _queue.size());
    } catch (...) {
        _ex = std::current_exception();
        snap_log.error("backup_task: listing {} failed: {}", _snapshot_dir.native(), _ex);
    }

    co_await snapshot_dir_lister.close();
    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

future<> backup_task_impl::sstables_manager_for_table::backup_file(sstring name, semaphore_units<> units) {
    try {
        auto gh = _async_gate.hold();
        co_await upload_component(std::move(name));
    } catch (const abort_requested_exception&) {
        // Ignore
    } catch (...) {
        snap_log.debug("backup_file {} failed: {}", name, std::current_exception());
        // keep the first exception
        if (!_ex) {
            _ex = std::current_exception();
        }
    }
};

backup_task_impl::sstables_manager_for_table::sstables_manager_for_table(const replica::database& db, table_id t, backup_task_impl& task,
        std::function<future<>(sstables::generation_type gen, sstables::manager_event_type event)> callback)
    : _manager(db.get_sstables_manager(*db.find_schema(t)))
    , _sub(_manager.subscribe(callback))
    , _task(task)
    , _client(task._sstm.local().get_endpoint_client(task._endpoint))
{
    _done_fut = uploads_worker();
}

future<> backup_task_impl::sstables_manager_for_table::done() {
    return std::exchange(_done_fut, make_ready_future());
}

future<> backup_task_impl::sstables_manager_for_table::stop() {
    return done().handle_exception([] (std::exception_ptr ex) {
        on_internal_error(snap_log, format("backup_task_impl::sstables_manager_for_table failue wasn't processed: {}", ex));
    });
}

void backup_task_impl::sstables_manager_for_table::abort() {
    _abort.request_abort();
}

future<> backup_task_impl::sstables_manager_for_table::uploads_worker() {
    auto gh = _async_gate.hold();
    while (!_abort.abort_requested() && !_ex) {
        // Pre-upload break point. For testing abort in actual s3 client usage.
        co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));

        auto units = co_await _manager.dir_semaphore().get_units(1, _abort);
        auto name_opt = co_await smp::submit_to(_task._backup_shard, [this] () {
            return _task.dequeue();
        });
        // done?
        if (!name_opt) {
            break;
        }
        // okay to drop future since async_gate is always closed before stopping
        std::ignore = backup_file(std::move(*name_opt), std::move(units));

        co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
    }
    gh.release();
    co_await _async_gate.close();
    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

void backup_task_impl::on_unlink(sstables::generation_type gen) {
    // Check that _sstable_comps contains `gen`.
    // Otherwise, it was already uploaded.
    if (_sstable_comps.contains(gen)) {
        _unlinked_sstables.push_back(gen);
    }
}

std::optional<std::string> backup_task_impl::dequeue() {
    if (_queue.empty()) {
        dequeue_sstable();
    }
    if (_queue.empty()) {
        return std::nullopt;
    }
    auto ret = _queue.back();
    _queue.pop_back();
    return ret;
}

void backup_task_impl::dequeue_sstable() {
    auto to_backup = _sstable_comps.begin();
    if (to_backup == _sstable_comps.end()) {
        return;
    }
    // Prioritize unlinked sstables to free-up their disk space earlier.
    // This is particularly important when running backup at high utilization levels (e.g. over 90%)
    while (!_unlinked_sstables.empty()) {
        auto gen = _unlinked_sstables.back();
        _unlinked_sstables.pop_back();
        if (auto it = _sstable_comps.find(gen); it != _sstable_comps.end()) {
            snap_log.debug("Prioritizing unlinked sstable gen={}", gen);
            to_backup = it;
            break;
        } else {
            snap_log.trace("Unlinked sstable gen={} was not found", gen);
        }
    }
    auto ent = _sstable_comps.extract(to_backup);
    snap_log.debug("Backing up SSTable generation {}", ent.key());
    for (auto& name : ent.mapped()) {
        _queue.emplace_back(std::move(name));
    }
}

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    co_await process_snapshot_dir();

    _backup_shard = this_shard_id();
    co_await _sharded_sstables_manager.start(std::ref(_snap_ctl.db()), _table_id, std::ref(*this),
            [this] (sstables::generation_type gen, sstables::manager_event_type event) -> future<> {
        switch (event) {
        case sstables::manager_event_type::add:
            break;
        case sstables::manager_event_type::unlink:
            // The notification is called for any sstable, so `gen` may belong
            // to another table, or to an sstable that was created after the snapshot
            // was taken.
            // To avoid needless call to submit_to on another shard (which is expensive),
            // check if `gen` was included in the snapshot.
            //
            // This is safe although `_sstables_in_snapshot` was created on `baskup_shard`,
            // since the set is immutable after `process_snapshot_dir` is done.
            if (_sstables_in_snapshot.contains(gen)) {
                return smp::submit_to(_backup_shard, [this, gen] {
                    on_unlink(gen);
                });
            }
        }
        return make_ready_future();
    });

    _abort_sub = _as.subscribe([this] () noexcept {
        // Safe to ignore future since we're waiting on done
        std::ignore = _sharded_sstables_manager.invoke_on_all([] (sstables_manager_for_table& m) {
            m.abort();
        });
    });

    try {
        co_await _sharded_sstables_manager.invoke_on_all([] (sstables_manager_for_table& m) {
            return m.done();
        });
    } catch (...) {
        _ex = std::current_exception();
    }

    _progress.completed = co_await _sharded_sstables_manager.map_reduce0([](const auto& m) {
        return m.progress.uploaded;
    }, size_t(0), std::plus<size_t>());

    _abort_sub.reset();

    co_await _sharded_sstables_manager.stop();
    if (_ex) {
        co_await coroutine::return_exception_ptr(std::move(_ex));
    }
}

future<> backup_task_impl::run() {
    // do_backup() removes a file once it is fully uploaded, so we are actually
    // mutating snapshots.
    co_await _snap_ctl.run_snapshot_modify_operation([this] {
        return do_backup();
    });
    snap_log.info("Finished backup");
}

} // db::snapshot namespace
