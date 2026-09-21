/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/rwlock.hh>
#include <boost/intrusive/list.hpp>
#include <limits>
#include "sstables/shared_sstable.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/updateable_value.hh"
#include "utils/serialized_action.hh"
#include <vector>
#include <functional>
#include "compaction.hh"
#include "compaction_backlog_manager.hh"
#include "compaction/compaction_descriptor.hh"
#include "compaction/task_manager_module.hh"
#include "compaction_state.hh"
#include "strategy_control.hh"
#include "backlog_controller.hh"
#include "seastarx.hh"
#include "compaction/exceptions.hh"
#include "tombstone_gc.hh"
#include "utils/pluggable.hh"
#include "compaction/compaction_reenabler.hh"
#include "utils/disk_space_monitor.hh"

namespace db {
class compaction_history_entry;
class system_keyspace;
class config;
}

namespace sstables { class test_env_compaction_manager; }

namespace compaction {
using throw_if_stopping = bool_class<struct throw_if_stopping_tag>;

class compaction_task_executor;
class sstables_task_executor;
class major_compaction_task_executor;
class custom_compaction_task_executor;
class regular_compaction_task_executor;
class offstrategy_compaction_task_executor;
class rewrite_sstables_compaction_task_executor;
class rewrite_sstables_component_compaction_task_executor;
class split_compaction_task_executor;
class cleanup_sstables_compaction_task_executor;
class validate_sstables_compaction_task_executor;

inline owned_ranges_ptr make_owned_ranges_ptr(dht::token_range_vector&& ranges) {
    return make_lw_shared<const dht::token_range_vector>(std::move(ranges));
}

// Compaction manager provides facilities to submit and track compaction jobs on
// behalf of existing tables.
class compaction_manager: public peering_sharded_service<compaction_manager> {
public:
    using compaction_stats_opt = std::optional<compaction_stats>;
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
        uint64_t active_tasks = 0; // Number of compaction going on.
        int64_t errors = 0;
    };
    using scheduling_group = backlog_controller::scheduling_group;
    struct config {
        scheduling_group compaction_sched_group;
        scheduling_group maintenance_sched_group;
        size_t available_memory = 0;
        utils::updateable_value<float> static_shares = utils::updateable_value<float>(0);
        utils::updateable_value<float> max_shares = utils::updateable_value<float>(0);
        utils::updateable_value<uint32_t> throughput_mb_per_sec = utils::updateable_value<uint32_t>(0);
        // 0 means no limit, for both.
        utils::updateable_value<uint32_t> max_concurrent_jobs = utils::updateable_value<uint32_t>(0);
        utils::updateable_value<uint32_t> max_concurrent_maintenance_jobs = utils::updateable_value<uint32_t>(0);
        std::chrono::seconds flush_all_tables_before_major = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::days(1));
    };

public:
    class can_purge_tombstones_tag;
    using can_purge_tombstones = bool_class<can_purge_tombstones_tag>;

private:
    shared_ptr<compaction::task_manager_module> _task_manager_module;

    using compaction_task_executor_list_type = bi::list<
            compaction_task_executor,
            bi::base_hook<bi::list_base_hook<bi::link_mode<bi::auto_unlink>>>,
            bi::constant_time_size<false>>;
    // compaction manager may have N fibers to allow parallel compaction per shard.
    compaction_task_executor_list_type _tasks;

    // Possible states in which the compaction manager can be found.
    //
    // none: started, but not yet enabled. Once the compaction manager moves out of "none", it can
    //       never legally move back
    // stopped: stop() was called. The compaction_manager will never be running again
    //          and can no longer be used (although it is possible to still grab metrics, stats,
    //          etc)
    // running: running, started and enabled at least once. Whether new compactions are accepted or not is determined by the counter
    enum class state { none, stopped, running };
    state _state = state::none;
    // The compaction manager is initiated in the none state. It is moved to the running state when start() is invoked
    // and the service is immediately enabled.
    uint32_t _disabled_state_count = 0;

    bool is_disabled() const { return _state != state::running || _disabled_state_count > 0; }
    // precondition: is_disabled() is true.
    std::exception_ptr make_disabled_exception(compaction::compaction_group_view& cg);

    std::optional<future<>> _stop_future;

    stats _stats;
    seastar::metrics::metric_groups _metrics;
    double _last_backlog = 0.0f;

    // Store sstables that are being compacted at the moment. That's needed to prevent
    // a sstable from being compacted twice.
    std::unordered_set<sstables::shared_sstable> _compacting_sstables;

    // Per-shard scheduler for regular compaction.
    //
    // Dispatch is owned by a single fiber (compaction_scheduler_fiber) rather
    // than by the submitters, so that the shard can weigh its compaction groups
    // against one another and cap how many jobs run at once. Groups waiting for
    // dispatch are held in intrusive queues: with tablets there are thousands of
    // them per shard, so enqueue and dequeue must be O(1) and allocation-free.
    //
    // A group is linked into at most one queue at a time:
    //  - _ready_groups: has work to do and is waiting for a dispatch slot.
    //  - _deferred_groups: its last job was refused by the weight tracker; moved
    //    back to _ready_groups once a weight is released.
    using compaction_queue = boost::intrusive::list<compaction::compaction_state,
            boost::intrusive::member_hook<compaction::compaction_state,
                    compaction::compaction_state::queue_hook_type,
                    &compaction::compaction_state::queue_hook>,
            boost::intrusive::constant_time_size<false>>;
    compaction_queue _ready_groups;
    compaction_queue _deferred_groups;
    // Woken whenever a group becomes ready or a dispatch slot is released.
    condition_variable _scheduler_wakeup;
    std::optional<future<>> _scheduler_fiber;
    // Job slots.
    //
    // A compaction task is long lived: it selects and fences its sstables, then
    // produces one or more compaction jobs. Tasks run concurrently -- how many
    // of each kind is governed by _maintenance_ops_sem and _off_strategy_sem for
    // maintenance, and by one task per group for regular compaction -- while the
    // jobs they produce compete for a slot here. The job is the unit of work
    // that costs disk and CPU, so it is the unit worth capping.
    //
    // _max_jobs caps jobs of every class, and _max_maintenance_jobs caps the
    // maintenance ones alone. Regular compaction has no sub-cap, so it uses
    // every slot while no maintenance job is running, and a maintenance job
    // waits for one to be freed rather than preempting. Waiters are woken in
    // arrival order, so regular compaction cannot starve maintenance.
    //
    // Both are set from compaction_max_concurrent_jobs and
    // compaction_max_concurrent_maintenance_jobs, which default to no limit.
    static constexpr size_t unlimited_jobs = std::numeric_limits<size_t>::max();
    size_t _jobs_running = 0;
    size_t _maintenance_jobs_running = 0;
    size_t _max_jobs = unlimited_jobs;
    size_t _max_maintenance_jobs = unlimited_jobs;
    // Maintenance jobs blocked in acquire_job_slot(). The scheduler stands down
    // for them, so it has to tell them apart from a regular job waiting to
    // retry after a failure, which it need not defer to.
    size_t _maintenance_jobs_waiting = 0;
    // Jobs blocked in acquire_job_slot(), of every class, for the metric.
    size_t _jobs_waiting = 0;
    // Woken whenever a job slot is released.
    condition_variable _job_slot_released;
    // tracks taken weights of ongoing compactions, only one compaction per weight is allowed.
    // weight is value assigned to a compaction job that is log base N of total size of all input sstables.

    std::unordered_map<compaction::compaction_group_view*, compaction_state> _compaction_state;

    // Purpose is to serialize all maintenance (non regular) compaction activity to reduce aggressiveness and space requirement.
    // If the operation must be serialized with regular, then the per-table write lock must be taken.
    seastar::named_semaphore _maintenance_ops_sem = {1, named_semaphore_exception_factory{"maintenance operation"}};

    // This semaphore ensures that off-strategy compaction will be serialized for
    // all tables, to limit space requirement and protect against candidates
    // being picked more than once.
    seastar::named_semaphore _off_strategy_sem = {1, named_semaphore_exception_factory{"off-strategy compaction"}};

    utils::pluggable<db::system_keyspace> _sys_ks;

    std::function<void()> compaction_submission_callback();
    // all registered tables are reevaluated at a constant interval.
    // Submission is a NO-OP when there's nothing to do, so it's fine to call it regularly.
    static constexpr std::chrono::seconds periodic_compaction_submission_interval() { return std::chrono::seconds(3600); }

    config _cfg;
    timer<lowres_clock> _compaction_submission_timer;
    compaction_controller _compaction_controller;
    compaction_backlog_manager _backlog_manager;
    optimized_optional<abort_source::subscription> _early_abort_subscription;
    serialized_action _update_compaction_static_shares_action;
    utils::observer<float> _compaction_static_shares_observer;
    utils::observer<float> _compaction_max_shares_observer;
    utils::observer<uint32_t> _max_concurrent_jobs_observer;
    utils::observer<uint32_t> _max_concurrent_maintenance_jobs_observer;
    uint64_t _validation_errors = 0;

    class strategy_control;
    std::unique_ptr<strategy_control> _strategy_control;

    shared_tombstone_gc_state _shared_tombstone_gc_state;

    utils::disk_space_monitor::subscription _out_of_space_subscription;
    bool _in_critical_disk_utilization_mode = false;
private:
    // Requires task->_compaction_state.gate to be held and task to be registered in _tasks.
    future<compaction_stats_opt> perform_task(shared_ptr<compaction::compaction_task_executor> task, throw_if_stopping do_throw_if_stopping);

    // Return nullopt if compaction cannot be started
    std::optional<gate::holder> start_compaction(compaction_group_view& t);

    template<typename TaskExecutor, typename... Args>
    requires std::is_base_of_v<compaction_task_executor, TaskExecutor> &&
            std::is_base_of_v<compaction_task_impl, TaskExecutor> &&
    requires (compaction_manager& cm, throw_if_stopping do_throw_if_stopping, Args&&... args) {
        {TaskExecutor(cm, do_throw_if_stopping, std::forward<Args>(args)...)} -> std::same_as<TaskExecutor>;
    }
    future<compaction_manager::compaction_stats_opt> perform_compaction(throw_if_stopping do_throw_if_stopping, tasks::task_info parent_info, Args&&... args);

    void stop_tasks(const std::vector<shared_ptr<compaction::compaction_task_executor>>& tasks, sstring reason) noexcept;
    future<> await_tasks(std::vector<shared_ptr<compaction::compaction_task_executor>>, bool task_stopped) const noexcept;

    // Return the largest fan-in of the compactions currently running for t.
    unsigned current_compaction_fan_in_threshold(const compaction::compaction_group_view& t) const;

    // Return true if compaction can be initiated
    bool can_register_compaction(compaction::compaction_group_view& t, int weight, unsigned fan_in) const;
    // Register weight for a table. Do that only if can_register_weight()
    // returned true.
    void register_weight(compaction::compaction_group_view& t, int weight);
    // Deregister weight for a table.
    void deregister_weight(compaction::compaction_group_view& t, int weight);

    // Get candidates for compaction strategy, which are all sstables but the ones being compacted.
    future<std::vector<sstables::shared_sstable>> get_candidates(compaction::compaction_group_view& t) const;

    bool eligible_for_compaction(const sstables::shared_sstable& sstable) const;
    bool eligible_for_compaction(const sstables::frozen_sstable_run& sstable_run) const;

    template <std::ranges::range Range>
    requires std::convertible_to<std::ranges::range_value_t<Range>, sstables::shared_sstable> || std::convertible_to<std::ranges::range_value_t<Range>, sstables::frozen_sstable_run>
    std::vector<std::ranges::range_value_t<Range>> get_candidates(compaction_group_view& t, const Range& sstables) const;

    template <std::ranges::range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, sstables::shared_sstable>
    void register_compacting_sstables(const Range& range);

    template <std::ranges::range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, sstables::shared_sstable>
    void deregister_compacting_sstables(const Range& range);

    // gets the table's compaction state
    // throws std::out_of_range exception if not found.
    compaction_state& get_compaction_state(compaction::compaction_group_view* t);
    const compaction_state& get_compaction_state(compaction::compaction_group_view* t) const {
        return const_cast<compaction_manager*>(this)->get_compaction_state(t);
    }

    // Return true if compaction manager is enabled and
    // table still exists and compaction is not disabled for the table.
    inline bool can_proceed(compaction::compaction_group_view* t) const;

    // Dispatches regular compaction jobs for the groups in _ready_groups, for as
    // long as can_start_job() allows. Runs until the manager is disabled.
    future<> compaction_scheduler_fiber();
    future<> stop_compaction_scheduler() noexcept;
    // Queue a group for regular compaction dispatch and wake the scheduler.
    // A no-op if the group is already queued.
    void enqueue_regular_compaction(compaction::compaction_state& cs) noexcept;
    // Park a group whose job was refused due to an ongoing similar-sized
    // compaction, until a compaction weight is released.
    void defer_regular_compaction(compaction::compaction_state& cs) noexcept;
    // Move every deferred group back to the ready queue and wake the scheduler.
    void reevaluate_deferred_compactions() noexcept;
    // Starts a single regular compaction job for cs, in the background.
    void dispatch_regular_compaction(compaction::compaction_state& cs);

public:
    // Class of a compaction job, for the purpose of capping how many run at once.
    enum class job_class { regular, maintenance };

    // Held for the duration of one compaction job.
    class job_slot {
        compaction_manager* _cm;
        job_class _class;
    public:
        job_slot(compaction_manager& cm, job_class c) noexcept : _cm(&cm), _class(c) {}
        job_slot(job_slot&& o) noexcept : _cm(std::exchange(o._cm, nullptr)), _class(o._class) {}
        job_slot(const job_slot&) = delete;
        job_slot& operator=(job_slot&& o) noexcept {
            if (this != &o) {
                if (_cm) {
                    _cm->release_job_slot(_class);
                }
                _cm = std::exchange(o._cm, nullptr);
                _class = o._class;
            }
            return *this;
        }
        job_slot& operator=(const job_slot&) = delete;
        ~job_slot() {
            if (_cm) {
                _cm->release_job_slot(_class);
            }
        }
    };

    // Waits for a slot to run one compaction job of the given class.
    //
    // Ordering: acquire every lock a job needs -- _maintenance_ops_sem,
    // _off_strategy_sem, compaction_state::incremental_repair_lock -- BEFORE
    // asking for a slot, and never ask for one while about to take such a lock.
    // seastar's rwlock is a FIFO semaphore, so a waiting repair writer blocks
    // later readers; a job holding a slot and waiting for the read lock, behind
    // a job holding the read lock and waiting for a slot, would deadlock.
    // Returns nullopt if `as` is aborted while waiting: a job asked to stop
    // should not first wait out a slot it is not going to use.
    future<std::optional<job_slot>> acquire_job_slot(job_class c, abort_source* as = nullptr);

private:
public:
    // The ready group whose compaction is most overdue, by backlog. Public for
    // the same reason should_defer_to_maintenance() is: the scheduler fiber is
    // the only caller in production, and a test cannot drive it.
    // Precondition: _ready_groups is not empty.
    compaction::compaction_state& pick_next_ready_group();

    // Whether a maintenance job is waiting for a slot that only the overall
    // limit is keeping it from taking. Regular compaction takes its slot
    // synchronously in the scheduler fiber, while a maintenance job waits for
    // its continuation to run, so without standing down the fiber would win
    // every released slot and the maintenance job would wait indefinitely --
    // holding _maintenance_ops_sem, and for major the repair read lock too.
    //
    // If the maintenance sub-limit is what blocks the waiter, a freed slot
    // cannot go to it anyway, so regular compaction may as well use it.
    bool should_defer_to_maintenance() const noexcept {
        return _maintenance_jobs_waiting && _maintenance_jobs_running < _max_maintenance_jobs;
    }
private:
    bool can_start_job(job_class c) const noexcept {
        return _jobs_running < _max_jobs &&
                (c != job_class::maintenance || _maintenance_jobs_running < _max_maintenance_jobs);
    }
    // Defined out of line: reports a broken invariant through cmlog.
    void take_job_slot(job_class c) noexcept;
    void release_job_slot(job_class c) noexcept;
    // A configured value of 0 means no limit.
    static size_t job_limit(uint32_t configured) noexcept {
        return configured ? size_t(configured) : unlimited_jobs;
    }
    void set_max_jobs(size_t max_jobs, size_t max_maintenance_jobs) noexcept;

    // Performs a single regular compaction job for cs and, if there is more work
    // to do, queues the group again for the scheduler to weigh against the
    // others. Holds gh for the lifetime of the job, which keeps cs alive.
    future<> perform_regular_compaction_job(compaction::compaction_state& cs, gate::holder gh);

    using quarantine_invalid_sstables = compaction_type_options::scrub::quarantine_invalid_sstables;
    future<compaction_stats_opt> perform_sstable_scrub_validate_mode(compaction::compaction_group_view& t, tasks::task_info info, quarantine_invalid_sstables quarantine_sstables);
    future<> update_static_shares(float shares);

    using get_candidates_func = std::function<future<std::vector<sstables::shared_sstable>>()>;

    // Guarantees that a maintenance task, e.g. cleanup, will be performed on all files available at the time
    // by retrieving set of candidates only after all compactions for table T were stopped, if any.
    template<typename TaskType, typename... Args>
    requires std::derived_from<TaskType, compaction_task_executor> &&
            std::derived_from<TaskType, compaction_task_impl>

    future<compaction_manager::compaction_stats_opt> perform_task_on_all_files(sstring reason, tasks::task_info info, compaction_group_view& t, compaction_type_options options, owned_ranges_ptr owned_ranges_ptr,
                                                                               get_candidates_func get_func, throw_if_stopping do_throw_if_stopping, Args... args);

    future<compaction_stats_opt> rewrite_sstables(compaction::compaction_group_view& t, compaction_type_options options, owned_ranges_ptr, get_candidates_func, tasks::task_info info,
                                                  can_purge_tombstones can_purge = can_purge_tombstones::yes, sstring options_desc = "");

    future<compaction_stats_opt> rewrite_sstables_component(compaction_group_view& t,
                                                            std::function<bool(const sstables::shared_sstable&)> filter,
                                                            compaction_type_options options,
                                                            std::unordered_map<sstables::shared_sstable, sstables::shared_sstable>& rewritten_sstables,
                                                            tasks::task_info info);

    // Stop all fibers, without waiting. Safe to be called multiple times.
    void do_stop() noexcept;
    future<> really_do_stop() noexcept;

    // Propagate replacement of sstables to all ongoing compaction of a given table
    void propagate_replacement(compaction::compaction_group_view& t, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added);

    // This constructor is supposed to only be used for testing so lets be more explicit
    // about invoking it. Ref #10146
    compaction_manager(tasks::task_manager& tm);
public:
    compaction_manager(config cfg, abort_source& as, tasks::task_manager& tm);
    ~compaction_manager();
    class for_testing_tag{};
    // An inline constructor for testing
    compaction_manager(tasks::task_manager& tm, for_testing_tag) : compaction_manager(tm) {}

    compaction::task_manager_module& get_task_manager_module() noexcept {
        return *_task_manager_module;
    }

    const scheduling_group& compaction_sg() const noexcept {
        return _cfg.compaction_sched_group;
    }

    const scheduling_group& maintenance_sg() const noexcept {
        return _cfg.maintenance_sched_group;
    }

    size_t available_memory() const noexcept {
        return _cfg.available_memory;
    }

    float static_shares() const noexcept {
        return _cfg.static_shares.get();
    }

    float max_shares() const noexcept {
        return _cfg.max_shares.get();
    }

    uint32_t throughput_mbs() const noexcept {
        return _cfg.throughput_mb_per_sec.get();
    }

    std::chrono::seconds flush_all_tables_before_major() const noexcept {
        return _cfg.flush_all_tables_before_major;
    }

    void register_metrics();

    // Overrides the configured caps on concurrent compaction jobs, until the
    // configuration changes again. For tests, which have no configuration to
    // set.
    void set_max_jobs_for_tests(size_t max_jobs, size_t max_maintenance_jobs) noexcept {
        set_max_jobs(max_jobs, max_maintenance_jobs);
    }

    // enable the compaction manager.
    void enable();

    // Stop all fibers. Ongoing compactions will be waited. Should only be called
    // once, from main teardown path.
    future<> stop() noexcept;
    future<> start(const db::config& cfg, utils::disk_space_monitor* dsm);

    // cancels all running compactions and moves the compaction manager into disabled state.
    // The compaction manager is still alive after drain but it will not accept new compactions
    // unless it is moved back to enabled state.
    future<> drain();

    // Check if compaction manager is running, i.e. it was enabled or drained
    bool is_running() const noexcept {
        return _state == state::running;
    }

    using compaction_history_consumer = noncopyable_function<future<>(const db::compaction_history_entry&)>;
    future<> get_compaction_history(compaction_history_consumer&& f);

    // Submit a table to be compacted.
    void submit(compaction::compaction_group_view& t);

    // Can regular compaction be performed in the given table
    bool can_perform_regular_compaction(compaction::compaction_group_view& t);

    // Maybe wait before adding more sstables
    // if there are too many sstables.
    future<> maybe_wait_for_sstable_count_reduction(compaction::compaction_group_view& t);

    // Submit a table to be off-strategy compacted.
    // Returns true iff off-strategy compaction was required and performed.
    future<bool> perform_offstrategy(compaction::compaction_group_view& t, tasks::task_info info);

    // Submit a table to be cleaned up and wait for its termination.
    //
    // Performs a cleanup on each sstable of the table, excluding
    // those ones that are irrelevant to this node or being compacted.
    // Cleanup is about discarding keys that are no longer relevant for a
    // given sstable, e.g. after node loses part of its token range because
    // of a newly added node.
    future<> perform_cleanup(owned_ranges_ptr sorted_owned_ranges, compaction::compaction_group_view& t, tasks::task_info info);
private:
    future<> try_perform_cleanup(owned_ranges_ptr sorted_owned_ranges, compaction::compaction_group_view& t, tasks::task_info info);

    // Add sst to or remove it from the respective compaction_state.sstables_requiring_cleanup set.
    bool update_sstable_cleanup_state(compaction_group_view& t, const sstables::shared_sstable& sst, const dht::token_range_vector& sorted_owned_ranges);

    future<> on_compaction_completion(compaction_group_view& t, compaction_completion_desc desc, sstables::offstrategy offstrategy);
public:
    // Submit a table to be upgraded and wait for its termination.
    future<> perform_sstable_upgrade(owned_ranges_ptr sorted_owned_ranges, compaction::compaction_group_view& t, bool exclude_current_version, tasks::task_info info);

    // Submit a table to be scrubbed and wait for its termination.
    future<compaction_stats_opt> perform_sstable_scrub(compaction::compaction_group_view& t, compaction_type_options::scrub opts, tasks::task_info info);

    // Picks sstables from view `t` that match `filter` and rewrites the given
    // component using `modifier`.  The input sstables are captured from
    // `t.main_sstable_set()` while compaction is disabled on `t`, and
    // registered in `_compacting_sstables` before re-enabling.  This
    // guarantees that:
    //   1) every input sstable belongs to `t`'s compaction group at capture
    //      time (no cross-CG routing surprises), and
    //   2) no other operation can move the inputs to a different compaction
    //      group while the rewrite is in flight.
    //
    // The caller is responsible for iterating storage_group -> compaction_group
    // -> view to cover the desired range; the filter is applied per view.
    //
    // Returns a map from each old sstable to its rewritten replacement.
    future<std::unordered_map<sstables::shared_sstable, sstables::shared_sstable>> perform_component_rewrite(
            compaction::compaction_group_view& t,
            tasks::task_info info,
            std::function<bool(const sstables::shared_sstable&)> filter,
            sstables::component_type component,
            std::function<void(sstables::sstable&)> modifier,
            sstables::update_sstable_id update_id = sstables::update_sstable_id::yes);

    // Submit a table for major compaction.
    future<> perform_major_compaction(compaction::compaction_group_view& t, tasks::task_info info, bool consider_only_existing_data = false);

    // Splits a compaction group by segregating all its sstable according to the classifier[1].
    // [1]: See compaction_type_options::splitting::classifier.
    // Returns when all sstables in the main sstable set are split. The only exception is shutdown
    // or user aborted splitting using stop API.
    future<compaction_stats_opt> perform_split_compaction(compaction::compaction_group_view& t, compaction_type_options::split opt, tasks::task_info info);

    // Splits a single SSTable by segregating all its data according to the classifier.
    // If SSTable doesn't need split, the same input SSTable is returned as output.
    // If SSTable needs split, then output SSTables are returned and the input SSTable is deleted.
    // Exception is thrown if the input sstable cannot be split due to e.g. out of space prevention.
    future<std::vector<sstables::shared_sstable>> maybe_split_new_sstable(sstables::shared_sstable sst, compaction_group_view& t, compaction_type_options::split opt);

    // Run a custom job for a given table, defined by a function
    // it completes when future returned by job is ready or returns immediately
    // if manager was asked to stop.
    //
    // parameter type is the compaction type the operation can most closely be
    //      associated with, use compaction_type::Compaction, if none apply.
    // parameter job is a function that will carry the operation
    future<> run_custom_job(compaction::compaction_group_view& s, compaction_type type, const char *desc, noncopyable_function<future<>(compaction_data&, compaction_progress_monitor&)> job, tasks::task_info info, throw_if_stopping do_throw_if_stopping);

    // Disable compaction temporarily for a table t.
    // Caller should call the compaction_reenabler::reenable
    future<compaction_reenabler> stop_and_disable_compaction(sstring reason, compaction::compaction_group_view& t);

    future<compaction_reenabler> await_and_disable_compaction(compaction::compaction_group_view& t);

    future<seastar::rwlock::holder> get_incremental_repair_read_lock(compaction::compaction_group_view& t, const sstring& reason);
    future<seastar::rwlock::holder> get_incremental_repair_write_lock(compaction::compaction_group_view& t, const sstring& reason);

    // Run a function with compaction temporarily disabled for a table T.
    future<> run_with_compaction_disabled(compaction::compaction_group_view& t, std::function<future<> ()> func, sstring reason = "custom operation");

    void plug_system_keyspace(db::system_keyspace& sys_ks) noexcept;
    future<> unplug_system_keyspace() noexcept;

    // Adds a table to the compaction manager.
    // Creates a compaction_state structure that can be used for submitting
    // compaction jobs of all types.
    void add(compaction::compaction_group_view& t);
    // Adds a group with compaction temporarily disabled. Compaction is only enabled back
    // when the compaction_reenabler returned is destroyed.
    compaction_reenabler add_with_compaction_disabled(compaction::compaction_group_view& view);

    // Remove a table from the compaction manager.
    // Cancel requests on table and wait for possible ongoing compactions.
    future<> remove(compaction::compaction_group_view& t, sstring reason = "table removal") noexcept;

    const stats& get_stats() const {
        return _stats;
    }

    const std::vector<compaction_info> get_compactions(std::function<bool(const compaction_group_view*)> filter = [] (auto) { return true; }) const;

    // Returns true if table has an ongoing compaction, running on its behalf
    bool has_table_ongoing_compaction(const compaction::compaction_group_view& t) const;

    bool compaction_disabled(compaction::compaction_group_view& t) const;

private:
    std::vector<shared_ptr<compaction_task_executor>>
    do_stop_ongoing_compactions(sstring reason, std::function<bool(const compaction_group_view*)> filter, std::optional<compaction_type_set> types_opt) noexcept;

public:
    // Stops ongoing compactions of the given types, on the compaction group views selected by filter.
    // Never fails: any error encountered while stopping is logged and swallowed, so
    // callers may rely on the returned future resolving successfully. In particular it
    // is safe to co_await it while holding a future that must still be awaited
    // afterwards (see compaction_group::stop()).
    // A disengaged types_opt means: stop compactions of any type.
    // An empty types_opt (engaged optional containing empty enum set) means: don't stop any compaction.
    future<> stop_ongoing_compactions(sstring reason, std::optional<compaction_type_set> types_opt = {}, std::function<bool(const compaction_group_view*)> filter = [] (auto) { return true; }) noexcept;
    future<> stop_ongoing_compactions(sstring reason, const compaction_group_view* v, std::optional<compaction_type_set> types_opt = {}) noexcept {
        return stop_ongoing_compactions(std::move(reason), std::move(types_opt), [v] (const compaction_group_view* x) { return !v || x == v; });
    }

    future<> await_ongoing_compactions(compaction_group_view* t);

    compaction_reenabler stop_and_disable_compaction_no_wait(compaction_group_view& t, sstring reason);

    double backlog() {
        return _backlog_manager.backlog();
    }

    void register_backlog_tracker(compaction_backlog_tracker& backlog_tracker) {
        _backlog_manager.register_backlog_tracker(backlog_tracker);
    }

    compaction_backlog_tracker& get_backlog_tracker(compaction::compaction_group_view& t);

    static compaction_data create_compaction_data();

    compaction::strategy_control& get_strategy_control() const noexcept;

    shared_tombstone_gc_state& get_shared_tombstone_gc_state() noexcept {
        return _shared_tombstone_gc_state;
    };

    const shared_tombstone_gc_state& get_shared_tombstone_gc_state() const noexcept {
        return _shared_tombstone_gc_state;
    };

    // Uncoditionally erase sst from `sstables_requiring_cleanup`
    // Returns true iff sst was found and erased.
    bool erase_sstable_cleanup_state(compaction_group_view& t, const sstables::shared_sstable& sst);

    // checks if the sstable is in the respective compaction_state.sstables_requiring_cleanup set.
    bool requires_cleanup(compaction_group_view& t, const sstables::shared_sstable& sst) const;
    const std::unordered_set<sstables::shared_sstable>& sstables_requiring_cleanup(compaction_group_view& t) const;

    friend class compacting_sstable_registration;
    friend class compaction_weight_registration;
    friend class sstables::test_env_compaction_manager;

    friend class compaction::compaction_task_impl;
    friend class compaction::compaction_task_executor;
    friend class compaction::sstables_task_executor;
    friend class compaction::major_compaction_task_executor;
    friend class compaction::split_compaction_task_executor;
    friend class compaction::custom_compaction_task_executor;
    friend class compaction::regular_compaction_task_executor;
    friend class compaction::offstrategy_compaction_task_executor;
    friend class compaction::rewrite_sstables_compaction_task_executor;
    friend class compaction::rewrite_sstables_component_compaction_task_executor;
    friend class compaction::cleanup_sstables_compaction_task_executor;
    friend class compaction::validate_sstables_compaction_task_executor;
    friend compaction_reenabler;
};

class compaction_task_executor
    : public enable_shared_from_this<compaction_task_executor>
    , public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
public:
    enum class state {
        none,       // initial and final state
        pending,    // task is blocked on a lock, may alternate with active
                    // counted in compaction_manager::stats::pending_tasks
        active,     // task initiated active compaction, may alternate with pending
                    // counted in compaction_manager::stats::active_tasks
        done,       // task completed successfully (may transition only to state::none, or
                    // state::pending for regular compaction)
                    // counted in compaction_manager::stats::completed_tasks
        postponed,  // task was postponed (may transition only to state::none)
                    // represented by the postponed_compactions metric
        failed,     // task failed (may transition only to state::none)
                    // counted in compaction_manager::stats::errors
    };
protected:
    compaction_manager& _cm;
    ::compaction::compaction_group_view* _compacting_table = nullptr;
    compaction::compaction_state& _compaction_state;
    compaction_data _compaction_data;
    state _state = state::none;
    throw_if_stopping _do_throw_if_stopping;
    compaction_progress_monitor _progress_monitor;

private:
    shared_future<compaction_manager::compaction_stats_opt> _compaction_done = make_ready_future<compaction_manager::compaction_stats_opt>();
    exponential_backoff_retry _compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
    compaction_type _type;
    sstables::run_id _output_run_identifier;
    sstring _description;
    compaction_manager::compaction_stats_opt _stats = std::nullopt;

public:
    explicit compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, ::compaction::compaction_group_view* t, compaction_type type, sstring desc);

    compaction_task_executor(compaction_task_executor&&) = delete;
    compaction_task_executor(const compaction_task_executor&) = delete;

    virtual ~compaction_task_executor() = default;

    // called when a compaction replaces the exhausted sstables with the new set
    struct on_replacement {
        virtual ~on_replacement() {}
        // called after the replacement completes
        // @param sstables the old sstable which are replaced in this replacement
        virtual void on_removal(const std::vector<sstables::shared_sstable>& sstables) = 0;
        // called before the replacement happens
        // @param sstables the new sstables to be added to the table's sstable set
        virtual void on_addition(const std::vector<sstables::shared_sstable>& sstables) = 0;
    };

protected:
    future<> perform();

    virtual future<compaction_manager::compaction_stats_opt> do_run() = 0;

    state switch_state(state new_state);

    future<semaphore_units<named_semaphore_exception_factory>> acquire_semaphore(named_semaphore& sem, size_t units = 1);

    // Return true if the task isn't stopped
    // and the compaction manager allows proceeding.
    inline bool can_proceed(throw_if_stopping do_throw_if_stopping = throw_if_stopping::no) const;
    void setup_new_compaction(sstables::run_id output_run_id = sstables::run_id::create_null_id());
    void finish_compaction(state finish_state = state::done) noexcept;

    // Compaction manager stop itself if it finds an storage I/O error which results in
    // stop of transportation services. It cannot make progress anyway.
    // Returns exception if error is judged fatal, and compaction task must be stopped,
    // otherwise, returns stop_iteration::no after sleep for exponential retry.
    future<stop_iteration> maybe_retry(std::exception_ptr err, bool throw_on_abort = false);

    future<compaction_result> compact_sstables_and_update_history(compaction_descriptor descriptor, compaction_data& cdata, on_replacement&,
                                compaction_manager::can_purge_tombstones can_purge = compaction_manager::can_purge_tombstones::yes);
    future<compaction_result> compact_sstables(compaction_descriptor descriptor, compaction_data& cdata, on_replacement&,
                                compaction_manager::can_purge_tombstones can_purge = compaction_manager::can_purge_tombstones::yes,
                                sstables::offstrategy offstrategy = sstables::offstrategy::no);
    future<> update_history(::compaction::compaction_group_view& t, compaction_result&& res, const compaction_data& cdata);
    bool should_update_history(compaction_type ct) {
        return ct == compaction_type::Compaction || ct == compaction_type::Major;
    }
public:
    compaction_manager::compaction_stats_opt get_stats() const noexcept {
        return _stats;
    }

    future<compaction_manager::compaction_stats_opt> run_compaction() noexcept;

    const ::compaction::compaction_group_view* compacting_table() const noexcept {
        return _compacting_table;
    }

    compaction_type compaction_type() const noexcept {
        return _type;
    }

    bool compaction_running() const noexcept {
        return _state == state::active;
    }

    const ::compaction::compaction_data& compaction_data() const noexcept {
        return _compaction_data;
    }

    ::compaction::compaction_data& compaction_data() noexcept {
        return _compaction_data;
    }

    bool generating_output_run() const noexcept {
        return compaction_running() && _output_run_identifier;
    }
    const sstables::run_id& output_run_id() const noexcept {
        return _output_run_identifier;
    }

    const sstring& description() const noexcept {
        return _description;
    }
private:
    // Before _compaction_done is set in compaction_task_executor::run_compaction(), compaction_done() returns ready future.
    future<compaction_manager::compaction_stats_opt> compaction_done() noexcept {
        return _compaction_done.get_future();
    }

    future<sstables::sstable_set> sstable_set_for_tombstone_gc(::compaction::compaction_group_view& t);
public:
    bool stopping() const noexcept {
        return _compaction_data.abort.abort_requested();
    }

    void abort(abort_source& as) noexcept;

    void stop_compaction(sstring reason) noexcept;

    compaction_stopped_exception make_compaction_stopped_exception() const;

    template<typename TaskExecutor, typename... Args>
    requires std::is_base_of_v<compaction_task_executor, TaskExecutor> &&
            std::is_base_of_v<compaction_task_impl, TaskExecutor> &&
    requires (compaction_manager& cm, throw_if_stopping do_throw_if_stopping, Args&&... args) {
        {TaskExecutor(cm, do_throw_if_stopping, std::forward<Args>(args)...)} -> std::same_as<TaskExecutor>;
    }
    friend future<compaction_manager::compaction_stats_opt> compaction_manager::perform_compaction(throw_if_stopping do_throw_if_stopping, tasks::task_info parent_info, Args&&... args);
    friend future<compaction_manager::compaction_stats_opt> compaction_manager::perform_task(shared_ptr<compaction_task_executor> task, throw_if_stopping do_throw_if_stopping);
    friend fmt::formatter<compaction_task_executor>;
    friend void compaction_manager::stop_tasks(const std::vector<shared_ptr<compaction_task_executor>>& tasks, sstring reason) noexcept;
    friend future<> compaction_manager::await_tasks(std::vector<shared_ptr<compaction_task_executor>>, bool task_stopped) const noexcept;
    friend sstables::test_env_compaction_manager;
};

}

template <>
struct fmt::formatter<compaction::compaction_task_executor::state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(compaction::compaction_task_executor::state c, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<compaction::compaction_task_executor> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const compaction::compaction_task_executor& ex, fmt::format_context& ctx) const  -> decltype(ctx.out());
};

namespace compaction {

bool needs_cleanup(const sstables::shared_sstable& sst, const dht::token_range_vector& owned_ranges);

// Return all sstables but those that are off-strategy like the ones in maintenance set and staging dir.
future<std::vector<sstables::shared_sstable>> in_strategy_sstables(compaction::compaction_group_view& table_s);

}
