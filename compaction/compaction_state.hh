/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/condition-variable.hh>
#include "seastarx.hh"

#include <boost/intrusive/list.hpp>

#include <memory>
#include <unordered_set>

#include "compaction/compaction_fwd.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "sstables/shared_sstable.hh"
#include "gc_clock.hh"

namespace compaction {

// There's 1:1 relationship between compaction_grop_view and compaction_state.
// Two or more compaction_group_view can be served by the same instance of sstable::sstable_set,
// so it's not safe to track any sstable state here.
//
// Locking and exclusion
// =====================
//
// Concurrency for compaction selection and sstable set mutation is governed by
// the compaction manager's scheduler, plus one lock:
//
// 1) Scheduler exclusion (major_compaction_pending, regular_compaction_dispatched)
//
//    Major compaction must see every sstable of the group, so it may not select
//    while a regular compaction is running: a running regular compaction has its
//    inputs registered in _compacting_sstables, get_candidates() excludes them,
//    and major would silently compact a subset.
//
//    A requested major raises major_compaction_pending, which stops the
//    scheduler from dispatching further regular jobs for the group, and then
//    waits for regular_compaction_dispatched to clear. Once it has selected and
//    registered its inputs it lowers the flag again, so regular compaction runs
//    in parallel with major's execution -- exclusivity covers selection, not the
//    whole job.
//
// 2) sstable_set_lock (semaphore, count=1)
//
//    Protects the atomicity of:
//      (a) regular compaction's: snapshot capture + eligibility filter + registration
//      (b) split/rewrite replacer's: on_compaction_completion (which mutates the
//          sstable set and deregisters old sstables from _compacting_sstables)
//
//    Without this lock, the following race is possible:
//      - Regular picker captures a snapshot of main_sstables (contains X).
//      - Split's replacer fires: moves X out of main_sstables AND deregisters
//        X from _compacting_sstables.
//      - Regular picker's filter runs on the stale snapshot, finds X eligible
//        (since it was deregistered), and selects it.
//      - Regular compaction completes, tries to remove X from main_sstables
//        where it no longer exists -> on_internal_error.
//
//    The sstable_set_lock ensures that (a) and (b) do not interleave.
//
struct compaction_state {
    // The compaction group this state belongs to. compaction_manager owns the
    // 1:1 mapping between the two, so a state linked into one of the
    // scheduler's queues can be resolved back to the group its jobs run on.
    compaction_group_view& view;

    // Hook for the compaction manager's scheduler queues. A state is linked
    // into at most one queue at a time. The link mode is auto_unlink so that
    // destroying a state -- which happens when its compaction group is removed
    // -- cannot leave a dangling entry behind in a queue.
    using queue_hook_type = boost::intrusive::list_member_hook<
            boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    queue_hook_type queue_hook;

    // Used both by compaction tasks that refer to the compaction_state
    // and by any function running under run_with_compaction_disabled().
    seastar::named_gate gate;

    // Protects the view's sstable set ownership. Serializes sstable set reads
    // (snapshot + filter + registration) against sstable set mutations
    // (on_compaction_completion which moves sstables between groups/views).
    seastar::semaphore sstable_set_lock{1};

    // Compations like major need to work on all sstables in the unrepaired
    // set, no matter if the sstable is being repaired or not. The
    // incremental_repair_lock lock is introduced to serialize repair and such
    // compactions. This lock guarantees that no sstables are being repaired.
    // Note that the minor compactions do not need to take this lock because
    // they ignore sstables that are being repaired.
    seastar::rwlock incremental_repair_lock;

    // Weights of the compaction jobs currently running for this group, used to
    // keep two similarly-sized jobs from running against it at the same time.
    // Bucketing is per group, not per shard, because a job's weight is a
    // function of its input size under the group's compaction strategy, so
    // weights from groups using different strategies are not comparable.
    std::unordered_set<int> weight_tracker;

    // Raised by any function running under run_with_compaction_disabled();
    long compaction_disabled_counter = 0;

    // Set while a regular compaction job dispatched by the scheduler
    // (compaction_manager::dispatch_regular_compaction) is in flight for this
    // group, so that a submit arriving meanwhile only marks the group for
    // requeueing instead of starting a second job. Regular compaction of a
    // group is serialized: intra-group parallelism would not buy disk
    // parallelism, which is already saturated across shards.
    bool regular_compaction_dispatched = false;

    // Set while the group is parked in the scheduler's deferred queue, so that a
    // submit can tell it apart from a group already queued as ready and promote
    // it back. Without that, a submit finds the group linked, does nothing, and
    // the group waits for something else to release a compaction weight.
    bool regular_compaction_deferred = false;

    // Raised while a major compaction for this group is waiting to select its
    // inputs, and lowered once it has selected and registered them. While it is
    // raised the scheduler does not dispatch regular compaction for the group,
    // so that major is not starved by a group that keeps finding work.
    bool major_compaction_pending = false;

    // Bumped whenever the group is submitted for regular compaction. A job
    // samples it when it starts and compares when it ends, so a submit that
    // arrived while it ran queues the group again and makes it select once more,
    // rather than being missed because the job was already in flight.
    uint64_t regular_compaction_submissions = 0;

    // Bumped whenever ongoing regular compactions are stopped for this group.
    uint64_t stop_generation = 0;

    // Signaled whenever a compaction task completes.
    condition_variable compaction_done;

    // Cleanup tracking is used only with vnodes (never with tablets) and only
    // while a cleanup is in progress. To avoid paying for it on every
    // compaction_state -- there is one per compaction group, and they are
    // long-lived -- it is held behind a lazily-allocated pointer that stays
    // null until the first sstable is marked as requiring cleanup, and is
    // released again once the set becomes empty.
    //
    // sizeof(cleanup_state) is ~64 bytes (an unordered_set plus an
    // owned_ranges_ptr), so this keeps that out of the common case where no
    // cleanup is running, shrinking compaction_state from 344 to 288 bytes.
    struct cleanup_state {
        // Set of sstables that still require cleanup.
        std::unordered_set<sstables::shared_sstable> sstables_requiring_cleanup;
        // Owned token ranges to keep while cleaning the above sstables.
        compaction::owned_ranges_ptr owned_ranges_ptr;
    };
private:
    std::unique_ptr<cleanup_state> _cleanup_state;

public:
    // Read-only view of the sstables requiring cleanup; empty when no cleanup
    // is in progress.
    //
    // Lifetime: the returned reference aliases the lazily-allocated
    // cleanup_state and is only valid until the next cleanup-state mutation.
    // Erasing the last sstable (erase_sstable_requiring_cleanup) frees the
    // cleanup_state, so a reference held across such a mutation dangles. All
    // current callers consume it synchronously (iterate or copy out) before any
    // mutation; do not retain it.
    //
    // Note: the accessors that construct/destroy the cleanup_state (and its
    // unordered_set / owned_ranges_ptr) are defined out-of-line in
    // compaction_manager.cc, next to the destructor, so that including this
    // header does not require the complete sstable/dht::token types.
    const std::unordered_set<sstables::shared_sstable>& sstables_requiring_cleanup() const;

    // Whether any sstable currently requires cleanup.
    bool has_sstables_requiring_cleanup() const {
        return _cleanup_state && !_cleanup_state->sstables_requiring_cleanup.empty();
    }

    // Whether the given sstable currently requires cleanup.
    bool requires_cleanup(const sstables::shared_sstable& sst) const;

    // The owned ranges associated with the in-progress cleanup, or null when no
    // cleanup is in progress.
    //
    // Returned by value (a cheap lw_shared_ptr refcount bump) rather than by
    // reference: the underlying owned_ranges_ptr lives in the lazily-allocated
    // cleanup_state, which is freed when the cleanup set drains. Handing out a
    // copy keeps the ranges alive for the caller independently of that
    // lifetime, so it cannot dangle.
    compaction::owned_ranges_ptr cleanup_owned_ranges() const;

    // Mark an sstable as requiring cleanup, allocating the cleanup state on
    // demand.
    void insert_sstable_requiring_cleanup(const sstables::shared_sstable& sst);

    // Remove an sstable from the cleanup set if present, returning whether it
    // was present. Releases the cleanup state (including the owned ranges) once
    // the set becomes empty.
    bool erase_sstable_requiring_cleanup(const sstables::shared_sstable& sst);

    // Record the owned ranges to retain while cleaning up. Precondition:
    // has_sstables_requiring_cleanup() -- the ranges are released together with
    // the cleanup state once the sstable set drains, so they must not be set
    // without sstables to drain them.
    void set_cleanup_owned_ranges(compaction::owned_ranges_ptr ranges);

    gc_clock::time_point last_regular_compaction;

    explicit compaction_state(compaction_group_view& t);
    compaction_state(compaction_state&&) = delete;
    ~compaction_state();

    bool compaction_disabled() const noexcept {
        return compaction_disabled_counter > 0;
    }
};

} // namespace compaction
