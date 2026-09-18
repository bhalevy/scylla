# The Compaction Scheduler

This document describes how the compaction manager decides *how much* compaction
runs at once on a shard. It is about admission, not about resource shares: the
amount of CPU and I/O a running compaction is allowed to consume is the business
of the [compaction controller](compaction_controller.md), which is a separate
mechanism. The controller decides how fast compaction goes; the scheduler decides
how many compactions go at all.

## Motivation

Regular compaction used to have no admission control. `submit()` started a fiber
per compaction group, and the only thing bounding how many ran at once was the
weight tracker, which refuses a job whose input size class collides with another
running job of the same compaction group. Nothing weighed one compaction group
against another, and nothing capped the total. With tablets there are thousands of compaction groups per
shard, so the number of concurrent compactions was bounded only by how many
groups happened to have work to do.

Maintenance compaction was bounded, but coarsely and at the wrong granularity:
`_maintenance_ops_sem` allows one major, cleanup, upgrade, scrub or reshard/
reshape *task* at a time, and `_off_strategy_sem` one off-strategy task.

## Tasks and jobs

The distinction the scheduler is built around:

- A **task** is long lived. It selects the sstables it will work on, fences them
  (see *Fencing* below), and then produces one or more compaction jobs. A
  cleanup task produces one job per sstable, the rewrite family (upgrade, scrub)
  one per input sstable, and a regular compaction task one per selection.
- A **job** is one call into `compact_sstables()`: a set of input sstables
  merged into output sstables.

The job is what consumes disk and CPU, so the job is what a per-shard limit
bounds. Tasks stay concurrently resident; their jobs queue for a slot.

## Two layers of admission

### Task-level admission

Unchanged from before the scheduler, and still enforced where it always was:

- `_maintenance_ops_sem` (count 1) — at most one major, cleanup, rewrite,
  component-rewrite or custom (reshard/reshape) task per shard.
- `_off_strategy_sem` (count 1) — at most one off-strategy task per shard.
  It is a separate semaphore, so an off-strategy task can run alongside one
  other maintenance task.
- Regular compaction runs at most one task per compaction group. Intra-group
  parallelism would not buy disk parallelism, which is already saturated across
  shards, and it made the amount of compaction a group ran a function of how
  often it happened to be submitted — which is driven by flushes, not by
  anything about the compaction work itself.

### Job-level admission

Most compaction jobs take a slot from the scheduler for their duration --
regular, major, off-strategy, cleanup, and the rewrite family (upgrade, scrub,
split, component rewrite). Reshard, reshape and scrub in validate mode do not
yet, so they are not bounded by these limits:

- `_max_jobs` caps jobs of every class.
- `_max_maintenance_jobs` caps the maintenance ones alone.

Regular compaction has no sub-cap, so it uses every slot while no maintenance
job is running. A maintenance job waits for a slot to be freed rather than
preempting one. Expressing the reservation as two caps rather than as a rule
keeps the policy in one predicate, `can_start_job()`.

A freed slot is not handed to the longest waiter, which needs care. Maintenance
jobs wait in `acquire_job_slot()`, while regular compaction is dispatched by the
scheduler fiber, which takes a slot synchronously without ever entering that
wait. Left alone the fiber would win every freed slot under sustained regular
load, and a maintenance job would wait indefinitely -- while holding
`_maintenance_ops_sem`, and for major via `compact_all_sstables()` the repair
read lock as well. `should_defer_to_maintenance()` is what prevents that: the
fiber stops dispatching while a maintenance job is waiting for a slot, so the
next freed slot goes to the waiter. It stands down only when the overall limit
is what blocks the waiter; if the maintenance sub-limit is, a freed slot could
not go to it anyway, so regular compaction may as well use it.

Both are configured by `compaction_max_concurrent_jobs` and
`compaction_max_concurrent_maintenance_jobs`, live-updatable, defaulting to `2`
and `1`. The maintenance sub-limit is a ceiling, not a reservation: while no
maintenance job runs, regular compaction uses every slot. `0` means no limit,
and is converted to the internal unlimited value at the boundary so the
predicate stays a plain comparison.

## Dispatch

Regular and maintenance compaction reach a slot by different routes, because
they are started by different things.

A **maintenance** job is produced by a task that a caller is already awaiting, so
it simply waits: `acquire_job_slot()` blocks until `can_start_job()` holds.

A **regular** job has no such caller. `submit()` marks the group as having work
and wakes the scheduler fiber, which owns dispatch. Groups are held in intrusive
queues — with one compaction group per tablet, enqueue and dequeue have to be
O(1) and allocation-free — and a group is linked into at most one:

- `_ready_groups` — has work to do, waiting for a dispatch slot.
- `_deferred_groups` — its last job was refused by the weight tracker, or a
  major compaction is selecting on it. Spliced back into `_ready_groups` when a
  weight is released or the major finishes selecting.

The hook uses `auto_unlink`, so destroying a `compaction_state` — which happens
when its compaction group is removed — cannot leave a dangling queue entry.

A dispatched regular task performs **one job and returns**, queueing its group
again if it did work or if a submit arrived meanwhile -- except on the retry
path below, where it keeps its slot and runs further jobs under the same task. One task_manager task is
therefore one compaction job, and the scheduler gets to weigh this group against
the others before the group's next job starts. The task's internal loop survives
only for the `maybe_retry()` path, so a failing job's exponential backoff is not
reset on every retry. That path keeps the job slot across a backoff of up to 300
seconds, and does not recheck `major_compaction_pending`, so persistently failing
groups can hold every slot; another known gap.

## Fencing, and major compaction

Fencing is per task: a task registers its inputs in
`compaction_manager::_compacting_sstables`, and `get_candidates()` excludes
anything registered. This is what stops two compactions from selecting the same
sstables.

It also creates the one ordering constraint the scheduler has to enforce
directly. Major compaction must see *every* sstable of its group; if it selected
while a regular compaction was running, that job's inputs would be fenced,
`get_candidates()` would exclude them, and major would silently compact a
subset. This used to be a per-group `rwlock` — regular took the read lock and
held it through execution, major took the write lock and released it as soon as
it had selected — and is now a dispatch predicate:

- a requested major raises `major_compaction_pending`, which stops the scheduler
  dispatching regular jobs for that group;
- it then waits for `regular_compaction_dispatched` to clear;
- once it has selected and registered its inputs it lowers the flag, which is
  the point the write lock used to be released.

So the exclusion covers selection only: regular compaction runs in parallel with
major's *execution*, just not with its selection. Sstables flushed after major
selected are not fenced by it, and regular compaction is free to compact them.

`compaction_state::sstable_set_lock` is unrelated to any of this and remains: it
serializes a regular compaction's snapshot-filter-register sequence against the
split replacer's `on_compaction_completion()`, which mutates sstable sets from
outside the task loop.

## Lock ordering

**A job takes every lock it needs before asking for a slot, and never asks for a
slot while about to take one.**

seastar's `rwlock` is a semaphore where a reader takes one unit and a writer
takes all of them, and its wait queue is FIFO, so a waiting writer blocks readers
that queue behind it. Without the rule above:

- job A holds `compaction_state::incremental_repair_lock` for read and waits for
  a slot;
- a repair session asks for the write lock and queues behind A;
- job B holds the last slot and asks for the read lock, queueing behind that
  writer;
- B never frees its slot, so A never runs, so the writer never runs.

Today's structure satisfies the rule: the repair read lock and
`_maintenance_ops_sem` are both taken at task start, before any job exists.
Releasing a lock or a slot early is always safe; only acquiring one while holding
the other in the wrong order is not.

## Stopping

A regular compaction runs as a sequence of single-job tasks, so a stop request
can land between two jobs and find no task to stop.
`compaction_state::stop_generation` is bumped by
`do_stop_ongoing_compactions()` for every group matching the filter, whether or
not it has a task at that moment, and a dispatched job does not queue its group
again once the generation changes. The other stop paths — `remove()`, `drain()`,
`really_do_stop()` and `stop_and_disable_compaction()` — are covered by the gate
or by `can_proceed()`, and the generation covers them uniformly.

## Metrics

- `jobs_running`, `maintenance_jobs_running` — jobs in flight.
- `jobs_waiting` — jobs blocked in `acquire_job_slot()`. These are the
  maintenance ones; regular compaction is held back at dispatch instead.
- `groups_ready` — compaction groups with regular work the scheduler has not
  dispatched, because the job limit is reached or a major is selecting. This is
  where a limit on regular compaction shows up.
- `postponed_compactions` — groups deferred by the weight tracker.

## Relationship to the weight tracker

The weight tracker predates the scheduler and still gates every regular
compaction job, in `can_register_compaction()`. All three of its rules are
evaluated per compaction group -- `compaction_state::weight_tracker` holds the
weights of that group's running jobs, and the fan-in threshold is taken over
that group's tasks. A job's weight is a function of its input size under the
group's compaction strategy and its fan-in is a property of that strategy, so
neither is comparable across groups: the weight of a TWCS job says nothing
about whether an ICS job elsewhere is similarly sized. It applies three rules,
in order:

1. If the strategy does not support parallel compaction — only LCS — refuse
   while the group has any compaction running.
2. A job's **weight** is `log4(total input size + 1MB)`, and only one job per
   weight class may run at a time within the group. Because the classes are logarithmic, jobs of
   *similar* size are serialized while jobs of *different* size are not: this is
   what lets a small, quick compaction proceed alongside a large, slow one
   instead of queueing behind it. A job made up solely of fully expired sstables
   gets weight 0 and is exempt, being cheap and worth doing immediately.
3. A job's **fan-in** must be at least the largest fan-in among the group's
   running compactions, capped at 32. This refuses a *less* efficient job while a more
   efficient one runs, so that a low-fan-in compaction does not dilute the
   write amplification a high-fan-in one is achieving.

A refused job leaves its group in `_deferred_groups`. It comes back when any
task finishes, when a submit arrives for that group, or on the periodic
submission.

The tracker is kept, and is how regular compaction is bucketed. The three
rules now sit alongside the job limits rather than being replaced by them:

- Rule 1 is subsumed. Regular compaction is serialized per compaction group for
  every strategy, not just LCS, so a group never has a second job for this rule
  to refuse.
- Rule 2 is the bucketing. One regular job per weight class per compaction
  group, underneath the overall job limit. The two bound different things: the
  shard-wide limit bounds how much compaction runs at once, while the bucket
  keeps a single group from spending several of those slots on jobs of the same
  size. Since regular compaction is already serialized per group, the bucket
  only has an effect once that serialization is relaxed; it is kept because it
  is what makes the rule meaningful -- weights are only comparable among jobs
  of one strategy.
- Rule 3 is what the job limit cannot express. A flat limit does not distinguish
  a large slow job from a small quick one, so a multi-hour compaction can hold a
  slot while small-tier work waits and read amplification climbs. The fan-in
  check refuses a job that would dilute the write amplification a
  higher-fan-in one is achieving, and there is nothing else doing that.

Maintenance jobs are deliberately not bucketed. They have their own reservation
through the maintenance job limit, and their sizes are not chosen by a strategy
weighing tiers against each other, so a size class carries no useful meaning for
them.

The rules are evaluated after selection, in can_register_compaction(), because
weight and fan-in are properties of a descriptor that does not exist until the
job has been selected -- which is after it was admitted. A refused job parks its
group in _deferred_groups and comes back when a weight is released. That is why
a regular job's slot is taken at dispatch and the bucket is checked later: the
two admissions answer different questions at different points.
