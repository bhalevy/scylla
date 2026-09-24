# Multi-dimensional tablet load balancing

This document describes the planned evolution of the tablet load balancer from a
single-dimensional (disk utilization) balancer into a multi-dimensional one, and the
related change of the tablet sizing algorithm from a statically configured target tablet
size to a target derived from the disk capacity managed by a shard.

It complements, and is intended to eventually supersede parts of,
[size-based-load-balancing.md](size-based-load-balancing.md).

## Table of contents

1. [Motivation](#motivation)
2. [Goals](#goals)
3. [Background: what the balancer does today](#background-what-the-balancer-does-today)
4. [The load vector](#the-load-vector)
5. [Combining dimensions: the load potential](#combining-dimensions-the-load-potential)
6. [Per-tablet workload metric](#per-tablet-workload-metric)
7. [Capacity-relative tablet sizing](#capacity-relative-tablet-sizing)
8. [Choosing which tables to merge](#choosing-which-tables-to-merge)
9. [Stability, hysteresis and convergence](#stability-hysteresis-and-convergence)
10. [Observability and cluster scaling signals](#observability-and-cluster-scaling-signals)
11. [Implementation plan](#implementation-plan)
12. [Related work](#related-work)

## Motivation

The current balancer equalizes a single scalar per shard: storage utilization
(`used_bytes / capacity_bytes`), with a secondary pass that equalizes each table's share of
a shard. Two problems follow from this.

First, **disk is not the only contended resource**. A shard that holds an average amount of
data may still be the bottleneck if the tablets it holds happen to be the hot ones. We have
no per-tablet workload signal at all today, so the balancer cannot see this.

Second, **the target tablet size is a static configuration value**
(`target_tablet_size_in_bytes`, 5 GiB by default) and therefore unrelated to how much disk a
shard actually manages. On a node with small shards, 5 GiB tablets are far too coarse to
balance with; on a node with very large shards, the resulting tablet count per shard blows
past `tablets_per_shard_goal` and the balancer is forced to scale tablet counts down again
through an unrelated code path. The two configured goals — target tablet size and
tablets-per-shard goal — fight each other, and which one wins depends on the hardware.

## Goals

The balancer should equalize, across the shards of a rack:

1. **Disk footprint** — bytes per shard while shards are far from full, and bytes used /
   effective capacity as they approach full. (Today's only goal is the latter, at all
   utilization levels.)
2. **CPU / workload** — a per-tablet activity rate, normalized per shard.
3. **Tablet count** — so that no shard carries a disproportionate number of tablets,
   independently of their size or heat (tablet count drives per-shard fixed overheads:
   memtables, sstable sets, compaction groups, reader-concurrency slots).
4. **Per-table mix** — each table's share of a shard should be proportional to that shard's
   share of the rack, so that a single hot table cannot concentrate on a few shards.

and should do so while:

5. **Minimizing data movement** — migrations cost I/O, network and double-quorum latency.
6. **Converging** — the sequence of incremental plans must terminate and must not oscillate.

Separately, the sizing algorithm should:

7. Derive the target tablet size from the **capacity managed by a shard**, so that the
   per-shard tablet count goal is the primary control variable and the tablet size follows.
8. Keep the per-shard tablet count bounded by a budget, and let it float between the goal
   and the budget in steady state, so that the **per-shard tablet count and the average
   tablet size together become a meaningful external signal** for scaling the cluster out
   or in.
9. Not over-fragment an under-utilized cluster: at low utilization it is fine to stay
   *below* the per-shard tablet count goal rather than create a large number of tiny tablets.

## Background: what the balancer does today

Relevant code:

- `service/tablet_allocator.cc` — `class load_balancer`. `make_plan()` produces an
  incremental migration plan; `make_sizing_plan()` / `make_resize_plan()` produce split and
  merge decisions.
- `locator/load_sketch.hh` — `class load_sketch` tracks per-node and per-shard
  `disk_usage {capacity, used}` and tablet counts, with a `_shards_by_load` btree ordered by
  `used / capacity`.
- `locator/tablets.hh` — `struct load_stats` / `struct tablet_load_stats`, the information
  collected from all nodes by the topology coordinator every
  `tablet_load_stats_refresh_interval_in_seconds` (60s).
- `replica/table.cc` — `tablet_storage_group_manager::table_load_stats()` produces the
  per-tablet sizes on each node.

Load is a single `double` (`load_sketch::load_type`). Node-level balancing runs first
(cheaper to get right, fewer moves), then per-node shard balancing. `migration_badness`
compares candidate moves. A DC is considered balanced when
`(max_load - min_load) / max_load < size_based_balance_threshold_percentage / 100`.

Sizing today, per table (`compute_target_tablet_count()`), takes the max over several
candidate tablet counts (`initial`, `min_tablet_count`, `expected_data_size_in_gb`,
`min_per_shard_tablet_count`, and a count derived from `avg_tablet_size` vs
`target_tablet_size`), then a global pass scales all tables down uniformly if the projected
per-shard tablet replica count in any rack exceeds `tablets_per_shard_goal`.

## The load vector

Replace the scalar `load_type` with a fixed-layout vector. For a shard `s`:

```
L(s) = [ size(s), full(s), work(s), count(s), share(s, t0), share(s, t1), ... ]
```

where each component is normalized so that a perfectly balanced rack has every component
of every shard equal to 1, or, for `full`, to the same value on every shard:

| Dimension | Numerator | Denominator |
|---|---|---|
| `size` | bytes used on `s` | `rack_bytes / shards(R)` |
| `full` | bytes used on `s` | `effective_capacity(s) * u_eq` |
| `work` | workload rate attributed to tablets on `s` | `rack_workload / shards(R)` |
| `count` | tablet replicas on `s` | `rack_tablet_count / shards(R)` |
| `share(s,t)` | bytes of table `t` on `s` | `rack_bytes(t) / shards(R)` |

The fair share of every dimension except `full` is **per shard**, not proportional to the
shard's disk capacity. Request processing, memtables, compaction and reader concurrency are
per-shard resources, and the data a shard holds is what brings requests to it, so a shard
with less free disk is not a shard that should serve less. Normalizing `work`, `count` and
`share` by the shard's share of the rack's effective capacity, as `disk` is normalized today,
would have exactly the failure described in SCYLLADB-3446: effective capacity (tablet data
plus free space) also shrinks with disk usage which is not tablet data, like snapshots and
commitlog, and varies a lot between nodes. In the production cluster from that issue, the
effective capacity of the 5 nodes of a rack ranged from 1.29 TiB to 1.68 TiB; one node was
28% above the rack average in effective utilization while only 6% above it in bytes.
Balancing effective utilization would have moved tablets, and with them requests, hard away
from that node, overloading the CPU of the others.

The `full` dimension is the one which cares about capacity, and it only matters as a shard
approaches full. It is absolute utilization, `used / effective_capacity`, scaled by `1 / u_eq`
and not normalized to the rack average. With the exponential potential below, its weight
relative to the `size` dimension is `exp(lambda * (u / u_eq - 1))`, which is negligible at
low utilization (about 2% at `u = 0.4` for `lambda = 8`, `u_eq = 0.8`) and equal to that of
`size` at `u = u_eq`. So the balancer equalizes bytes per shard while there is room, and
equalizes fullness as disks fill up, which also covers heterogeneous nodes whose disk per
shard differs. This is the hybrid metric SCYLLADB-3446 proposes for today's balancer,
`effective_weight = clamp((max_effective_utilization + 0.1)^8, 0, 1)`, with the crossover at
80% utilization; here it falls out of the potential instead of being a separately tuned
weight, and `u_eq = 0.8` gives the same crossover.

Note that goal 4 (per-table mix) needs no separate machinery: it is simply one extra
dimension per table. The dimension count `d` becomes `4 + number_of_tables`, which can be in
the thousands. This is fine as long as:

- The potential (below) is maintained **incrementally**. A single tablet migration touches
  exactly five components (`size`, `full`, `work`, `count`, and the one `share` of its
  table), so the delta evaluation is `O(1)`, not `O(d)`.
- The per-shard vector is stored sparsely for the `share` dimensions — a shard only has
  entries for tables it actually holds. `shard_load` in `load_balancer` already keeps
  `tablet_sizes_per_table` and `tablet_count_per_table` maps for exactly this purpose.

## Combining dimensions: the load potential

The obvious combination, a weighted sum `w1*disk + w2*work + ...`, is a poor choice: slack in
one dimension masks saturation in another, and the weights are unit-dependent and must be
retuned per workload and per hardware generation.

Instead we use an **exponential potential**:

```
Phi = sum over shards s, dimensions i of  exp(lambda * L_i(s))
```

and treat a candidate migration as good if it decreases `Phi`. This is the standard online
vector-scheduling algorithm and is `O(log d)`-competitive for makespan across all dimensions
(see [Related work](#related-work)). Its practical properties are what we want:

- **No weights to tune.** The only parameter is `lambda`, which sets how sharply the
  balancer prioritizes the most-loaded dimension. `lambda -> 0` degenerates to balancing the
  sum of all dimensions; `lambda -> infinity` degenerates to balancing only the single worst
  `L_i(s)`. A value around `lambda = 8` makes a shard at 1.25x fair share contribute ~7x the
  penalty of a shard at 1.0x, which is a reasonable "fix the hot spot first, but don't ignore
  the rest" trade-off.
- **Scale-free**, because every `L_i` is already a normalized fraction.
- **Smooth gradient**, unlike `max_i L_i(s)`, so local search can always tell which of two
  similar moves is better — this is what `migration_badness` needs.
- **Monotone descent gives convergence for free.** If we only accept moves with
  `delta_Phi < 0` and the set of reachable states is finite, the algorithm terminates. The
  current `(max - min) / max < threshold` stop condition gives no such guarantee.

`exp()` on the hot path is avoidable: `L_i` is bounded in practice, and a 256-entry lookup
table over `[0, 2]` with linear interpolation is accurate enough for ranking candidate moves.

### Movement cost

A migration of `B` bytes is only accepted if

```
delta_Phi + beta * B / rack_effective_capacity < 0
```

`beta` converts bytes moved into potential units. It is the single knob that trades
convergence speed against churn, and it replaces the current
`size_based_balance_threshold_percentage` deadband with something that composes across
dimensions. The deadband is kept as a coarse early-out.

### Two-level structure

The existing node-then-shard decomposition is retained, with a larger `beta` at the
inter-node level (cross-node migration streams data; intra-node migration on the same node is
much cheaper). The same `Phi` is evaluated over node-level aggregates for step one and over
shard-level vectors for step two.

## Per-tablet workload metric

We have no per-tablet activity signal today. The proposed metric is deliberately simple.

### What to measure

**Bytes touched per second, per tablet**, as the sum of:

- *write bytes*: the memory footprint of each mutation applied to the tablet
  (`table::do_apply()` already has the `compaction_group`).
- *read bytes*: the memory footprint of the result a read produces, recorded by
  `table::query()` and `table::mutation_query()` against the storage group owning the read's
  partition range.

Bytes rather than operation counts, because operation cost varies by orders of magnitude
between a point lookup and a wide-partition scan, whereas bytes correlate reasonably with
both CPU and I/O.

Counting at the *reader* level rather than at the query level would be more faithful - it
would charge a scan that reads 1 GiB and returns one row for the 1 GiB it actually read - but
it must not be done by wrapping the reader returned by `table::make_mutation_reader()`. That
function documents an assumption that cache and memtables are read atomically for single-key
queries; a wrapping reader whose `fill_buffer()` introduces a deferring point breaks it, and
the tombstone-GC-overlap tests in `test/boost/row_cache_test.cc` catch this. Any future
per-fragment accounting has to happen inside an existing reader rather than around it.

Crucially this metric is **placement-invariant**: it measures the work a tablet demands, not
the CPU the shard happened to spend. Measured shard CPU would be a feedback signal — moving
a tablet changes it — and balancing directly on it invites oscillation. Bytes/s is an open-loop
proxy that a migration transports unchanged along with the tablet.

### How to smooth it

An exponentially weighted moving average with a half-life of a few minutes, evaluated lazily:

```
rate = rate * exp(-dt / tau) + bytes / tau
```

updated on read/write with `dt` since the last update, and decayed on read-out. This costs one
`exp()` per update, which is too much for the write path; instead we accumulate into a raw
counter and let the periodic `table_load_stats()` collection (every 60s) do the decay. The
accumulator lives in `compaction_group` (already per-tablet, one per split/merge side) and is
summed over the storage group at report time.

### How to ship it

There are two granularities to ship, and they have very different costs.

**Per table** is what the sizing algorithm needs, and it is cheap. `locator::load_stats`
gains:

```cpp
// Workload rate per table, in bytes per second, summed over all replicas of the table.
std::unordered_map<table_id, uint64_t> table_workload;
```

`load_stats` is not `final` in the IDL, so the field is added as a versioned field, and it
merges across nodes by summation the same way `table_load_stats::size_in_bytes` does.

**Per tablet** is what the multi-dimensional balancer needs, and it cannot be bolted onto
`tablet_load_stats`: that struct is declared `final` in the IDL precisely to bound how much is
put on the wire, so it admits no versioned fields. Per-tablet rates therefore need a new
`final` struct referenced from a new versioned field on `load_stats`:

```cpp
struct tablet_workload_stats final {
    // Keyed like tablet_load_stats::tablet_sizes; token ranges are in the form (a, b].
    std::unordered_map<table_id, std::unordered_map<dht::token_range, uint64_t>> tablet_rates;
};
```

with the same migration and resize reconciliation as `tablet_sizes`
(`migrate_tablet_size()`, `reconcile_tablets_resize()`): on migration the rate moves with the
tablet; on split it is divided between the children; on merge the children's rates are summed.
The replica-side accounting is already per tablet, so this step is purely transport.

### Degradation

A node that does not report `tablet_workload` (older version, or feature not yet enabled)
must not be excluded from balancing the way missing tablet sizes exclude a node today —
workload is a secondary goal. Instead, the `work` dimension is dropped from the load vector
for the whole rack while any node in it is not reporting, so that all shards are compared on
the same set of dimensions.

## Capacity-relative tablet sizing

### The rule

For each rack `R`:

```
shard_capacity(R)     = sum over hosts h in R of capacity(h) / shards(R)
capacity_tablet_size  = shard_capacity(R) * tablet_size_fraction_of_shard_capacity
target_tablet_size    = clamp(min over racks R of capacity_tablet_size(R),
                              minimal_tablet_size_for_balancing,
                              target_tablet_size_in_bytes)
```

with a new option `tablet_size_fraction_of_shard_capacity`, whose intended value is
`1 / tablets_per_shard_goal` — i.e. 1% with the default goal of 100 tablets per shard. It
defaults to 0 (disabled) until the balancer is aware of fullness (step 7 of the
implementation plan): a larger target means fewer, larger tablets at moderate utilization,
which only pays off once placement no longer balances effective utilization at all levels.

`capacity(h)` is the gross disk capacity for data files, not the effective capacity used by
size-based balancing, for the reason given under [The load vector](#the-load-vector): the
effective capacity follows snapshots and other non-tablet disk usage, and the target tablet
size should not.
The existing `target_tablet_size_in_bytes` (5 GiB) is retained as an **upper** clamp, which
bounds the migration unit on very large shards and makes the change backward compatible on
today's typical hardware. The lower clamp prevents absurdly small tablets on small disks and
implements goal 9.

The split/merge hysteresis around this target is unchanged: split above
`2 * target_tablet_size`, merge below `target_tablet_size / 2`, cancel a decision only once
the average size crosses back past `target_tablet_size`.

### Why this makes tablet count the control variable

At the target size, a shard filled to capacity holds exactly
`1 / tablet_size_fraction_of_shard_capacity = tablets_per_shard_goal` tablets. So, below the
per-shard tablet count budget, the per-shard tablet count is by construction the shard's disk
utilization expressed in percent, as long as utilization is high enough for the lower clamp
and the per-table floors not to bind. Tablets stay close to the target size (between
`target / 2` and `2 * target`) and the count grows with the data.

Once the per-shard count reaches the budget, the budget holds the count and tablets grow
instead: the average tablet size tracks utilization. The `table-growth` scenario of
`perf-load-balancing` shows both regimes. With 50 GiB per shard and a fraction of 1%, the
count grows from 10 to 66 tablets per shard as utilization grows from 1% to 92%, while the
average tablet size settles around 700 MiB; with the goal lowered to 20, the count is held at 20
from 46% utilization on and the average tablet size doubles with every doubling of the data.

Tablets are smaller than today's 5 GiB static target on typical hardware, which is what we
want: finer migration units and better achievable balance. Note that on production clusters
today the per-shard tablet count is usually dominated by the floors rather than by size: the
cluster from SCYLLADB-3446 holds 155 tablets per shard averaging 0.58 GiB, far below the
static target, most likely because `tablets_initial_scale_factor` and power-of-two
rounding set the count.

At low utilization — a freshly loaded cluster — the total data is small, so
`total_size / target_tablet_size` is small and the per-shard count lands *below* the goal.
No special case is needed for goal 9 beyond the lower clamp; the arithmetic does it. The
existing `tablets_initial_scale_factor` / `min_per_shard_tablet_count` floors still apply and
keep a minimum degree of parallelism for small tables.

### The per-shard tablet count budget

Today the global scale-down pass triggers when the projected per-shard tablet replica count
in a rack exceeds `tablets_per_shard_goal` and scales every table down uniformly to hit
exactly the goal. This changes to:

- **Trigger at `tablets_per_shard_budget_factor * tablets_per_shard_goal`** (the
  *budget*), not at the goal.
- **Scale down to `tablets_per_shard_goal`** (the *goal*).

With a budget factor of 2, the steady-state per-shard tablet count floats in
`[goal, 2 * goal]`. The gap between trigger and target is the hysteresis that prevents a
merge/split cycle: after merging, the count is at the goal, which is a factor of two away
from the next merge trigger, and tablet growth has to double the count again before we
merge once more.

The budget factor defaults to 1, which keeps today's behavior, and should stay there as long
as tablet counts are aligned to powers of two. The budget is compared against the projected
count before alignment, and alignment rounds up, so power-of-two rounding already lets the
count float up to twice the goal — which is why the cluster above sits at 155 tablets per
shard with a goal of 100. A budget factor of 2 on top of that would let it reach four times
the goal. The factor should move to 2 together with dropping the alignment
(`pow2_count = false` with arbitrary tablet boundaries), or with comparing the budget against
the aligned counts.

`tablets_per_shard_goal` thus becomes a soft goal with a hard-ish budget at twice its value,
and the balancer is allowed to keep counts above the goal rather than immediately merging —
which is the "prefer the per-shard tablet count goal" behavior asked for: a shard's tablet
count, not a globally fixed tablet size, is what the system regulates.

## Choosing which tables to merge

When the budget is exceeded, the current code scales every table in the rack by the same
factor. That is fair but wrong: merging tablets of a hot table halves the concurrency
available to it, while merging tablets of a cold archival table costs almost nothing.

The new rule picks tables in increasing order of **workload density**:

```
workload_density(t) = workload_rate(t) / bytes(t)
```

i.e. bytes/s of activity per byte stored — how hot the table's data is, independent of how
much of it there is. Tables are sorted ascending, ties broken by `table_id` for determinism,
and scaled down one at a time until the projected per-shard tablet count is back at the goal.
A table is not scaled below its own floor (`min_tablet_count`, `min_per_shard_tablet_count`,
`initial`, `expected_data_size_in_gb`), and is skipped if merges are forbidden for it
(`tablet_merges_forbidden()`, e.g. Alternator streams), or if it is already converging to a
power-of-two layout. The per-shard count derived from `tablets_initial_scale_factor` is not a
floor: it is a cluster-wide default rather than a demand of the table, and with enough tables
it alone reaches the goal, at which point treating it as a floor would leave the rack over the
goal for good.

Two stability concerns:

- **Ordering churn.** `workload_density` is derived from the smoothed rate, so it changes
  slowly. Still, two tables of similar heat could swap places between rounds and each be
  merged in turn. To avoid this, the density is quantized to its binary exponent before
  sorting, so a table has to be twice as dense as another to overtake it. This is stateless;
  two tables straddling a power-of-two boundary can still swap places. Keeping the previous
  round's relative order unless the densities differ by more than a fixed ratio would be
  stricter, but needs state carried between rounds.
- **Coldest-table starvation.** A single cold but very large table could absorb every merge
  round until it hits its floor. This is intentional — it is the correct table to merge — but
  it means cold tables without explicit floors end up with a single tablet, even when that
  tablet is well above `2 * target_tablet_size`, as the `table-growth` scenario shows. Whether
  the floor should include the count the table's size demands is an open question; the
  budget has always taken precedence over size.

When workload data is not available for a rack, the selection degrades to the current uniform
scale-down, which keeps behavior unchanged on clusters that have not enabled the feature.
Workload data counts as unavailable until the `TABLET_WORKLOAD_STATS` cluster feature is
enabled, because in a mixed-version cluster the reported rates only cover the upgraded
nodes.

## Stability, hysteresis and convergence

Summary of the mechanisms, since this design adds several control loops over the same state:

| Loop | Trigger | Target | Hysteresis |
|---|---|---|---|
| Split | `avg_tablet_size > 2 * target` | `target` | cancel only below `target` |
| Merge (per table) | `avg_tablet_size < target / 2` | `target` | cancel only above `target` |
| Merge (budget) | per-shard count > `budget_factor * goal` | `goal` | `budget_factor` gap to next trigger |
| Migration | `delta_Phi + beta * bytes < 0` | `Phi` minimum | movement cost `beta` |

The three sizing loops act on tablet *count* and are driven by *size*; the migration loop acts
on *placement* and is driven by the potential. They are coupled only through the tablet count
and sizes that both observe, and they run in the same `make_plan()` round, resize first. The
factor-of-two gaps make it impossible for a split decision and a merge decision to alternate
for the same table without the underlying data volume actually moving by 2x.

The migration loop is a monotone descent on `Phi`, so it terminates. The sizing loops are not
descent methods, and their termination rests on the hysteresis bands above — this is the same
situation as today, and is the reason the bands are multiplicative rather than additive.

## Observability and cluster scaling signals

With capacity-relative sizing, the per-shard tablet count and the average tablet size
together become the cluster's utilization signal, readable from outside. Below the budget,
the count tracks utilization and tablets stay near the target size:

```
tablets_per_shard / tablets_per_shard_goal  ~=  disk utilization
```

Once the budget holds the count, tablets grow instead:

```
tablets_per_shard / tablets_per_shard_goal * avg_tablet_size / target_tablet_size  ~=  disk utilization
```

- A per-shard count at the budget with the average tablet size growing towards
  `2 * target_tablet_size` means shards are approaching full: **scale out**.
- A per-shard count well below the goal, with tablets near the target size, means the
  cluster is over-provisioned: **scale in**.

This is a better signal than raw disk utilization alone because it already accounts for the
lower clamp and for tables with explicit floors.

New metrics to export per rack (or per DC, aggregated):

- `avg_tablet_size`, `target_tablet_size` (they are now dynamic, so the target must be
  exported too).
- `tablets_per_shard` — current, goal, budget.
- `load_potential` and per-dimension max/min normalized load, so that "which dimension is
  currently limiting balance" is directly visible.
- Per-table `workload_density`, and the merge-selection order it produced.

## Implementation plan

Each step is independently buildable and testable, in dependency order:

1. **Per-tablet workload accumulators** (`replica/`). `tablet_workload_tracker` in
   `replica/tablet_workload.hh`, owned by `storage_group_manager` and keyed by storage group
   id, with `table::apply()` and `table::query()` / `table::mutation_query()` recording into
   it. No behavior change.
2. **Report per-table workload in load stats** (`locator/`, `idl/`, `service/`). The
   `table_workload` field, the collection in `storage_service::load_stats_for_tablets()`, and
   the summation in `load_stats::operator+=`. No behavior change.
3. **Capacity-relative tablet sizing** (`service/tablet_allocator.cc`, `db/config.cc`).
   `tablet_size_fraction_of_shard_capacity`, the per-rack target, and the budget at
   `tablets_per_shard_budget_factor * goal`. Behavior-changing, so both options default to
   the current behavior, and the defaults should only move once
   `test/boost/tablets_test.cc` and `test/perf/tablet_load_balancing.cc` cover it.
4. **Workload-ordered merge selection** (`service/tablet_allocator.cc`). Replaces uniform
   scale-down, degrading to it when workload stats are incomplete.
5. **Report per-tablet workload** (`locator/`, `idl/`, `service/`). The new
   `tablet_workload_stats` struct and its migration/resize reconciliation. Gate on a cluster
   feature. No behavior change on its own; a prerequisite for step 7.
6. **Load potential** (`locator/load_potential.hh`). A standalone, unit-testable
   implementation of the normalized load vector and `Phi`, not yet wired into the balancer.
7. **Wire the potential into the balancer** (`locator/load_sketch.hh`,
   `service/tablet_allocator.cc`). Replace `load_type` with the vector, `migration_badness`
   with `delta_Phi`, and the `(max - min) / max` stop condition with monotone descent.
   Largest and riskiest step; kept behind a config flag initially, with the existing
   single-dimension path as the fallback. SCYLLADB-3446 proposes a blended size /
   effective-utilization metric for the existing path; it is the interim form of the `size`
   and `full` dimensions, and the two should be kept consistent.

Steps 1, 2, 5 and 6 are pure additions. Step 7 subsumes `force_capacity_based_balancing` and
the existing `size_based_balance_threshold_percentage` semantics, which must be kept working
for at least one release.

What the implementation of steps 1-4 leaves out:

- The per-tablet rates are collected on the replica but only the per-table sum is shipped
  (step 5).
- Read accounting charges the size of the result rather than the bytes actually read, and
  resolves the storage group from the first partition range of the query rather than per
  partition. In tablet mode the coordinator issues a separate read per tablet, so the
  attribution is exact for nearly all reads, but a heavily filtering scan is under-counted.
- The merge-order stability band is implemented by quantizing workload density to its binary
  exponent rather than by carrying the previous round's order forward. This is stateless and
  gives a factor-of-two band, but two tables straddling a power-of-two boundary can still
  swap places.

## Related work

Theory:

- R. Panigrahy, K. Talwar, L. Uyeda, U. Wieder. *Heuristics for Vector Bin Packing*, MSR
  TR, 2011. Establishes that dot-product / alignment heuristics beat naive weighted sums for
  multi-dimensional packing.
- S. Im, N. Kell, J. Kulkarni, D. Panigrahy. *Tight Bounds for Online Vector Scheduling*,
  FOCS 2015. The `O(log d / log log d)` bound and the exponential potential algorithm this
  design's `Phi` is taken from.
- N. Bansal, M. Elias, A. Khan. *Improved Approximation for Vector Bin Packing*, SODA 2016.
- V. Mirrokni, M. Thorup, M. Zadimoghaddam. *Consistent Hashing with Bounded Loads*,
  SODA 2018. Bounded movement under membership change.

Systems:

- Apache HBase `StochasticLoadBalancer` — simulated annealing over weighted cost functions
  including region count, per-server table skew, request rates, store file size, locality and
  a move cost. The closest existing analogue to the goals here; its weakness is exactly the
  weighted-sum combination this design avoids.
- A. Adya et al. *Slicer: Auto-Sharding for Datacenter Applications*, OSDI 2016. Key-range
  assignment with load-driven splitting and move minimization — the observation that resizing
  the unit of balancing is often cheaper than moving it.
- I. Gog et al. *Firmament: Fast, Centralized Cluster Scheduling at Scale*, OSDI 2016.
  Placement as min-cost max-flow; a candidate for a periodic global refinement pass on top of
  the incremental greedy planner described here.
- S. Kumar et al. *Shard Manager*, SOSP 2021. Constrained-optimization shard placement over
  multiple resources.
- Ceph `pg-upmap` balancer — incremental per-placement-group exception mapping driven by
  per-OSD utilization and PG count, structurally very close to tablet migration.
