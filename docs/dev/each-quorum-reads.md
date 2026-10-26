# EACH_QUORUM reads

Tracking issue: https://scylladb.atlassian.net/browse/SCYLLADB-922

## Motivation

`EACH_QUORUM` requires a quorum of replicas in every datacenter of a
`NetworkTopologyStrategy` keyspace. Until now ScyllaDB implemented it
only for writes, as Cassandra did when the relevant code was written
(Cassandra added `EACH_QUORUM` reads later, CASSANDRA-9602).

In multi-DC deployments writes often cannot use `EACH_QUORUM` for
latency reasons and go out at `LOCAL_QUORUM`. A reader in another DC
that wants to be sure it sees such a write today has only `ALL`, which
fails as soon as a single replica anywhere is down. `EACH_QUORUM` reads
overlap any `LOCAL_QUORUM` write in any DC (see
[Consistency guarantees](#consistency-guarantees)) while tolerating a
minority of replicas being down in each DC.

## Current state

`db::validate_for_read()` (`db/consistency_level.cc`) rejects
`EACH_QUORUM` with "EACH_QUORUM ConsistencyLevel is only supported for
writes". It is called from every CQL read entry point:
`select_statement::do_execute()`, the indexed and aggregate select
paths, `modification_statement::read_command()` (read-before-write of
non-LWT statements, e.g. list operations), and the external-search
select statement.

Everything below the statement layer already knows about `EACH_QUORUM`
in a per-DC way *except* replica selection and read-response
accounting:

| Function | Per-DC today? | Notes |
|---|---|---|
| `db::block_for()` | sum over DCs | `block_for_each_quorum()` returns the sum of per-DC quorums; falls back to `quorum_for()` for non-NTS. |
| `db::each_quorum_block_for_dc()` | yes | One DC's quorum, from the token's replica set (see [Replication factor changes](#replication-factor-changes-tablets)). |
| `db::assure_sufficient_live_nodes()` | yes | Throws `unavailable_exception(cl, dc_block_for, dc_live)` for the first DC that lacks a quorum. |
| `db::is_sufficient_live_nodes()` | yes | Used by the range-scan merger. |
| `db::filter_for_query()` | **no** | Takes the first `block_for` live endpoints by proximity. |
| `digest_read_resolver` (`service/storage_proxy.cc`) | **no** | Scalar `_cl_responses >= _block_for`. |
| `datacenter_sync_write_response_handler` | yes | Write-side per-DC accounting; the model for the read side. |

Why the two "no" rows matter, with two DCs at RF=3 (`block_for` = 2+2 = 4):

* `filter_for_query()` picks the four closest live replicas: three in
  the coordinator's DC and one remote. The remote DC's quorum is never
  contacted, so the read cannot honor the CL even though
  `assure_sufficient_live_nodes()` (which sees the same four targets)
  would then fail the request with `Unavailable` — a false failure
  whenever the cluster is in fact healthy.
* Even with correct selection, speculative retry adds an extra target
  (the default `speculative_retry` is a percentile, so
  `get_read_executor()` usually appends one). With five targets and a
  scalar count, three local responses plus one remote reach
  `4 >= 4` while the remote DC has answered with one of two.

## Design

The change is coordinator-only. Replicas are not involved, the wire
protocol already carries `EACH_QUORUM` (`transport/request.hh`,
`idl/consistency_level.idl.hh`), and the paging state is unchanged.
No cluster feature is needed; during a rolling upgrade a client may be
accepted by an upgraded coordinator and refused by an old one.

### Replica selection: `db::filter_for_query()`

Add an `EACH_QUORUM` branch, gated exactly like the write side
(`rs.get_type() == replication_strategy_type::network_topology`);
other strategies keep falling through to the scalar path, where
`block_for()` already degrades to `quorum_for()`.

`live_endpoints` arrives sorted by proximity
(`storage_proxy::get_endpoints_for_reading()`), with the local node
first if it is a replica. The new branch:

1. Groups the live endpoints by DC, preserving their relative order.
2. Takes `each_quorum_block_for_dc(erm, rf, dc)` endpoints from each
   DC, where `rf` is the `token_replication_factor` of the token's
   read replica set (see below). If a DC has fewer live replicas than
   that, it takes what is there and does **not** throw: the caller's
   `assure_sufficient_live_nodes()` is the single place that raises
   `Unavailable`, with the per-DC `required`/`alive` numbers the
   drivers' downgrading retry policies expect.
3. Applies `preferred_endpoints` (the replicas the previous page was
   served from) per DC, so paging stays sticky in each DC as it does
   for the other CLs.
4. Sets `*extra` (the replica for speculative retry) to the first
   unselected live replica, preferring the local DC. Since the
   resolver below counts per DC, an extra replica can only ever help
   the DC it lives in; preferring the local DC is where speculation
   is most useful and cheapest.
5. Does not apply cache-hit-rate load balancing
   (`miss_equalizing_combination`) in the first version. The scalar
   path reorders the whole candidate list; doing that per DC is a
   self-contained follow-up and skipping it costs nothing in
   correctness.

Legacy `read_repair_decision` values: `GLOBAL` already returns all
live endpoints before any CL logic. For `DC_LOCAL` the local DC takes
all of its live replicas instead of a quorum, matching the
`bf = max(bf, local_count)` rule of the scalar path. Nothing sets a
non-`NONE` decision any more (it is only carried in old paging
states), so this is for compatibility, not a feature.

RF=0 DCs (including DCs of zero-token nodes) have a quota of 0 and
select nothing, and so does a DC the token has no read replica in yet
(see below). A DC that holds replicas but has none live selects nothing
and makes `assure_sufficient_live_nodes()` throw — the same rule writes
follow.

### Replication factor changes (tablets)

With tablets, a replication factor change migrates the tablets one by
one (`rebuild` transitions, one replica per DC per `ALTER`). In the
default flow (`topology_coordinator::keyspace_rf_change`,
`default_rf_change`) the new strategy options are committed in the
same group0 batch that creates the transitions, so the *schema* RF is
the new one from the first instant, while reads use the old replica
set until the `write_both_read_new` stage and writes go to the old set
plus the pending replica. Sizing a consistency level from the schema
RF is therefore wrong during the change — for every per-DC level, not
just `EACH_QUORUM`: with RF 1→2 in a DC, `LOCAL_QUORUM`, `QUORUM`,
`EACH_QUORUM` and `ALL` reads and writes are all `Unavailable` until
the migration completes (verified by pausing a migration in the
`streaming` stage with the `migration_streaming_wait` injection;
tracked as SCYLLADB-1228 / scylladb/scylladb#20625, with the write-side
double count of the pending replica as SCYLLADB-4856).

scylladb/scylladb#31903 fixes this generally and this series builds on
it: `effective_replication_map::get_read_replicas_with_rf()` returns the
read replica set together with a `locator::token_replication_factor`
counted from that set (per DC for the levels that need it; the schema
RF for vnodes), and `db::block_for()`, `filter_for_query()`,
`is_sufficient_live_nodes()` and `assure_sufficient_live_nodes()` take
it. `each_quorum_block_for_dc(erm, rf, dc)` is the quorum of the
token's read replicas in `dc`, and 0 for a DC the schema does not list
or lists at RF 0. For `EACH_QUORUM` reads this gives exactly the
behaviour an RF change needs without any special casing:

* a DC being added (RF 0→N) has no read replica until
  `write_both_read_new`, so its quota is 0 and the read ignores it —
  the data is still only on the old replica set, which is where it is
  read from, while writes already go to both;
* a DC going k→k+1 asks for a quorum of its k read replicas while
  reads are on the old set, and of k+1 once they move to the new one;
* a DC removed from the schema whose tablets still hold a replica gets
  no quota; that replica is never selected (it may serve as the
  speculative extra).

The rack-list-only `multi_rf_change` flow (0→N changes between
rack-list strategies) keeps the old strategy options, with the new
ones in `next_strategy_options`, until the migrations finish, and its
`ALTER KEYSPACE` blocks until then; there the schema RF is the old one
throughout and both sizings agree.

Vnodes have no transitional replica sets: an RF change takes effect
immediately and the operator repairs afterwards, so `rf` is the
schema RF and the selection reduces to the per-DC configured quorum.

### Response accounting: `digest_read_resolver`

Mirror `datacenter_sync_write_response_handler`. Today the resolver is
constructed from counts only (`block_for`, `target_count_for_cl`); it
needs the executor's `_targets` vector as well, so that it can group
them by DC, and the token's `token_replication_factor`, which the
executor keeps as `_rf` (`get_read_executor()` and the range-scan path
get it from `get_endpoints_for_reading()`). The targets are passed **as
they are at construction time, the speculative extra included** — the
same convention as today's `_target_count_for_cl`, which is
`_targets.size()` before the speculating executor has sent to its last
target, so the failure predicate already treats the extra as
available. Counting only already-sent targets would make
`need + failed > targets` fire too early in the DC that holds the
extra.

For `EACH_QUORUM` on NTS the resolver keeps a per-DC record:

```c++
struct dc_info {
    size_t need;       // responses required from this DC
    size_t responses;  // received so far
    size_t failed;     // errors so far
    size_t targets;    // targets in this DC
};
std::unordered_map<sstring, dc_info> _dc_responses;
```

* `need` is `each_quorum_block_for_dc(erm, rf, dc)`, except when the
  executor has raised `_block_for` to `_targets.size()`
  (`never_speculating_read_executor` does this: every target is
  required). Then `need = targets`, so that executor keeps its current
  "wait for everyone" semantics. The resolver detects this from
  `block_for == targets.size()` on its own arguments; no new flag is
  added to the executor interface.
* `waiting_for(ep)` is `true` for every target (it is `false` only for
  remote replicas under `LOCAL_*` with a global read-repair decision).
* CL is reached when every DC has `responses >= need` and a data
  result has arrived (`_data_result`), same as today's
  `_cl_responses >= _block_for && _data_result`.
* On an error from a replica in DC *d*: `failed++` for *d*; the
  request fails when `need + failed > targets` for *d*, i.e. that DC
  can no longer reach its quorum. The `DISCONNECT`-without-spare
  special case (`_block_for == _target_count_for_cl`) becomes
  `need == targets` for *d*.
* The timeout and failure exceptions report
  `received = Σ responses`, `block_for = Σ need`, as today.

For all other CLs the resolver is unchanged. Implementation choice:
keep the scalar counters and add the map only when
`cl == EACH_QUORUM && NTS`; the predicates `cl_reached()` and
`dc_cannot_reach_quorum(ep)` hide the two cases from `got_response()`
and `on_error()`.

### What does not need to change (verified)

* `get_read_executor()`: the speculation decisions use
  `block_for == all_replicas.size()` and `target_replicas.size() ==
  block_for`, which hold under sum semantics. The
  `is_datacenter_local(cl) && !local_dc_filter(*extra_replica)` check
  that drops a remote extra under `LOCAL_*` does not apply.
* Range scans (`query_partition_key_range_concurrent()`): go through
  `filter_replicas_for_read()` → `filter_for_query()`, merge adjacent
  vnode ranges only if `is_sufficient_live_nodes()` (already per-DC)
  holds for the intersection, and always use
  `never_speculating_read_executor`, whose `need = targets` rule above
  is exact.
* Digest mismatch: `reconcile()` sends mutation-data requests to every
  contacted target and `data_read_resolver` waits for all of them;
  nothing CL-specific. `schedule_repair()` then writes the diffs with
  `_cl = EACH_QUORUM`, which `make_write_response_handler()` routes to
  `datacenter_sync_write_response_handler`. That is satisfiable because
  `data_read_resolver::resolve()` emits a diff entry for *every*
  contacted replica (`nullopt` for an up-to-date one; only partitions
  with no diff on any replica are dropped), so the write targets are
  all contacted replicas (≥ a quorum in each DC), and
  `per_destination_mutation` immediately acks a `nullopt` entry
  without sending anything.
* The "local repair only if the data changed within `write_timeout`"
  shortcut in `execute()` is `is_datacenter_local()`-only; an
  `EACH_QUORUM` read repairs all contacted DCs, which is what it
  promises.
* LWT: `SERIAL`/`LOCAL_SERIAL` reads are a separate path. The commit
  CL (`cl_for_learn`) already accepts `EACH_QUORUM`
  (`validate_for_cas_learn()`).
* Strongly consistent (tablet-raft) tables parse the CL themselves
  (`cql3/statements/strong_consistency/select_statement.cc`) and refuse
  `EACH_QUORUM` with their own message. They are unaffected by this
  change and keep refusing it — a linearizable read already reads a
  raft quorum, so `EACH_QUORUM` has nothing to add there.

### Enabling: `validate_for_read()`

Remove the `EACH_QUORUM` case. `modification_statement::read_command()`
picks this up too: a non-LWT write that needs a read-before-write
(e.g. `list[i] = x`) at `EACH_QUORUM` now reads at `EACH_QUORUM`, as it
does for every other write CL, instead of failing with "Write operation
require a read but consistency EACH_QUORUM is not supported on reads".

## Consistency guarantees

An `EACH_QUORUM` read contacts a quorum in every DC. It therefore
overlaps — and so observes — any completed write at:

| Write CL | Overlap guaranteed? | Why |
|---|---|---|
| `EACH_QUORUM` | yes | two quorums in the same DC intersect |
| `LOCAL_QUORUM` (any DC) | yes | same, in the writer's DC |
| `QUORUM` | yes | a global majority is a majority in at least one DC |
| `ALL` | yes | |
| `ONE`, `TWO`, `THREE`, `LOCAL_ONE`, `ANY` | no | |

Conversely an `EACH_QUORUM` write is observed by `EACH_QUORUM`,
`LOCAL_QUORUM` (any DC), `QUORUM` and `ALL` reads. Availability: in
each DC the read tolerates `RF_dc - quorum_dc` replicas down; the
whole read fails if any one DC loses its quorum.

These rules drive the documentation and the consistency-level
calculator changes below.

## Tests

`test/cluster/test_each_quorum_reads.py`, two DCs. Where the test
needs to distinguish per-DC from scalar logic it uses RF=3 in each DC
(6 nodes, `block_for` = 4); the rest can use smaller topologies.

1. **Availability is per DC.** Stop two nodes in DC1. Four replicas
   are live in total, which satisfies a scalar `block_for` of 4, but
   DC1 has 1 < 2: the read must fail with `Unavailable`, and the
   exception's `required`/`alive` must be DC1's 2/1.
2. **Accounting is per DC.** RF 3/2 (5 nodes, `block_for` = 2+2 = 4)
   and a table with `speculative_retry = 'ALWAYS'`: with all five
   replicas live the extra is appended and `always_speculating_read_executor`
   contacts all five, so both DC2 replicas are targets whichever one
   the test picks (at RF 3/3 only two of the three DC2 nodes would be,
   and the delayed one might be the unselected one). Delay one DC2
   replica's response with an error injection on the replica read path
   (reuse an existing injection point in
   `storage_proxy::query_result_local()` / `database::query()` rather
   than adding one). Three DC1 responses plus one DC2 response reach
   the scalar `4 >= 4`; the read must not complete until the delayed
   DC2 replica answers.
3. **Cross-DC visibility.** `LOCAL_QUORUM` write via a DC1 coordinator;
   `EACH_QUORUM` read via a DC2 coordinator returns it.
4. **Read repair.** Hints disabled. Stop one DC2 node, write at
   `LOCAL_QUORUM` via DC1, restart the node, `EACH_QUORUM` read via DC2
   returns the write; a subsequent `ONE` read pinned to the restarted
   node (or its `read_repair_repaired_blocking` metric) shows it was
   repaired.
5. **Read-before-write.** A list-element update at `EACH_QUORUM`
   succeeds.
6. **SimpleStrategy fallback.** On a `SimpleStrategy` keyspace an
   `EACH_QUORUM` read behaves as `QUORUM` (succeeds with one replica
   down at RF=3, fails with two down).
7. **Range scan.** A full-table `SELECT` at `EACH_QUORUM` succeeds with
   one node down per DC and fails with two down in one DC.
8. `test/cluster/test_zero_token_nodes_multidc.py`: drop the
   `if cl == ConsistencyLevel.EACH_QUORUM: continue` skip; both
   assertions there hold for `EACH_QUORUM` reads.
9. `test/cqlpy`: single-node smoke test that a `SELECT` at
   `EACH_QUORUM` no longer raises `InvalidRequest`.
10. `test/boost`: a unit test for the new `filter_for_query()` branch
    (pure logic over a synthetic topology: per-DC pick, order
    preserved, short DC returns what it has, preferred endpoints and
    `extra` honored per DC). It lands with patch 1 so that patch
    carries its own test.
11. **RF change in progress** (tablets). Pause a migration in the
    `streaming` stage (`migration_streaming_wait`) and read at
    `EACH_QUORUM`: adding a DC (RF 0→1) must not involve the new DC —
    the read succeeds and returns the data from the old replica set;
    raising a DC from `['rack1']` to `['rack1', 'rack2']` must ask that
    DC for a quorum of its one old replica, so the read succeeds while
    the schema RF already says 2.

`test/cqlpy/strong_consistency/test_strong_consistency.py` keeps
`EACH_QUORUM` in its refused list; see above.

## Documentation

* `docs/cql/consistency.rst`: drop "(Write Only)" from the
  `EACH_QUORUM` row and describe the read semantics.
* `docs/cql/consistency-calculator-raw.html`: today it models a single
  DC (`nodes`, `replication factor`, CLs `ONE`/`TWO`/`THREE`/`QUORUM`/`ALL`)
  and decides "consistent" by `write_n + read_n > rf`. Extend it with a
  datacenter count and a per-DC RF (uniform RF across DCs is enough for
  the calculator), add `LOCAL_ONE`, `LOCAL_QUORUM` and `EACH_QUORUM` to
  both selectors, and derive the verdict from the overlap table above:
  per-DC levels intersect within a DC, global levels across the
  cluster, and the `QUORUM`-vs-`EACH_QUORUM` case by the pigeonhole
  rule. The "nodes you can lose" lines become per-DC for the per-DC
  levels.

## Patch series

The series is based on scylladb/scylladb#31903 (consistency levels
sized from the token's replica set) and must not be merged before it.

Each patch builds and passes tests on its own. Patches 1 and 2 are
unreachable until patch 3 removes the rejection, so no `xfail` is
needed for the new tests, which land in patch 4.

1. `consistency_level: select a quorum per datacenter for EACH_QUORUM reads` —
   the `filter_for_query()` branch.
2. `storage_proxy: count EACH_QUORUM read responses per datacenter` —
   the `digest_read_resolver` change.
3. `consistency_level: allow EACH_QUORUM for reads` — `validate_for_read()`.
4. `test: EACH_QUORUM reads` — new cluster test, zero-token test
   un-skip, cqlpy smoke test. (The `filter_for_query()` boost test goes
   with patch 1.)
5. `docs: EACH_QUORUM is supported for reads` — `consistency.rst`.
6. `docs: multi-DC consistency levels in the CL calculator`.

## Follow-ups (out of scope)

* Per-DC cache-hit-rate load balancing in `filter_for_query()`.
* Per-DC speculative retry (one extra replica per DC instead of one in
  total).
