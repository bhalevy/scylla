# Age tiering in the incremental compaction strategy

Status: experimental. The `age_tiering` compaction option is off by default.

## Motivation

A tombstone is purged by a compaction only if no sstable outside that
compaction may hold data it shadows. `get_max_purgeable_timestamp()`
(`compaction/compaction.cc`) decides that per partition key: it takes the
oldest (live) timestamp of every other sstable whose bloom filter matches
the key, and a tombstone is purgeable only if it is older than all of them.

Two consequences follow:

- An sstable that holds only data newer than the tombstone costs nothing.
  It is skipped before its bloom filter is even consulted.
- An sstable that holds any older data holds back the purging of every
  tombstone newer than its oldest timestamp, for every key it may contain.

Size-tiered bucketing looks at size alone. A small run that holds old data
therefore lands among the recently flushed ones and is compacted with them.
The output carries the old run's oldest timestamp and covers the keys of all
its inputs, so it holds back more tombstones than the old run did, and it
keeps being rewritten among the small tiers.

Small runs holding old data come from:

- compactions whose output is much smaller than their input (deletes, TTL,
  overwrites, the garbage collection job);
- cleanup and tablet split;
- repair, streaming and tablet migration;
- memtable flushes of varying size: a small flush followed by a larger one.

The goal is a laminar flow: data moves from small and new runs to large and
old ones, and old data is compacted with data of its own age.

## Design

All of it is in `compaction/incremental_compaction_strategy.cc`.

### Age of a run

A run's age is its newest timestamp, `sstable_run::max_timestamp()`. Its
oldest is the minimum `min_timestamp` of its fragments.

The newest timestamp decides whether a run is old, so that a recently
written run holding a few writes with old client timestamps is not taken
for old data.

### Bucketing: `get_age_tiered_buckets()`

1. Runs are bucketed by size, as before (`get_buckets()`, unchanged, and
   still what reshape, cleanup, TWCS and LCS use).
2. Each bucket gets a watermark: the newest data in it or in any larger
   bucket. Watermarks are therefore non-increasing with size.
3. A run whose newest data is older than the watermark of the next larger
   bucket is moved out of its size bucket, to the *age band* of the largest
   bucket whose watermark is still newer than the run.
4. The runs of each band are bucketed by size among themselves. These are
   the *age buckets*.

The result has three views:

- `size_buckets`: the size buckets, without the runs moved out.
- `age_buckets`: the moved runs, by band and size.
- `tiers`: each size bucket together with the runs of its band.

Watermarks are computed from all runs of the table, including those being
compacted, which are not candidates. Otherwise, while a large tier is being
compacted, the small runs holding data of its age would pass for new.

### Selection

- A job is picked among the size buckets and the age buckets alike: the one
  with the most runs, of those holding `min_threshold`. Runs moved out do
  not count towards their band's size bucket, so they never cause a large
  tier to be rewritten.
- The garbage collection job looks at `tiers`, so that a tier is compacted
  along with the smaller runs of its age. These hold the old data its
  tombstones shadow, and tombstones of their own.
- The space amplification goal job looks at the size buckets.

### Taking in runs of fitting age: `add_runs_within_age_range()`

A run moved out of its size bucket could wait long for others of its age and
size. So any job also takes smaller runs that it can take without mixing
ages:

- the run holds no data newer than the job's newest data;
- the run holds no data older than the job's oldest data, unless no larger
  run outside the job holds data newer than that. Otherwise the output
  would hold back tombstones in those larger runs, which the job alone
  would not;
- the run is no larger than the job's smallest run.

The runs written earliest are taken first. They are limited to a tenth of
the job's size and to `max_threshold` runs in total.

### Backlog

The backlog tracker and `estimated_pending_compactions()` use the same
bucketing, so the compaction controller sees what selection would compact.
Runs taken in for their age are not accounted for.

## Evaluation

`scylla perf-compaction-efficiency` got, for this:

- age inversions: runs holding only data older than the newest data of a
  run at least twice their size;
- data and tombstones on disk, averaged over the run;
- per-sstable timestamps, tombstones and origin under `--verbose`;
- `--commitlog-holds-back-tombstone-gc`. By default the benchmark now lifts
  the commitlog's bound on tombstone purging: a tombstone newer than the
  oldest write a live commitlog segment holds for the table is not purged,
  and in a short run the first segment never goes away, so nothing was
  purged at all.

Results, dev mode, serial mode, 500k operations over 20k partitions, mean of
5 seeds, `tombstone_compaction_interval=1,tombstone_threshold=0.05`, option
off -> on:

| Workload | Tombstones on disk | Data on disk | Write amp. | Read amp. |
|---|---|---|---|---|
| default (25% rewrites, 25% deletes) | 4265 -> 3486 | 2.204 -> 2.161 MB | 4.12 -> 4.14 | 1.85 -> 1.78 |
| 50% deletes, 20% rewrites | 7253 -> 6191 | 1.195 -> 1.159 MB | 2.85 -> 2.55 | 1.72 -> 1.68 |
| uniform key distribution | 4041 -> 3316 | 2.228 -> 2.180 MB | 4.22 -> 4.07 | 1.57 -> 1.50 |

Tombstones on disk were lower in 13 of the 15 pairs of runs.

These are small (1-2 MB), short (15 s) runs. In them, age inversions come
from the varying flush size alone: with a fixed flush size there are none,
and the option changes nothing.

## Known limitations and open questions

- The benchmark does not produce the cases the design is for: a large tier
  shrinking, or old data arriving late.
- A run that already mixes old and new data cannot be told from a new one
  without a per-sstable timestamp histogram.
- The oldest timestamp used is `min_timestamp`, while the purge check uses
  `min_live_timestamp` where available.
- There is no tolerance for clock skew between coordinators.
- The space amplification goal job does not see the age buckets.
- `tombstone_purge_stats`, and so `system.compaction_history`, counts
  partition and range tombstones only. Row and cell tombstones are purged
  through `compact_and_expire()` without being counted.
