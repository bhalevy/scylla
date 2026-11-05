# Object-storage NVMe page cache: cost model

A cost model for serving a ScyllaDB workload whose dataset lives on object storage, with the
active working set served from a page cache on local NVMe. It sizes a cluster for a given
workload and compares the annual cost against the same workload held entirely on local NVMe.

This is a sizing and sensitivity tool, not a quote. Every default is an assumption, and the
useful output is which parameter the answer is sensitive to. Six of those assumptions have
already been corrected, each time moving the answer substantially and always in the same
direction, which is reason to distrust what is left rather than to trust the total.

The defaults now follow the implemented design in
[PR #32563](https://github.com/scylladb/scylladb/pull/32563), which is the authoritative
description of how the cache actually behaves. Read it before trusting anything here.

## The two designs it compares

**v1, "tablet"** (implemented). Cached pages are rows in a CQL table. Eviction deletes a row,
and incremental compaction holds the table at a space amplification goal of 1.5.

**v2, "files"** (a proposal, not implemented). Cached pages are regular files under one
directory per sstable, and the CQL table holds only an index row per cached page. Eviction
unlinks the file, so amplification falls to about 1.09.

## Running it

```sh
python3 model.py              # the default scenario under both designs
python3 sweep.py              # every sensitivity sweep
python3 sweep.py hit_rate replicas
python3 edge_cases.py         # the two degenerate bounding cases
python3 scaleout.py           # hit rate and cost against cluster size
python3 scaleout.py --svg DIR # and the chart the deck carries
```

No dependencies beyond the standard library. `model.Params` is a frozen dataclass; override
any field with `dataclasses.replace`.

## Where it stands

At the default scenario (50 TB logical cold data, 2 TB working set, RF 3, LZ4 2x, 150K
reads/s, 20K writes/s, 90% data-range hit rate with the sstable index resident, 4 KiB pages,
i7i.2xlarge):

| | nodes | binding | annual cost | vs all-NVMe |
|---|---|---|---|---|
| all-NVMe baseline | 60 | NVMe | $396,828 | |
| with page cache | 7 | throughput | $448,814 | −13.1% |

Both cache designs give that same figure. The capacity case is settled: sixty nodes become
seven, disk falls to 27% utilisation, and what binds instead is an assumed 10,000 operations
per vCPU. The cost case is entirely object-storage requests, which are $378,699 of the total.

That −13.1% is the figure for a cache sized to stop at the 90% target. It is not the best the
same seven nodes can do, because they are bought for throughput and most of their disk is
idle; see "Scaling out" below.

Break-even is close on every axis: 91% hit rate against an assumed 90%, 130K reads/s against
an actual 150K, and 63 TB of cold data against an actual 50 TB. Any single read-path change
closes the gap: one replica per read instead of two gives +37.9%, a 95% hit rate gives
+34.6%, and both together give +80.8%.

## The bounding cases

Two degenerate scenarios, neither of them considered a typical workload, bracket what the
design can be worth. `edge_cases.py` costs each under three setups: all-NVMe, object storage
with no cache at all (expressed as hit rates of zero, so every range read is a GET), and
object storage behind the page cache.

**Write only.** 50 TB logical, 20,000 writes/s, no reads.

| | nodes | binding | annual cost | vs all-NVMe |
|---|---|---|---|---|
| all-NVMe | 60 | NVMe | $396,828 | |
| S3 only, no cache | 4 | RAM | $50,552 | +87.3% |
| S3 + page cache | 4 | RAM | $50,552 | +87.3% |

With no reads the cache is never populated, since only user reads fill it, so the last two
rows are identical to the dollar: the cache costs nothing and buys nothing. The saving is
entirely in not owning NVMe for 75 TB. What sizes the cluster is RAM for per-sstable metadata
over about 82,000 cold sstables, which makes this case sensitive to `sstable_metadata_mb`
rather than to anything about caching: 3 nodes at 2 MB, 13 nodes and +72.3% at 10 MB.

**Perfect fit.** 20 TB logical, a 2 TB working set, 100,000 reads/s at a 100% hit rate, no
writes. "Raw" is read as logical, uncompressed, single copy, the same convention as the
customer scenario.

| | nodes | binding | annual cost | vs all-NVMe |
|---|---|---|---|---|
| all-NVMe | 24 | NVMe | $158,731 | |
| S3 only, no cache | 4 | throughput | $5,081,323 | −3101% |
| S3 + page cache | 4 | throughput | $35,563 | +77.6% |

The cache is what stands between those two figures: without it the workload pays 400,000
GETs/s. With it, object storage is billed for capacity alone. Both cache designs cost the
same; v1 needs 4 nodes for NVMe and v2 needs 3, but throughput binds at 4 either way.

The caveat is that "perfect" does all the work. At a 99% hit rate this case is +61.7%, and at
95% it is −1.9%, already break-even. Five percent of 100,000 reads/s is 10,000 misses/s, and
misses are the entire bill. The no-cache row charges two ranges per read per replica against
the 1.16 requests per read PR #32563 measured uncached, so treat it as an upper bound.

The envelope across all three scenarios is therefore +87% to −13%, set almost entirely by the
read path.

Setting the spreadsheet's read rate, working set and both hit rates to zero reproduces the
write-only row there too, at 4 nodes and $50,552, so the two stay verified against each other
for this case as well.

## Scaling out: the cache the cluster already has

Everything above sizes forwards: pick a hit rate, size the cache to meet it, count the nodes.
That understates the architecture, because at this scenario the binding constraint is
throughput, not disk. Seven nodes carry 10.7 TB of usable NVMe and a cache sized for a 90%
target uses 4.2 TB of it, so two thirds of the disk sits idle.

`scaleout.py` runs it the other way round, which is what the implementation does anyway: pin
the cluster at n nodes, give the cache all their NVMe, and read the hit rate off
`model.hit_rate_for`. More nodes buy more cache and a higher hit rate against a higher
instance bill, so there is a cost optimum rather than a constraint.

The answer turns almost entirely on `tail_exponent`, the concavity of the hit-rate curve once
the working set is cached, and nobody has measured it. So the output is a band, not a number
(implemented v1 design, i7i.2xlarge):

| tail exponent | at 7 nodes | optimum | vs all-NVMe |
|---|---|---|---|
| 1.0, uniform tail | 90.6%, $427,791 | none; the curve only rises | −7.8% |
| 0.5, midpoint | 92.4%, $359,637 | 15 nodes, $346,348 | +12.7% |
| 0.215, PR 32563's Zipf | 95.4%, $245,549 | 11 nodes, $238,884 | +39.8% |

Against $448,814 and −13.1% with the cache capped at the 90% target. The three exponents each
have a provenance: 1.0 is a uniformly accessed tail and the pessimistic bound; 0.215 is what
PR #32563's benchmark implies, since 0.34 requests per read against 1.16 uncached is a 0.707
hit rate at a cache one fifth of the data, and `ln 0.707 / ln 0.2` is 0.215 under a power law;
0.5 is the midpoint and nothing more. Quote the band, not a row of it.

Two caveats. The curve itself is the least grounded thing in the model: it assumes a share
`ws_read_share` of reads falls inside the working set and is served once that is cached, with
the rest landing in a tail whose concavity is the swept parameter. And the implementation
configures a cache size rather than inheriting the disk: `object_storage_page_cache_size_in_mb`
defaults to 10 GiB per node, so this graph assumes the cache is configured to use the NVMe the
cluster already owns.

## Does the file-based design earn its place?

At this scenario, no, and two errors of mine are why it once looked necessary. Assuming
64 KiB pages and letting tombstones accumulate over a retention window produced amplification
of 7x and more. The implementation caches 4 KiB pages and compaction targets 1.5x.

| working set | v1 tablet | v2 files |
|---|---|---|
| 2 TB | 7 nodes, $448,814 | 7 nodes, $448,814 |
| 5 TB | 7 nodes, $448,799 | 7 nodes, $448,799 |
| 10 TB | 14 nodes, $495,069 | 11 nodes, $475,228 |
| 40 TB | 56 nodes, $772,693 | 41 nodes, $673,486 |

The designs are indistinguishable until the working set is large enough for NVMe to bind
again, around 10 TB. The argument that survives is not node count but capacity per disk:
1.09x fits about 38% more live cache than 1.5x on the same hardware.

That argument is no longer hypothetical. Once the cache fills the disk rather than stopping
at a target, the cache is disk-limited by construction, and the extra capacity turns straight
into hit rate. At seven nodes the file design holds 9.8 TB live against the tablet design's
7.1, and costs $14K to $26K a year less depending on the tail exponent. Whether that is worth
the implementation is a different question, but it is no longer zero.

## What decides the answer

1. **Requests per read**, which is 85% of the bill. The implementation coalesces a miss into
   a single request for the rest of the range, so this turns on how often a range misses, not
   how many pages miss. Page size therefore does not affect cost at all: sweeping 4 to 64 KiB
   leaves the total unchanged.
2. **Replicas contacted per read.** Each one pays its own requests.
3. **Sustainable ops per vCPU**, currently binding at an assumed 10,000. At 15,000 it is five
   nodes.
4. **Per-sstable resident metadata**, which sets cold-data RAM. Immaterial between 1 and 3 MB
   because throughput binds first, binding at 5 MB, 13 nodes at 10 MB.

## Open questions

Parked for whoever picks this up, roughly in order of value.

1. **The shape of the cold tail**, `tail_exponent`, which now decides the scale-out answer
   and swings it from −8% to +40%. The same measurement answers the next question, since
   both are about how concentrated reads are.
2. **Requests per read on a real path.** PR #32563 measured 0.34 per read with a cache one
   fifth the size of the data under Zipf point reads, and 1.16 with no cache. This model
   assumes 0.10 per read per replica at a cache sized for 90%. Those are not directly
   comparable, and reconciling them is the single most valuable thing left to do.
3. **Whether a read must consult two replicas**, or whether one suffices. This alone moves
   −13% to +38%.
4. **Sustainable operations per vCPU** for this workload, which now sets the cluster size.
5. **Resident metadata per cold sstable**, against what TOC, Statistics, CompressionInfo and
   the bloom filter actually occupy. The 3 MB default is built from a bloom filter at the
   0.01 default false-positive rate (about 10 bits per partition, roughly 1.2 MB at 1 KB
   partitions), a compression offset table at the 4 KiB default chunk size (about 262,000
   bit-packed entries, roughly 1.2 MB), and about 0.2 MB of index summary.
6. **Whether the working-set sstable index genuinely stays resident.** The model assumes it
   does, which holds only if hot rows cluster in key space. At a 95% index hit rate the
   design is −61% rather than −13%.

## Still not modelled

Behaviours described in PR #32563 that the model does not represent:

- **The cold period after compaction.** New sstables are not cached and the old ones' pages
  are evicted, so reads of compacted data miss until they repopulate. The model has an
  invalidation term but no cold period following it.
- **No scan resistance.** A full scan without `BYPASS CACHE` evicts pages other reads need.
- **Only Data and index components are cached.** The small components are fetched whole when
  an sstable is opened and held in memory, which is what the per-sstable metadata parameter
  counts.

## Corrections made so far

The headline moved from −121% to −13% across these. Every one moved the same way.

1. The cache index row size was grounded in the ME-format writer rather than estimated. A
   live row keyed by `(sstable_id, component, offset)` is 15–20 bytes, and an evicted page
   costs 40–60 bytes including its tombstone. Note that at a 4 KiB page a 200-byte row is 5%
   of the payload, where at 128 KiB it was 0.15%, so this is no longer negligible.
2. The hit rate was ambiguous between per page lookup and per read, a factor of four.
3. Sstable index pages were given their own hit rate, separate from data pages. The index is
   3–5% of the data, so the working-set index is small, dense and heavily reused.
4. Compaction read size was decoupled from the cache page size. Tying them made smaller pages
   spuriously expensive, since compaction streams large sequential ranges.
5. The flat 1:100 data:RAM ratio was replaced by 1:30 over the working set plus per-sstable
   resident metadata over the cold remainder. The all-NVMe baseline applies 1:30 to
   everything, having no tiering, which makes storage-dense instance types RAM-limited rather
   than disk-limited and so a less punishing comparison than it first appeared.
6. Requests were charged per missed page; they are now charged per missed range, which is
   what the implementation does. Page geometry moved to 4 KiB, space amplification to the
   1.5 goal compaction actually targets, and write-allocate to zero, since only user reads
   populate the cache.

## Companion artefacts

A spreadsheet of the same model,
[NVMe caching TCO Calculator](https://docs.google.com/spreadsheets/d/1y1FKyORcZpXb9wLGLIut5iyKBkc-NUQm8Gj3E8GmbcM),
viewable across the ScyllaDB organisation, carries the same formulas plus a pricing tab, a
method tab and a conclusions tab holding the full sweep output. The two are verified against
each other to the dollar for both cache designs; if you change one, change the other and
re-check.

A [slide deck](https://claude.ai/artifact/KgZDcu1DpcZrokRYKdYtCJ) walks through the model and
the sweeps, and is current with this revision.

Instance and object-storage prices are AWS us-east-1 on-demand figures taken from third-party
aggregators on 2026-10-28, not from the AWS price list. Verify them before quoting.
