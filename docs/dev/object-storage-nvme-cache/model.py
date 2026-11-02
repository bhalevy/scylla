# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Cost model for serving a ScyllaDB workload from object storage behind an NVMe page cache.

The model sizes a cluster for a given workload under two candidate cache designs and
compares the result against the same workload held entirely on local NVMe:

  v1, "tablet"  every cached page is a row in a CQL table. Eviction is a DELETE, so the
                space returns only when compaction merges the tombstone with the page.
  v2, "files"   every cached page is a regular file under one directory per sstable, and
                the CQL table holds only an index row per cached page. Eviction is an
                unlink, so the space returns immediately and compaction never carries the
                payload.

Node count is the maximum of four independent constraints (NVMe, RAM, throughput and the
replication factor), each reported separately so the binding one is visible. Annual cost is
instances plus object-storage capacity plus PUT and GET requests.

This module is the reference implementation of the spreadsheet of the same name; the two are
kept in step and verified against each other to the dollar. Run it directly to print the
default scenario under both designs, or import `size_cluster` and override any parameter.

Every default here is an assumption, not a measurement. The ones that decide the answer are
called out in README.md; treat the output as a sensitivity analysis rather than a quote.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Literal

GIB = 1.073741824
"""GiB expressed in decimal GB. Instance memory is quoted in GiB, everything else in GB."""

SECONDS_PER_HOUR = 3600


@dataclass(frozen=True)
class Instance:
    """A cloud instance type, as listed in the spreadsheet's Pricing tab."""

    vcpus: int
    ram_gib: int
    nvme_gb: int
    usd_per_hour: float


# Prices are AWS us-east-1 on-demand from third-party aggregators, fetched 2026-10-28.
# They are not from the AWS price list; verify before quoting them to anyone.
INSTANCES: dict[str, Instance] = {
    "i7i.2xlarge": Instance(8, 64, 1875, 0.755),
    "i7i.4xlarge": Instance(16, 128, 3750, 1.510),
    "i7i.8xlarge": Instance(32, 256, 7500, 3.020),
    "i8g.2xlarge": Instance(8, 64, 1875, 0.6864),
    "i3en.2xlarge": Instance(8, 64, 5000, 0.904),
    "i3en.3xlarge": Instance(12, 96, 7500, 1.356),
    "i3en.6xlarge": Instance(24, 192, 15000, 2.712),
}


@dataclass(frozen=True)
class Params:
    """
    Inputs to the model. Sizes are TB unless stated; rates are per second.

    The dataset and working set are given as logical, uncompressed, single-copy sizes. The
    model derives stored bytes as logical x RF / compression, which is what object storage
    bills and what the cache must hold.
    """

    # --- cache design -------------------------------------------------------------
    design: Literal["tablet", "files"] = "files"

    # --- workload -----------------------------------------------------------------
    logical_tb: float = 50.0
    """Cold dataset, logical and uncompressed, single copy."""
    logical_ws_tb: float = 2.0
    """Active working set, logical and uncompressed, single copy."""
    rf: int = 3
    compression: float = 2.0
    """Uncompressed bytes per stored byte. 2 is a typical LZ4 ratio on sstable chunks."""
    read_ops: int = 150_000
    write_ops: int = 20_000
    write_size_b: int = 1024

    # --- cache behaviour ----------------------------------------------------------
    data_hit_rate: float = 0.90
    """
    Probability that a read's data-component range is served entirely from NVMe.

    This is per read, not per page. The implementation looks up a range's pages in batches
    and, at the first uncached page, fetches the rest of the range in a single request, so
    the request count turns on how often a range misses at all, not on how many of its pages
    miss. PR #32563 measured 0.34 requests per read with a cache one fifth the size of the
    data under Zipf point reads, and 1.16 with no cache; these defaults imply 0.10 requests
    per read per replica at a cache sized for a 90% hit rate.
    """
    index_hit_rate: float = 1.00
    """
    Probability that a read's index-component range is served entirely from NVMe.

    The index is a few percent of the data, so the working-set index is small, dense and
    heavily reused and is assumed to stay resident. This holds only if the hot rows cluster
    in key space: scattered across the whole keyspace, the index pages touched also cover
    cold rows and the resident index approaches `sstable_index_share` of the entire dataset.
    """
    sstable_index_share: float = 0.04
    """Sstable index plus summary as a fraction of the data component, typically 3-5%."""
    page_kib: int = 4
    """
    Page cache granularity, as in PR #32563: a compressed sstable chunk (4 KiB uncompressed
    by default), a 4 KiB encryption block, or a 4 KiB page for other components.

    Page size sets how many bytes a miss pulls into the cache, not how many requests it
    costs, since a miss fetches the rest of its range in a single request either way.
    """
    data_pages_per_read: int = 4
    """Data-component pages a read touches. Sets the bytes a miss brings back, not requests."""
    index_pages_per_read: int = 2
    """Index-component pages a read touches. Same role: fill bytes, not request count."""
    replicas_per_read: int = 2
    """2 is QUORUM at RF=3: one data and one digest replica, both of which read the data."""

    # --- writes -------------------------------------------------------------------
    write_allocate_fraction: float = 0.0
    """
    Fraction of newly flushed data inserted into the cache on write.

    Zero in PR #32563: only user reads populate the cache, and memtables flush straight to
    object storage. Compaction, repair, streaming and view building read cached pages but
    add none. Keeping new sstables local is listed there as future work, so this is modelled
    but off by default.
    """
    hot_hours_after_write: float = 3.0
    """
    How long newly written data stays resident before falling out of the cache.

    If the per-hour access probability halves each hour, the mean age at last access is two
    hours, so three is slightly conservative. Set `write_allocate_fraction` to 0 if the
    working-set input already accounts for recent writes, to avoid double counting.
    """
    compaction_write_amp: float = 4.0
    """Bytes written to object storage per byte ingested, over the data lifetime."""

    # --- space --------------------------------------------------------------------
    cache_space_amplification: float = 1.5
    """
    Disk the cache's CQL table occupies per byte of live cached data.

    A configured target, not an emergent property: the `pagecache` table uses incremental
    compaction with a space amplification goal of 1.5, and PR #32563 notes real disk usage is
    about 1.5x the configured cache size. Earlier revisions of this model instead accumulated
    tombstoned pages over a retention window and produced figures of 7x and more, which were
    artefacts of treating retention as a free parameter rather than as something compaction
    targets.
    """
    object_storage_overhead: float = 1.1
    """In-flight compaction output plus lazily deleted sstables, on object storage."""
    cache_index_row_b: int = 200
    """
    Bytes per index row in the files design, one row per cached page.

    A deliberate upper bound. Against the ME-format writer, a live row keyed by
    (sstable_id, component, offset) is 15-20 bytes and an evicted page costs 40-60 bytes
    including its tombstone. The node count does not move between 20 and 1000 bytes.
    """
    payload_fs_overhead: float = 1.02
    """Block rounding and partial trailing pages. No compaction amplification applies."""

    # --- memory -------------------------------------------------------------------
    working_set_ram_ratio: float = 30.0
    """
    GB of hot data per GB of RAM, applied to the working set only.

    This is the ratio used today without tiering; it covers row cache, index caching, bloom
    filters and sstable metadata for data that is actually served. The all-NVMe baseline
    applies it to the whole dataset, since it has no tiering.
    """
    sstable_metadata_mb: float = 3.0
    """
    Memory a cold object-storage sstable needs even when nothing in it is read.

    For a 1 GB sstable the dominant terms are the bloom filter (Scylla default fp chance
    0.01, about 10 bits per partition, so roughly 1.2 MB at 1 KB partitions) and the
    compression offset table (default 4 KiB chunks, about 262,000 bit-packed entries,
    roughly 1.2 MB), plus around 0.2 MB of index summary. Both scale with partition and
    chunk count, so a larger average partition reduces this sharply. The node count is
    sensitive to this above about 5 MB.
    """
    avg_sstable_gb: float = 1.0
    """ICS default target. Drives both component PUT counts and the cold sstable count."""

    # --- throughput ---------------------------------------------------------------
    ops_per_vcpu: int = 10_000
    """ASSUMPTION, not a measurement. Calibrate from benchmarks; it is currently binding."""
    cpu_utilization: float = 0.70

    # --- object storage requests ---------------------------------------------------
    multipart_part_mib: int = 50
    """Scylla's s3::client default part size."""
    sstable_components: int = 8
    """TOC, Index, Summary, Filter, Statistics, Digest, CRC, Scylla. Data goes as parts."""
    compaction_read_mib: float = 8.0
    """
    Range size compaction uses to stream uncached sstable input from object storage.

    Deliberately decoupled from the page size: compaction reads are large and sequential,
    and a fully streamed sstable is a single GET, so 8 MiB is conservative. Setting this
    equal to the page size, which is what reading through the page cache would mean,
    multiplies compaction GETs by 64.
    """

    # --- platform and pricing -------------------------------------------------------
    instance: str = "i7i.2xlarge"
    disk_utilization: float = 0.85
    price_multiplier: float = 1.0
    """1 = on-demand; e.g. 0.6 for a one-year reservation or savings plan."""
    usd_per_gb_month: float = 0.023
    usd_per_m_put: float = 5.0
    usd_per_m_get: float = 0.4
    hours_per_year: int = 8760

    # --- all-NVMe baseline -----------------------------------------------------------
    baseline_instance: str | None = None
    """Defaults to `instance`. Set it to compare against a storage-dense type."""
    baseline_compaction_overhead: float = 1.2
    """1.2 for ICS, 2.0 for STCS."""


@dataclass(frozen=True)
class Result:
    """Everything the model derives, cluster-wide unless stated."""

    nodes: int
    binding: str
    nodes_by_nvme: int
    nodes_by_ram: int
    nodes_by_throughput: int

    cache_live_tb: float
    cache_from_writes_tb: float
    cache_footprint_tb: float
    space_amplification: float
    index_footprint_tb: float

    misses_per_s: float
    compaction_gets_per_s: float
    fill_gb_s: float
    churn_gb_s: float
    file_creates_per_s: float
    file_unlinks_per_s: float

    cold_tb: float
    cold_sstables: float
    ram_working_set_gb: float
    ram_cold_metadata_gb: float
    ram_total_gb: float

    cost_instances: float
    cost_capacity: float
    cost_put: float
    cost_get: float
    cost_total: float
    cost_per_logical_gb: float

    baseline_nodes: int
    baseline_cost: float
    savings: float
    savings_fraction: float


def size_cluster(p: Params) -> Result:
    """
    Size a cluster for `p` and cost it against the all-NVMe baseline.

    Args:
        p: The scenario to evaluate.

    Returns:
        The derived quantities, node counts per constraint and annual costs.
    """
    inst = INSTANCES[p.instance]
    page_b = p.page_kib * 1024

    # Stored bytes: what object storage bills and what the cache must hold.
    total_tb = p.logical_tb * p.rf / p.compression
    ws_tb = p.logical_ws_tb * p.rf / p.compression

    # The cache holds a fraction of the working set's data pages, a fraction of its much
    # smaller and much denser index pages, and whatever was written recently enough to
    # still be hot.
    ingest_gb_s = p.write_ops * p.write_size_b * p.rf / 1e9
    cache_from_writes_tb = ingest_gb_s * p.write_allocate_fraction * p.hot_hours_after_write * SECONDS_PER_HOUR / 1000
    cache_live_tb = ws_tb * (p.data_hit_rate + p.index_hit_rate * p.sstable_index_share) + cache_from_writes_tb
    cached_fraction = cache_live_tb / total_tb

    # Each page class misses at its own rate. Charging index lookups the data miss rate
    # roughly doubles the request bill, which is the single largest error this model has
    # made so far.
    reads_per_replica = p.read_ops * p.replicas_per_read
    data_misses_per_s = reads_per_replica * (1 - p.data_hit_rate)
    index_misses_per_s = reads_per_replica * (1 - p.index_hit_rate)
    misses_per_s = data_misses_per_s + index_misses_per_s
    miss_fill_gb_s = (data_misses_per_s * p.data_pages_per_read + index_misses_per_s * p.index_pages_per_read) * page_b / 1e9

    os_write_gb_s = ingest_gb_s * p.compaction_write_amp
    compaction_read_gb_s = ingest_gb_s * (p.compaction_write_amp - 1)
    compaction_gets_per_s = compaction_read_gb_s * (1 - cached_fraction) / (p.compaction_read_mib * 1048576 / 1e9)

    write_fill_gb_s = ingest_gb_s * p.write_allocate_fraction
    fill_gb_s = miss_fill_gb_s + write_fill_gb_s
    # Base-table compaction deleting an sstable invalidates whatever of it was cached.
    invalidation_gb_s = os_write_gb_s * cached_fraction
    churn_gb_s = fill_gb_s + invalidation_gb_s

    creates_per_s = fill_gb_s * 1e9 / page_b
    unlinks_per_s = churn_gb_s * 1e9 / page_b

    if p.design == "tablet":
        # Payload and per-page metadata both live in the CQL table, so both carry its
        # amplification.
        cache_footprint_tb = cache_live_tb * p.cache_space_amplification
        index_footprint_tb = 0.0
    else:
        # Only the index rows are compacted; the payload is unlinked on eviction. Note the
        # index is no longer negligible at a 4 KiB page: 200 bytes per page is 5% of the
        # payload, where at 128 KiB it was 0.15%.
        index_live_tb = cache_live_tb * p.cache_index_row_b / page_b
        index_footprint_tb = index_live_tb * p.cache_space_amplification
        cache_footprint_tb = cache_live_tb * p.payload_fs_overhead + index_footprint_tb

    # Scylla reserves commitlog space equal to the node's memory by default.
    commitlog_gb = inst.ram_gib * GIB
    usable_nvme_gb = inst.nvme_gb * p.disk_utilization - commitlog_gb
    nodes_by_nvme = math.ceil(cache_footprint_tb * 1000 / usable_nvme_gb)

    # Hot data needs the untiered ratio; cold data needs only per-sstable metadata, since
    # nothing reads it and it takes no row-cache footprint.
    cold_tb = max(0.0, total_tb - ws_tb)
    cold_sstables = cold_tb * 1000 * p.object_storage_overhead / p.avg_sstable_gb
    ram_working_set_gb = ws_tb * 1000 / p.working_set_ram_ratio
    ram_cold_metadata_gb = cold_sstables * p.sstable_metadata_mb / 1000
    ram_total_gb = ram_working_set_gb + ram_cold_metadata_gb
    nodes_by_ram = math.ceil(ram_total_gb / (inst.ram_gib * GIB))

    replica_ops = p.read_ops * p.replicas_per_read + p.write_ops * p.rf
    nodes_by_throughput = math.ceil(replica_ops / (inst.vcpus * p.ops_per_vcpu * p.cpu_utilization))

    nodes = max(nodes_by_nvme, nodes_by_ram, nodes_by_throughput, p.rf)
    if nodes == nodes_by_nvme:
        binding = "NVMe"
    elif nodes == nodes_by_ram:
        binding = "RAM"
    elif nodes == nodes_by_throughput:
        binding = "throughput"
    else:
        binding = "RF"

    seconds_per_year = SECONDS_PER_HOUR * p.hours_per_year
    cost_instances = nodes * inst.usd_per_hour * p.price_multiplier * p.hours_per_year
    cost_capacity = total_tb * 1000 * p.object_storage_overhead * p.usd_per_gb_month * 12
    # Sstables upload as multipart parts, plus one PUT per non-Data component.
    puts_per_year = (os_write_gb_s / (p.multipart_part_mib * 1048576 / 1e9) + os_write_gb_s / p.avg_sstable_gb * p.sstable_components) * seconds_per_year
    gets_per_year = (misses_per_s + compaction_gets_per_s) * seconds_per_year
    cost_put = puts_per_year / 1e6 * p.usd_per_m_put
    cost_get = gets_per_year / 1e6 * p.usd_per_m_get
    cost_total = cost_instances + cost_capacity + cost_put + cost_get

    baseline_nodes, baseline_cost = _size_baseline(p, total_tb, replica_ops)

    return Result(
        nodes=nodes,
        binding=binding,
        nodes_by_nvme=nodes_by_nvme,
        nodes_by_ram=nodes_by_ram,
        nodes_by_throughput=nodes_by_throughput,
        cache_live_tb=cache_live_tb,
        cache_from_writes_tb=cache_from_writes_tb,
        cache_footprint_tb=cache_footprint_tb,
        space_amplification=cache_footprint_tb / cache_live_tb,
        index_footprint_tb=index_footprint_tb,
        misses_per_s=misses_per_s,
        compaction_gets_per_s=compaction_gets_per_s,
        fill_gb_s=fill_gb_s,
        churn_gb_s=churn_gb_s,
        file_creates_per_s=creates_per_s,
        file_unlinks_per_s=unlinks_per_s,
        cold_tb=cold_tb,
        cold_sstables=cold_sstables,
        ram_working_set_gb=ram_working_set_gb,
        ram_cold_metadata_gb=ram_cold_metadata_gb,
        ram_total_gb=ram_total_gb,
        cost_instances=cost_instances,
        cost_capacity=cost_capacity,
        cost_put=cost_put,
        cost_get=cost_get,
        cost_total=cost_total,
        cost_per_logical_gb=cost_total / (p.logical_tb * 1000),
        baseline_nodes=baseline_nodes,
        baseline_cost=baseline_cost,
        savings=baseline_cost - cost_total,
        savings_fraction=(baseline_cost - cost_total) / baseline_cost,
    )


def _size_baseline(p: Params, total_tb: float, replica_ops: float) -> tuple[int, float]:
    """
    Size the same workload held entirely on local NVMe.

    Having no tiering, the baseline applies the working-set RAM ratio to the whole dataset,
    which is what makes storage-dense instance types RAM-limited rather than disk-limited.
    """
    inst = INSTANCES[p.baseline_instance or p.instance]
    data_tb = total_tb * p.baseline_compaction_overhead
    usable_nvme_gb = inst.nvme_gb * p.disk_utilization - inst.ram_gib * GIB
    nodes = max(
        math.ceil(data_tb * 1000 / usable_nvme_gb),
        math.ceil(total_tb * 1000 / p.working_set_ram_ratio / (inst.ram_gib * GIB)),
        math.ceil(replica_ops / (inst.vcpus * p.ops_per_vcpu * p.cpu_utilization)),
        p.rf,
    )
    return nodes, nodes * inst.usd_per_hour * p.price_multiplier * p.hours_per_year


def main() -> None:
    """Print the default scenario under both cache designs."""
    base = Params()
    for design in ("tablet", "files"):
        r = size_cluster(replace(base, design=design))
        label = "v1 CQL tablet" if design == "tablet" else "v2 files per sstable"
        print(f"{label}")
        print(f"  nodes            {r.nodes} ({r.binding}-bound; nvme {r.nodes_by_nvme}, ram {r.nodes_by_ram}, throughput {r.nodes_by_throughput})")
        print(f"  cache footprint  {r.cache_footprint_tb:.2f} TB for {r.cache_live_tb:.2f} TB live ({r.space_amplification:.2f}x)")
        print(f"  RAM required     {r.ram_total_gb:,.0f} GB ({r.ram_working_set_gb:,.0f} hot + {r.ram_cold_metadata_gb:,.0f} cold metadata)")
        print(f"  misses           {r.misses_per_s:,.0f}/s, churn {r.churn_gb_s:.2f} GB/s, unlinks {r.file_unlinks_per_s:,.0f}/s")
        print(f"  cost             ${r.cost_total:,.0f}/yr (instances ${r.cost_instances:,.0f}, GET ${r.cost_get:,.0f}, capacity ${r.cost_capacity:,.0f})")
        print(f"  vs all-NVMe      {r.baseline_nodes} nodes, ${r.baseline_cost:,.0f}/yr -> {r.savings_fraction * 100:+.1f}%")
        print()


if __name__ == "__main__":
    main()
