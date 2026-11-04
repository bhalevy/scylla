# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Two degenerate scenarios that bracket what the object-storage page cache can be worth.

Neither is considered a typical workload. They are the corners of the envelope: the first
removes reads entirely, so object storage is charged for almost nothing, and the second gives
the cache a working set that fits with room to spare and is read at a perfect hit rate. The
customer scenario in `model.Params` sits between them, and the point of printing all three is
that the spread is enormous and decided almost entirely by the read path.

Each case is costed under three setups:

  NVMe only     the all-NVMe baseline, which `model.size_cluster` already computes.
  S3 only       the dataset on object storage with no NVMe cache at all, expressed as
                hit rates of zero: nothing is cached, so every range read is a GET.
  page cache    object storage behind the NVMe cache, under both cache designs.

Run it directly, optionally naming cases:

    python3 edge_cases.py
    python3 edge_cases.py write_only
"""

from __future__ import annotations

import sys
from dataclasses import replace

from model import Params, Result, size_cluster

CASES: dict[str, tuple[str, Params, str]] = {
    "write_only": (
        "Write only: 50 TB logical, 20K writes/s, no reads",
        Params(read_ops=0, logical_ws_tb=0.0),
        "No reads means no working set, so the cache stays empty and costs nothing. What "
        "sizes the cluster is RAM for per-sstable metadata over 75 TB of stored data.",
    ),
    "perfect_fit": (
        "Perfect fit: 20 TB logical, 2 TB working set, 100K reads/s, no writes",
        Params(
            logical_tb=20.0,
            logical_ws_tb=2.0,
            read_ops=100_000,
            write_ops=0,
            data_hit_rate=1.0,
            index_hit_rate=1.0,
        ),
        "Every read is served from NVMe, so object storage is billed for capacity alone. "
        "With no writes there is no compaction and no PUT traffic either.",
    ),
}

HEADER = (
    f"{'Setup':<22} {'nodes':>5} {'binding':<11} {'GET/s':>9} "
    f"{'instances':>11} {'requests':>11} {'capacity':>10} {'total $/yr':>12} {'vs NVMe':>9}"
)


def _line(label: str, nodes: int, binding: str, gets: float, inst: float, req: float, cap: float, baseline: float) -> str:
    total = inst + req + cap
    return (
        f"{label:<22} {nodes:>5} {binding:<11} {gets:>9,.0f} {inst:>11,.0f} {req:>11,.0f} "
        f"{cap:>10,.0f} {total:>12,.0f} {(baseline - total) / baseline * 100:>8.1f}%"
    )


def run_case(name: str) -> None:
    """Print one edge case under all three setups."""
    title, p, note = CASES[name]
    print(f"\n{title}\n{note}")
    print(HEADER)

    cached: Result = size_cluster(p)
    baseline = cached.baseline_cost
    print(_line("NVMe only", cached.baseline_nodes, cached.baseline_binding, 0.0, baseline, 0.0, 0.0, baseline))

    # No cache at all: every data and index range read goes to object storage.
    bare = size_cluster(replace(p, data_hit_rate=0.0, index_hit_rate=0.0))
    print(_line("S3 only, no cache", bare.nodes, bare.binding, bare.misses_per_s + bare.compaction_gets_per_s,
                bare.cost_instances, bare.cost_get + bare.cost_put, bare.cost_capacity, baseline))

    for design in ("tablet", "files"):
        r = size_cluster(replace(p, design=design))
        tag = "page cache, v1" if design == "tablet" else "page cache, v2"
        print(_line(tag, r.nodes, r.binding, r.misses_per_s + r.compaction_gets_per_s,
                    r.cost_instances, r.cost_get + r.cost_put, r.cost_capacity, baseline))


def main(argv: list[str]) -> int:
    names = argv[1:] or list(CASES)
    unknown = [n for n in names if n not in CASES]
    if unknown:
        print(f"unknown case(s): {', '.join(unknown)}", file=sys.stderr)
        print(f"available: {', '.join(CASES)}", file=sys.stderr)
        return 1
    for name in names:
        run_case(name)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
