# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Sensitivity sweeps over the object-storage NVMe cache cost model.

The model's value is not any single cost figure but which parameter the answer is sensitive
to. This script varies one parameter at a time around the defaults in `model.Params` and
prints the result for both cache designs side by side.

Run it with no arguments for every sweep, or name the ones you want:

    python3 sweep.py
    python3 sweep.py hit_rate page_size

Add a sweep by appending to SWEEPS: a name, a label format and the overrides to apply.
"""

from __future__ import annotations

import sys
from dataclasses import replace

from model import Params, Result, size_cluster

Overrides = dict[str, object]

SWEEPS: dict[str, tuple[str, list[tuple[str, Overrides]]]] = {
    "design": (
        "Cache design at the defaults",
        [("defaults", {})],
    ),
    "sstable_metadata": (
        "Per-sstable resident metadata, which sets the cold-data RAM",
        [(f"{m} MB per sstable", {"sstable_metadata_mb": m}) for m in (1, 2, 3, 5, 10)],
    ),
    "ops_per_vcpu": (
        "Sustainable ops/s per vCPU, currently the binding constraint",
        [(f"{o:,} ops/vCPU", {"ops_per_vcpu": o}) for o in (10_000, 15_000, 20_000, 30_000)],
    ),
    "index_hit_rate": (
        "Sstable index page hit rate, with the data hit rate held at 90%",
        [(f"index hit {h:.0%}", {"index_hit_rate": h}) for h in (0.90, 0.95, 0.99, 1.00)],
    ),
    "hit_rate": (
        "Data page hit rate, with the index held resident",
        [(f"data hit {h:.0%}", {"data_hit_rate": h}) for h in (0.80, 0.90, 0.95, 0.99)],
    ),
    "replicas": (
        "Replicas contacted per read",
        [(f"{n} replica(s) per read", {"replicas_per_read": n}) for n in (1, 2, 3)],
    ),
    "read_rate": (
        "Read workload",
        [(f"{q:,} reads/s", {"read_ops": q}) for q in (10_000, 50_000, 150_000, 500_000)],
    ),
    "cold_data": (
        "Cold dataset size, with the working set held at 2 TB",
        [(f"{t} TB logical", {"logical_tb": t}) for t in (50, 100, 200, 500, 1000)],
    ),
    "page_size": (
        "Cache page size",
        [(f"page {p} KiB", {"page_kib": p}) for p in (4, 8, 16, 64)],
    ),
    "amplification": (
        "Space amplification goal of the cache's CQL table",
        [(f"ICS goal {a}x", {"cache_space_amplification": a}) for a in (1.2, 1.5, 2.0, 3.0)],
    ),
    "index_row": (
        "Cache index row size, which is 5% of a 4 KiB page rather than 0.15% of 128 KiB",
        [(f"index row {b} B", {"cache_index_row_b": b}) for b in (20, 45, 200, 500)],
    ),
    "compression": (
        "Compression ratio",
        [(f"compression {c}x", {"compression": c}) for c in (1, 2, 3, 4)],
    ),
    "working_set": (
        "Working set size, with the dataset held at 50 TB",
        [(f"working set {w} TB", {"logical_ws_tb": w}) for w in (0.5, 2, 5, 10)],
    ),
    "baseline": (
        "All-NVMe baseline instance type",
        [(f"baseline {b}", {"baseline_instance": b}) for b in ("i7i.2xlarge", "i3en.2xlarge", "i3en.6xlarge")],
    ),
    "combined": (
        "Combined read-path levers",
        [
            ("defaults", {}),
            ("1 replica per read", {"replicas_per_read": 1}),
            ("99% data hit", {"data_hit_rate": 0.99}),
            ("both", {"replicas_per_read": 1, "data_hit_rate": 0.99}),
            ("both, 500 TB cold", {"replicas_per_read": 1, "data_hit_rate": 0.99, "logical_tb": 500}),
        ],
    ),
}

HEADER = f"{'Configuration':<28} {'design':<7} {'nodes':>5} {'binding':<11} {'cache TB':>9} {'amp':>5} {'RAM GB':>7} {'total $/yr':>12} {'vs NVMe':>8}"


def _line(label: str, design: str, r: Result) -> str:
    tag = "v1" if design == "tablet" else "v2"
    return (
        f"{label:<28} {tag:<7} {r.nodes:>5} {r.binding:<11} {r.cache_footprint_tb:>9.2f} "
        f"{r.space_amplification:>5.2f} {r.ram_total_gb:>7,.0f} {r.cost_total:>12,.0f} {r.savings_fraction * 100:>7.1f}%"
    )


def run_sweep(name: str, base: Params) -> None:
    """Print one named sweep for both cache designs."""
    title, cases = SWEEPS[name]
    print(f"\n{title}")
    print(HEADER)
    for label, overrides in cases:
        for design in ("tablet", "files"):
            print(_line(label, design, size_cluster(replace(base, design=design, **overrides))))


def main(argv: list[str]) -> int:
    base = Params()
    names = argv[1:] or list(SWEEPS)
    unknown = [n for n in names if n not in SWEEPS]
    if unknown:
        print(f"unknown sweep(s): {', '.join(unknown)}", file=sys.stderr)
        print(f"available: {', '.join(SWEEPS)}", file=sys.stderr)
        return 1
    for name in names:
        run_sweep(name, base)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
