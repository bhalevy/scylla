# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
What scaling the cluster out buys, when the cache is sized by the hardware.

The rest of this model works forwards: pick a target hit rate, size the cache to meet it,
and see how many nodes that needs. At the customer scenario that gives 7 nodes, and the
binding constraint is throughput, not disk. Those 7 nodes carry about 10.7 TB of usable NVMe
while a cache sized for 90% asks for 3.1 TB, so most of the disk sits idle.

This script works the other way round, which is what the implementation actually does: pin
the cluster at n nodes, give the cache every byte of NVMe those nodes have, and read the hit
rate off `model.hit_rate_for`. More nodes mean more cache, a higher hit rate and fewer GETs,
against a higher instance bill, so there is a cost optimum rather than a constraint.

The answer turns almost entirely on `tail_exponent`, the concavity of the hit-rate curve
over the cold tail, which nobody has measured. The script therefore reports a band over
three values of it rather than a number, and the band is wide.

    python3 scaleout.py              # the table, both designs
    python3 scaleout.py --svg DIR    # also write the two chart panels
"""

from __future__ import annotations

import sys
from dataclasses import replace

from model import Params, Result, cache_amplification, size_cluster, usable_nvme_gb

TAILS: list[tuple[float, str]] = [
    (1.0, "uniform tail"),
    (0.5, "midpoint"),
    (0.215, "PR 32563 Zipf"),
]
"""Tail exponents to sweep, pessimistic first. See `Params.tail_exponent` for provenance."""

NODE_RANGE = range(7, 61)
"""From the throughput-bound floor to the size of the all-NVMe baseline."""


def at_nodes(p: Params, nodes: int) -> Result:
    """
    Cost the scenario pinned at `nodes`, with the cache filling those nodes' NVMe.

    Args:
        p: The scenario. Its `cache_live_tb` and `nodes_floor` are overridden.
        nodes: Cluster size to pin.

    Returns:
        The sized and costed result, whose `nodes` equals `nodes`.
    """
    total_tb = p.logical_tb * p.rf / p.compression
    # The 1e-9 shave keeps the derived footprint from rounding up to one node more than the
    # capacity it was computed from.
    live_tb = min(total_tb, nodes * usable_nvme_gb(p) / 1000 / cache_amplification(p) * (1 - 1e-9))
    r = size_cluster(replace(p, cache_live_tb=live_tb, nodes_floor=nodes))
    assert r.nodes == nodes, f"pinned at {nodes} but sized to {r.nodes}"
    return r


def curve(p: Params, tail: float) -> list[tuple[int, Result]]:
    """The whole node range at one tail exponent."""
    q = replace(p, tail_exponent=tail)
    return [(n, at_nodes(q, n)) for n in NODE_RANGE]


def _print(p: Params) -> None:
    base = size_cluster(p)
    print(f"\nScale-out with the cache filling the disk, {p.design} design")
    print(f"Sized forwards, this is {base.nodes} nodes at a {p.data_hit_rate:.0%} target and "
          f"${base.cost_total:,.0f}; all-NVMe is {base.baseline_nodes} nodes and ${base.baseline_cost:,.0f}.")
    print(f"{'nodes':>5} {'live TB':>8}", end="")
    for tail, label in TAILS:
        print(f" | {label + ' hit':>20} {'$/yr':>10} {'vs NVMe':>8}", end="")
    print()
    rows = {tail: dict(curve(p, tail)) for tail, _ in TAILS}
    for n in NODE_RANGE:
        if n > 12 and n % 4 and n != NODE_RANGE[-1]:
            continue
        first = rows[TAILS[0][0]][n]
        print(f"{n:>5} {first.cache_live_tb:>8.1f}", end="")
        for tail, _ in TAILS:
            r = rows[tail][n]
            print(f" | {r.data_hit_rate:>20.1%} {r.cost_total:>10,.0f} {r.savings_fraction * 100:>7.1f}%", end="")
        print()
    for tail, label in TAILS:
        best = min(rows[tail].values(), key=lambda r: r.cost_total)
        print(f"  optimum, {label:<14} {best.nodes:>3} nodes, hit {best.data_hit_rate:.1%}, "
              f"${best.cost_total:,.0f}, {best.savings_fraction * 100:+.1f}% vs all-NVMe")


def main(argv: list[str]) -> int:
    base = Params()
    for design in ("tablet", "files"):
        _print(replace(base, design=design))
    print()
    if "--svg" in argv:
        import chart
        chart.write(argv[argv.index("--svg") + 1], base)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
