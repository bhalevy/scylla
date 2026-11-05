# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""
Draws the scale-out curves as an SVG, for the slide that carries them.

Two panels sharing one x axis, the cluster size: hit rate above, annual cost below. One
series per `scaleout.TAILS` entry, because the tail exponent is the whole question. The cost
panel carries the all-NVMe bill as a reference line and marks both the forward-sized 7-node
point and each series' optimum.

Generated rather than hand-placed, so the geometry follows the numbers. Written by
`scaleout.py --svg <dir>`.
"""

from __future__ import annotations

from dataclasses import replace

from model import Params, size_cluster
from scaleout import NODE_RANGE, TAILS, curve

INK = "#101225"
MUTED = "#465271"
GRID = "#d1dff3"
SERIES = ["#3c4f97", "#dd7900", "#00a0d7"]
"""Fixed assignment, pessimistic tail first; never cycled, never reordered."""

W, H = 1664, 510
X0, X1 = 170, 1398
PANELS = {"hit": (70, 212), "cost": (284, 436)}


def _x(n: int) -> float:
    lo, hi = NODE_RANGE[0], NODE_RANGE[-1]
    return X0 + (n - lo) / (hi - lo) * (X1 - X0)


def _y(panel: str, v: float, lo: float, hi: float) -> float:
    top, bottom = PANELS[panel]
    return bottom - (v - lo) / (hi - lo) * (bottom - top)


def _poly(points: list[tuple[float, float]]) -> str:
    return " ".join(f"{x:.1f},{y:.1f}" for x, y in points)


def _text(x: float, y: float, s: str, size: int = 24, fill: str = MUTED, anchor: str = "start", weight: int = 400) -> str:
    return (f'<text x="{x:.0f}" y="{y:.0f}" font-size="{size}" fill="{fill}" text-anchor="{anchor}" '
            f'font-weight="{weight}" font-family="Roboto Flex, Arial, sans-serif">{s}</text>')


def build(p: Params) -> str:
    """Return the SVG for scenario `p`, one series per tail exponent."""
    data = {tail: dict(curve(p, tail)) for tail, _ in TAILS}
    baseline = size_cluster(p).baseline_cost
    forward = size_cluster(p)

    hit_lo, hit_hi = 0.90, 1.00
    cost_lo, cost_hi = 200_000, 500_000

    out: list[str] = [f'<svg viewBox="0 0 {W} {H}" aria-label="Hit rate and annual cost against cluster size, '
                      f'for three assumptions about how concentrated reads of the cold tail are">']

    # Legend.
    lx = X0
    for (tail, label), colour in zip(TAILS, SERIES):
        out.append(f'<rect x="{lx}" y="10" width="28" height="6" rx="3" fill="{colour}"/>')
        out.append(_text(lx + 40, 24, f"{label} (exponent {tail:g})", 24, MUTED))
        lx += 400

    for panel, (lo, hi, fmt, title) in {
        "hit": (hit_lo, hit_hi, lambda v: f"{v * 100:g}%", "Data-range hit rate"),
        "cost": (cost_lo, cost_hi, lambda v: f"${v / 1000:,.0f}K", "Annual cost"),
    }.items():
        top, bottom = PANELS[panel]
        out.append(_text(X0, top - 16, title, 26, INK, weight=600))
        for i in range(5):
            v = lo + (hi - lo) * i / 4
            y = _y(panel, v, lo, hi)
            out.append(f'<line x1="{X0}" y1="{y:.1f}" x2="{X1}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
            out.append(_text(X0 - 16, y + 8, fmt(v), 24, MUTED, anchor="end"))

    # The all-NVMe reference line.
    y_base = _y("cost", baseline, cost_lo, cost_hi)
    out.append(f'<line x1="{X0}" y1="{y_base:.1f}" x2="{X1}" y2="{y_base:.1f}" stroke="{MUTED}" '
               f'stroke-width="2" stroke-dasharray="10 8"/>')
    out.append(_text(X1 + 12, y_base + 8, "all-NVMe", 24, MUTED))

    # Series. Direct labels go on the hit panel at a node count where the lines are well
    # separated, rather than at the right end, where they converge within a few pixels.
    label_at = 26
    for (tail, label), colour in zip(TAILS, SERIES):
        rows = data[tail]
        for panel, pick, lo, hi in (("hit", lambda r: r.data_hit_rate, hit_lo, hit_hi),
                                    ("cost", lambda r: r.cost_total, cost_lo, cost_hi)):
            pts = [(_x(n), _y(panel, min(hi, max(lo, pick(rows[n]))), lo, hi)) for n in NODE_RANGE]
            out.append(f'<polyline points="{_poly(pts)}" fill="none" stroke="{colour}" stroke-width="2"/>')
        out.append(_text(_x(label_at), _y("hit", rows[label_at].data_hit_rate, hit_lo, hit_hi) - 12,
                         label, 24, MUTED))
        # Only an interior optimum is worth marking; at the left edge there is no optimum,
        # the curve simply never turns, which is the finding rather than a point on it.
        # Mark an interior optimum with a dot only. The figures go in a row on the slide:
        # on a panel this short there is no placement for three captions that does not
        # collide with a curve or with each other.
        best = min(rows.values(), key=lambda r: r.cost_total)
        if best.nodes > NODE_RANGE[0]:
            bx, by = _x(best.nodes), _y("cost", best.cost_total, cost_lo, cost_hi)
            out.append(f'<circle cx="{bx:.1f}" cy="{by:.1f}" r="7" fill="{colour}"/>')

    # Where the deck's headline sits today, annotated clear of the curves.
    fx, fy = _x(7), _y("cost", forward.cost_total, cost_lo, cost_hi)
    out.append(f'<circle cx="{fx:.1f}" cy="{fy:.1f}" r="7" fill="none" stroke="{INK}" stroke-width="2"/>')
    out.append(_text(X1, PANELS["cost"][0] - 16, "\u25cb  cache capped at the 90% target, $449K", 24, INK, anchor="end"))

    # x axis.
    out.append(f'<line x1="{X0}" y1="{PANELS["cost"][1]}" x2="{X1}" y2="{PANELS["cost"][1]}" '
               f'stroke="{MUTED}" stroke-width="2"/>')
    for n in (7, 10, 20, 30, 40, 50, 60):
        out.append(_text(_x(n), PANELS["cost"][1] + 32, str(n), 24, MUTED, anchor="middle"))
    out.append(_text((X0 + X1) / 2, PANELS["cost"][1] + 64, "Cluster size, nodes", 24, MUTED, anchor="middle"))
    out.append("</svg>")
    return "".join(out)


def write(directory: str, p: Params) -> None:
    """Write the chart for the implemented design into `directory`."""
    import pathlib
    svg = build(replace(p, design="tablet"))
    path = pathlib.Path(directory) / "scaleout.svg"
    path.write_text(svg)
    print(f"wrote {path} ({len(svg):,} bytes)")
