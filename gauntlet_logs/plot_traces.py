#!/usr/bin/env python3
"""Render Sedge per-second traces as Unicode timeseries charts.

Each trial chart shows offered rps, completed rps (goodput), and backlog
over the trial's seconds, with the trigger window marked.
"""
import csv
import sys
from pathlib import Path

BLOCKS = " ▁▂▃▄▅▆▇█"


def sparkline(values, lo=None, hi=None):
    lo = min(values) if lo is None else lo
    hi = max(values) if hi is None else hi
    span = (hi - lo) or 1
    return "".join(BLOCKS[round((v - lo) / span * (len(BLOCKS) - 1))] for v in values)


def chart(rows, series, title, height=10, trigger=(10, 15)):
    """Multi-line bar chart of one series with axis labels."""
    vals = [float(r[series]) for r in rows]
    hi = max(vals) or 1
    lines = []
    for level in range(height, 0, -1):
        cut = hi * (level - 0.5) / height
        row = "".join("█" if v >= cut else " " for v in vals)
        label = f"{hi * level / height:>8.0f} ┤" if level in (height, height // 2, 1) else "         │"
        lines.append(label + row)
    lines.append("       0 └" + "─" * len(vals))
    # time axis: tick every 10 s
    axis = "          "
    for i in range(len(vals)):
        axis += str(i // 10 % 10) if i % 10 == 0 else " "
    lines.append(axis + "   (seconds ×10)")
    # trigger marker
    marker = "          " + "".join(
        "^" if trigger[0] <= i < trigger[1] else " " for i in range(len(vals)))
    lines.insert(0, f"  {title}  (max {hi:.0f})")
    lines.append(marker + "   ^ = trigger window")
    return "\n".join(lines)


def summarize(path, trigger=(10, 15)):
    rows = list(csv.DictReader(open(path), delimiter="\t"))
    name = Path(path).stem.replace("hydro_test.rpc_with_retries.", "")
    out = [f"\n{'=' * 72}", f"TRIAL: {name}", "=" * 72]
    for series, title in [
        ("offered", "offered rps"),
        ("completed", "completed rps (goodput)"),
        ("backlog", "backlog (server queue)"),
        ("retried", "retries sent /s"),
    ]:
        if series in rows[0]:
            out.append(chart(rows, series, title, trigger=trigger))
            out.append("")
    return "\n".join(out)


if __name__ == "__main__":
    for p in sys.argv[1:]:
        print(summarize(p))
