#!/usr/bin/env python3
"""Plot Cress hold curves: extra messages from the busiest sender against hold length.

    cress_hold_plot.py OUT_PREFIX "title" SOURCE_RS LABEL=REPORT.txt [LABEL=REPORT.txt ...]

Reads Cress report files (the `reports/*.txt` that `scripts/cress` writes) and, from each, the
curve of the decision point whose `use::batch(acks, ...)` line in SOURCE_RS matches the curve's
`file:line`. Plots "extra messages, busiest sender" against "hold length (rounds)" on log-log
axes (zero values are not drawable on a log axis and are left out). Writes OUT_PREFIX.svg and,
with ImageMagick, OUT_PREFIX.png. No third-party Python packages.
"""

import math
import re
import subprocess
import sys

COLORS = ["#d62728", "#1f77b4", "#2ca02c", "#9467bd", "#ff7f0e", "#8c564b", "#e377c2", "#17becf"]


def acks_lines(source_rs):
    lines = set()
    with open(source_rs) as f:
        for n, line in enumerate(f, 1):
            if "use::batch(acks," in line:
                lines.add(n)
    if not lines:
        raise SystemExit(f"no `use::batch(acks,` in {source_rs}")
    return lines


def parse(report, lines):
    holds = None
    curve = None
    current = None
    with open(report) as f:
        for raw in f:
            line = raw.rstrip("\n")
            if holds is None and line.startswith("hold length (rounds)"):
                holds = [int(x) for x in line.split()[3:]]
                continue
            m = re.match(r"^\S*reliable_send\.rs:(\d+), batch of ", line)
            if m:
                current = int(m.group(1))
                continue
            if current in lines and line.strip().startswith("extra messages, busiest sender"):
                vals = [int(x) for x in line.split()[4:]]
                curve = vals
                break
    if holds is None or curve is None:
        raise SystemExit(f"{report}: no hold header or no acks curve on lines {sorted(lines)}")
    if len(holds) != len(curve):
        raise SystemExit(f"{report}: {len(holds)} holds but {len(curve)} values")
    return holds, curve


def render(out_prefix, title, series):
    W, H = 980, 560
    L, R, T, B = 90, 310, 60, 70
    pw, ph = W - L - R, H - T - B
    xs = [h for _, holds, _ in series for h in holds]
    ys = [v for _, _, vals in series for v in vals if v > 0]
    x0, x1 = math.log10(min(xs)), math.log10(max(xs))
    y0, y1 = 0.0, math.ceil(math.log10(max(ys)))
    X = lambda h: L + (math.log10(h) - x0) / (x1 - x0) * pw
    Y = lambda v: T + ph - (math.log10(v) - y0) / (y1 - y0) * ph
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="Helvetica,Arial,sans-serif">',
        f'<rect width="{W}" height="{H}" fill="white"/>',
        f'<text x="{L}" y="30" font-size="17" font-weight="bold">{title}</text>',
        f'<rect x="{L}" y="{T}" width="{pw}" height="{ph}" fill="none" stroke="#888"/>',
    ]
    for e in range(int(y0), int(y1) + 1):
        y = Y(10**e)
        out.append(f'<line x1="{L}" y1="{y:.1f}" x2="{L + pw}" y2="{y:.1f}" stroke="#e5e5e5"/>')
        out.append(f'<text x="{L - 8}" y="{y + 4:.1f}" font-size="12" text-anchor="end">{10**e:,}</text>')
    for h in sorted(set(xs)):
        x = X(h)
        out.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{T + ph}" stroke="#f0f0f0"/>')
        out.append(f'<text x="{x:.1f}" y="{T + ph + 18}" font-size="12" text-anchor="middle">{h}</text>')
    out.append(f'<text x="{L + pw / 2}" y="{H - 22}" font-size="13" text-anchor="middle">hold length of the acknowledgement batch (rounds)</text>')
    out.append(f'<text x="22" y="{T + ph / 2}" font-size="13" text-anchor="middle" transform="rotate(-90 22 {T + ph / 2})">extra sender messages over the unheld run</text>')
    for i, (label, holds, vals) in enumerate(series):
        c = COLORS[i % len(COLORS)]
        same = next((l for l, h, v in series[:i] if (h, v) == (holds, vals)), None)
        dash = ' stroke-dasharray="7,5"' if same else ""
        if same:
            label = f"{label} (identical to {same})"
        pts = [(X(h), Y(v)) for h, v in zip(holds, vals) if v > 0]
        if pts:
            path = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            out.append(f'<polyline points="{path}" fill="none" stroke="{c}" stroke-width="2.2"{dash}/>')
            for x, y in pts:
                out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{c}"/>')
        ly = T + 18 + i * 22
        out.append(f'<line x1="{L + pw + 15}" y1="{ly - 4}" x2="{L + pw + 40}" y2="{ly - 4}" stroke="{c}" stroke-width="2.2"{dash}/>')
        out.append(f'<text x="{L + pw + 46}" y="{ly}" font-size="13">{label}</text>')
    out.append(f'<text x="{L}" y="{H - 4}" font-size="11" fill="#666">Every hold begins at round 1; input in rounds 0..125. Zero extra work is not drawn (log axis).</text>')
    out.append("</svg>")
    with open(out_prefix + ".svg", "w") as f:
        f.write("\n".join(out))
    subprocess.run(["convert", "-density", "110", out_prefix + ".svg", out_prefix + ".png"], check=True)


def main():
    if len(sys.argv) < 5:
        print(__doc__, file=sys.stderr)
        raise SystemExit(2)
    out_prefix, title, source_rs = sys.argv[1:4]
    lines = acks_lines(source_rs)
    series = []
    for arg in sys.argv[4:]:
        label, report = arg.rsplit("=", 1)
        holds, vals = parse(report, lines)
        series.append((label, holds, vals))
        print(f"{label}: " + " ".join(f"{h}:{v}" for h, v in zip(holds, vals)))
    render(out_prefix, title, series)


if __name__ == "__main__":
    main()
