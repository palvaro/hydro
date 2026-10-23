#!/usr/bin/env python3
"""Render a Sedge per-second trace TSV (generic round-based format) as a stacked SVG chart.

No third-party dependencies. Columns understood:
  second, rounds, offered, completed, abandoned, p50_ms, p99_ms
  <stream>_n   records per second on an output stream
  <stream>_v   latest value of a level (gauge) stream
Any other numeric column is ignored.

    sedge_plot.py TRACE.tsv OUT.svg "title" [--trigger 10,15] [--tail-window 37,60] [--rates a,b] [--dip]

--tail-window: the tail verdict window to shade (default [37,60); with `--tail 60` it is [45,75)).
--rates: streams that carry `_v` columns but are not levels (for example a stream of `u64` ids
that Cress classified as conserved); they are drawn with the record rates instead.
Convert to PNG with ImageMagick: convert -density 110 OUT.svg OUT.png
"""
import csv
import math
import sys

W, H_PANEL, PAD_L, PAD_R, PAD_T, PAD_B, GAP = 1060, 190, 70, 230, 44, 30, 44
COLORS = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#8c564b", "#17becf", "#7f7f7f", "#e377c2"]


def num(s):
    try:
        v = float(s)
        return v if not math.isnan(v) else None
    except (ValueError, TypeError):
        return None


def load(path):
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    return {k: [num(r.get(k, "")) for r in rows] for k in rows[0].keys()}


def panel(out, y0, xs, series, title, ylabel, log=False, shade=None):
    x_lo, x_hi = min(xs), max(xs)
    vals = [v for _, vs in series for v in vs if v is not None and (not log or v > 0)]
    if not vals:
        vals = [0, 1]
    if log:
        y_lo = math.floor(math.log10(max(min(vals), 1e-3)))
        y_hi = math.ceil(math.log10(max(vals))) if max(vals) > 10 ** math.floor(math.log10(max(vals))) else math.log10(max(vals)) + 1
        if y_hi <= y_lo:
            y_hi = y_lo + 1
    else:
        y_lo, y_hi = 0, (max(vals) * 1.08) or 1
    pw, ph = W - PAD_L - PAD_R, H_PANEL
    sx = lambda x: PAD_L + (x - x_lo) / (x_hi - x_lo) * pw

    def sy(v):
        if log:
            v = math.log10(v) if v > 0 else y_lo
        return y0 + ph - (v - y_lo) / (y_hi - y_lo) * ph

    out.append(f'<text x="{PAD_L}" y="{y0 - 8}" font-size="14" font-weight="bold">{title}</text>')
    if shade:
        out.append(f'<rect x="{sx(shade[0]):.1f}" y="{y0}" width="{sx(shade[1]) - sx(shade[0]):.1f}" height="{ph}" fill="#f4c7c3" opacity="0.5"/>')
    # verdict windows: baseline [5,10) and the tail window
    for a, b in ((5, 10), TAIL_WINDOW):
        if a >= x_lo and b <= x_hi + 1:
            out.append(f'<rect x="{sx(a):.1f}" y="{y0}" width="{sx(min(b, x_hi)) - sx(a):.1f}" height="{ph}" fill="#cfe8cf" opacity="0.35"/>')
    out.append(f'<rect x="{PAD_L}" y="{y0}" width="{pw}" height="{ph}" fill="none" stroke="#888"/>')
    if log:
        ticks = [10 ** e for e in range(int(y_lo), int(math.ceil(y_hi)) + 1)]
    else:
        step = 10 ** math.floor(math.log10(y_hi - y_lo)) if y_hi > y_lo else 1
        if (y_hi - y_lo) / step < 3:
            step /= 2
        ticks = [y_lo + i * step for i in range(int((y_hi - y_lo) / step) + 1)]
    for t in ticks:
        y = sy(t)
        out.append(f'<line x1="{PAD_L}" x2="{PAD_L + pw}" y1="{y:.1f}" y2="{y:.1f}" stroke="#ddd"/>')
        out.append(f'<text x="{PAD_L - 6}" y="{y + 4:.1f}" font-size="11" text-anchor="end">{t:g}</text>')
    out.append(f'<text x="{PAD_L - 52}" y="{y0 + ph / 2}" font-size="11" text-anchor="middle" transform="rotate(-90 {PAD_L - 52} {y0 + ph / 2})">{ylabel}</text>')
    for t in range(int(x_lo), int(x_hi) + 1, 5):
        out.append(f'<line x1="{sx(t):.1f}" x2="{sx(t):.1f}" y1="{y0 + ph}" y2="{y0 + ph + 4}" stroke="#888"/>')
        out.append(f'<text x="{sx(t):.1f}" y="{y0 + ph + 16}" font-size="11" text-anchor="middle">{t}</text>')
    for i, (name, vs) in enumerate(series):
        color = COLORS[i % len(COLORS)]
        # break the line wherever a sample is missing (or zero on a log axis)
        segs, cur = [], []
        for x, v in zip(xs, vs):
            if v is None or (log and v <= 0):
                if cur:
                    segs.append(cur)
                cur = []
            else:
                cur.append((sx(x), sy(v)))
        if cur:
            segs.append(cur)
        for pts in segs:
            if len(pts) == 1:
                out.append(f'<circle cx="{pts[0][0]:.1f}" cy="{pts[0][1]:.1f}" r="2.5" fill="{color}"/>')
            else:
                d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
                out.append(f'<polyline points="{d}" fill="none" stroke="{color}" stroke-width="2"/>')
        lx, ly = PAD_L + pw + 12, y0 + 14 + i * 18
        out.append(f'<line x1="{lx}" x2="{lx + 22}" y1="{ly}" y2="{ly}" stroke="{color}" stroke-width="2"/>')
        out.append(f'<text x="{lx + 28}" y="{ly + 4}" font-size="12">{name}</text>')


TAIL_WINDOW = (37, 60)


def main(argv):
    global TAIL_WINDOW
    path, out_path, title = argv[1], argv[2], argv[3]
    trigger = (10, 15)
    if "--trigger" in argv:
        a, b = argv[argv.index("--trigger") + 1].split(",")
        trigger = (float(a), float(b))
    if "--tail-window" in argv:
        a, b = argv[argv.index("--tail-window") + 1].split(",")
        TAIL_WINDOW = (float(a), float(b))
    not_levels = set()
    if "--rates" in argv:
        not_levels = set(argv[argv.index("--rates") + 1].split(","))
    d = load(path)
    xs = d["second"]
    level_cols = sorted(k for k in d if k.endswith("_v") and k[:-2] not in not_levels)
    level_names = {k[:-2] for k in level_cols}
    # a level stream's _n column is just its sample rate (one per tick); not work
    rate_cols = sorted(k for k in d if k.endswith("_n") and k[:-2] not in level_names)

    panels = []
    load_series = [("rounds offered/s", d.get("rounds", d.get("offered")))]
    if "completed" in d and any(v for v in d["completed"] if v):
        load_series.append(("completions/s (by id)", d["completed"]))
    if "abandoned" in d and any(v for v in d["abandoned"] if v):
        load_series.append(("abandoned/s", d["abandoned"]))
    panels.append((load_series, "offered load and completions", "per second", False))
    if "p50_ms" in d and any(v for v in d["p50_ms"] if v):
        panels.append(([("p50 ms", d["p50_ms"]), ("p99 ms", d["p99_ms"])], "latency of completions (log)", "ms", True))
    if rate_cols:
        rates = [(k[:-2], d[k]) for k in rate_cols]
        pos = sorted(v for _, vs in rates for v in vs if v)
        wide = bool(pos) and pos[-1] > 20 * pos[len(pos) // 2]
        panels.append((rates, "output stream record rates" + (" (log; zero not drawn)" if wide else ""), "records/s", wide))
    for k in level_cols:
        panels.append(([(k[:-2], d[k])], f"level: {k[:-2]}", "value", False))

    height = PAD_T + len(panels) * (H_PANEL + GAP) + PAD_B
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" font-family="Helvetica, Arial, sans-serif">',
           f'<rect width="{W}" height="{height}" fill="white"/>',
           f'<text x="{PAD_L}" y="24" font-size="16" font-weight="bold">{title}</text>',
           f'<text x="{W - PAD_R}" y="24" font-size="11" text-anchor="end" fill="#555">pink = trigger window; green = baseline [5,10) and tail [{TAIL_WINDOW[0]:g},{TAIL_WINDOW[1]:g}) verdict windows</text>']
    y = PAD_T + 12
    for series, ptitle, ylabel, log in panels:
        panel(out, y, xs, series, ptitle, ylabel, log=log, shade=trigger)
        y += H_PANEL + GAP
    out.append("</svg>")
    with open(out_path, "w") as f:
        f.write("\n".join(out))


if __name__ == "__main__":
    main(sys.argv)
