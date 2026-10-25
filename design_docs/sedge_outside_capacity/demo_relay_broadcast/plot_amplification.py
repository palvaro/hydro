"""Plots whether Cress's amplified streams sustain a relay_broadcast collapse under Sedge.

Takes one burst trial and its paired no-burst control (same configuration, factor and length)
and draws four figures:

  PREFIX_resends.png  (a) re-sends/s over time, burst run against control
  PREFIX_wire.png     (b) wire traffic measured from outside the members (/proc wchar/rchar),
                          over time, burst run against control, with member RSS and CPU
                          underneath, so that a crash (RSS at the limit, CPU to 0) is not
                          mistaken for a collapse
  PREFIX_streams.png  (c) Cress's amplified streams (wire, merged, acks_sent, delivered) over
                          time, each divided by the control's mean
  PREFIX_tail.png     (d) each stream's mean in the tail verdict window as a ratio to the same
                          run's pre-trigger baseline, burst run against control

It also prints (d) as a table, for pasting into the demo.

usage:
  python3 plot_amplification.py --burst TRACE.tsv --control TRACE.tsv --out DIR --prefix NAME \
      [--title TEXT] [--trigger-start 10] [--trigger 5] [--tail 60] [--memory-mb 400] \
      [--quota 0.01]

Every input is a Sedge per-second trace (`traces/<group>/factor<f>.trial<n>.tsv`); nothing is
re-run.

How the trace columns map to Cress's streams (`relay_broadcast.rs`, `Outputs` and
`RelayMetrics`). The members report once a second, in band, summed over the members whose report
arrived in that second (`unacked_n` counts them; 9 = all):

  wire       `wire_n`: relays put on the wire, first sends and re-sends
  re-sends   `resent_n`: the re-send part of `wire_n`
  merged     `delivered_n`: arrivals merged, duplicates included
  delivered  `delivered_n` as well: Cress's `delivered` stream carries one id per merged
             arrival, so the two counts are the same; it is drawn once, labelled both ways
  acks_sent  derived: `delivered_n - MEMBERS * rounds`. Every merged arrival that came from a
             peer is acknowledged, and the rest are the source's broadcast, one per member per
             request. This is approximate per second (a request's broadcast and its arrivals can
             fall in different seconds) but exact over a window.

Out of band, read by Sedge from /proc/<pid>/io of every member's processes, never via the
members' own queues:

  wire_x     sum of `wchar_kb_Member_*` / RELAY_BYTES: relays the members actually wrote
  recv_x     sum of `rchar_kb_Member_*` / RELAY_BYTES: relays the members actually read. In
             the traces so far `rchar` is 0 throughout (the members' socket reads are `recv*`
             calls, which `rchar` does not count), so this series is dropped when it is all 0

When the in-band reports stop (a starved or dead member), the in-band series undercount. Seconds
with fewer than MEMBERS reports are marked with a grey tick along the bottom of each in-band
panel; in (d) and the table, an in-band stream whose tail window averaged fewer than MEMBERS
reports per second is marked "dark" (hatched bar), because its ratio then measures the missing
reports, not the stream.
"""

import argparse
import csv
import math
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator

MEMBERS = 9
PEERS = MEMBERS - 1
RELAY_BYTES = 4120.0  # bincode Relay {u32, u64, 4096-byte body} plus framing
PERIODS_PER_S = 10  # 100 ms CFS period
BASELINE_FROM = 5  # Sedge's baseline window starts after warm-up


def load(path):
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return math.nan

    cols = {}
    for k in rows[0].keys() if rows else []:
        cols[k] = [num(r.get(k)) for r in rows]
    return cols


def members(cols, prefix):
    return sorted(k for k in cols if k.startswith(prefix + "Member_"))


def summed(cols, prefix):
    ks = members(cols, prefix)
    n = len(cols["second"])
    out = []
    for i in range(n):
        vs = [cols[k][i] for k in ks if not math.isnan(cols[k][i])]
        out.append(sum(vs) if vs else math.nan)
    return out


def averaged(cols, prefix, fn=None):
    ks = members(cols, prefix)
    n = len(cols["second"])
    out = []
    for i in range(n):
        vs = [cols[k][i] for k in ks if not math.isnan(cols[k][i])]
        out.append((fn or (lambda xs: sum(xs) / len(xs)))(vs) if vs else math.nan)
    return out


def series(path):
    """Every stream this script draws, per second, keyed by name."""
    c = load(path)
    n = len(c["second"])
    get = lambda k: c.get(k, [math.nan] * n)
    rounds = get("rounds")
    delivered = get("delivered_n")
    acks = [
        max(0.0, d - MEMBERS * r) if not (math.isnan(d) or math.isnan(r)) else math.nan
        for d, r in zip(delivered, rounds)
    ]
    # A second with no member report at all has no in-band reading: draw a gap, not a 0.
    reports = get("unacked_n")
    blank = lambda xs: [math.nan if (math.isnan(r) or r == 0) else x for x, r in zip(xs, reports)]
    delivered, acks = blank(delivered), blank(acks)
    wchar = summed(c, "wchar_kb_")
    rchar = summed(c, "rchar_kb_")
    return {
        "t": get("second"),
        "rounds": rounds,
        "reports": get("unacked_n"),
        "unacked": blank(get("unacked_v")),
        "resent": blank(get("resent_n")),
        "wire": blank(get("wire_n")),
        "merged": delivered,
        "acks_sent": acks,
        "wire_x": [w * 1024 / RELAY_BYTES for w in wchar],
        "recv_x": [w * 1024 / RELAY_BYTES for w in rchar],
        "wchar_mb": [w * 1024 / 1e6 for w in wchar],
        "rss_mean": averaged(c, "rss_mb_"),
        "rss_max": averaged(c, "rss_mb_", max),
        "cpu": averaged(c, "cpu_"),
        "thr": [x / PERIODS_PER_S for x in averaged(c, "nr_throttled_")],
    }


def log_ticks(ax):
    """Readable ticks on a log axis: 1-2-5 steps, printed as plain numbers."""
    lo, hi = ax.get_ylim()
    ticks = [m * 10 ** e for e in range(-4, 7) for m in (1, 2, 5) if lo <= m * 10 ** e <= hi]
    ax.yaxis.set_major_locator(FixedLocator(ticks))
    ax.yaxis.set_minor_locator(NullLocator())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))


def window_mean(s, key, a, b):
    xs = [x for t, x in zip(s["t"], s[key]) if a <= t < b and not math.isnan(x)]
    return sum(xs) / len(xs) if xs else math.nan


def shade(ax, args, control=False):
    t0, t1 = args.trigger_start, args.trigger_start + args.trigger
    ax.axvspan(t0, t1, color="orange", alpha=0.2, lw=0)
    ax.axvspan(*tail_window(args), color="grey", alpha=0.08, lw=0)
    ax.grid(alpha=0.3)


def tail_window(args):
    end = args.trigger_start + args.trigger + args.tail
    return (args.trigger_start + args.trigger + args.tail / 2, end)


def mark_missing_reports(ax, s):
    xs = [t for t, r in zip(s["t"], s["reports"]) if math.isnan(r) or r < MEMBERS]
    if xs:
        ax.plot(xs, [0] * len(xs), "|", color="grey", ms=8, transform=ax.get_xaxis_transform(),
                label=f"burst run: < {MEMBERS} member reports that second")


def xlabel(ax):
    ax.set_xlabel("seconds since start (orange: trigger window; grey: Sedge's tail verdict window)")


def fig_resends(b, c, args, out):
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    for ax in axes:
        shade(ax, args)
    a = axes[0]
    a.plot(b["t"], b["resent"], color="tab:red", lw=1.6, label="burst run: re-sends/s (in band)")
    a.plot(c["t"], c["resent"], color="tab:red", lw=1.0, ls="--", alpha=0.7,
           label="no-burst control: re-sends/s")
    base = window_mean(b, "resent", BASELINE_FROM, args.trigger_start)
    a.axhline(base, color="black", ls=":", lw=1, label=f"burst run, pre-trigger mean ({base:.1f}/s)")
    a.set_yscale("symlog", linthresh=10)
    a.set_ylabel("re-sends/s")
    mark_missing_reports(a, b)
    a.legend(loc="upper right", fontsize=8)
    a.set_title(args.title or f"{args.prefix}: re-sends", fontsize=11)
    a = axes[1]
    a.plot(b["t"], b["unacked"], color="tab:brown", lw=1.6, label="burst run: unacked relays")
    a.plot(c["t"], c["unacked"], color="tab:brown", lw=1.0, ls="--", alpha=0.7,
           label="no-burst control: unacked relays")
    a.set_yscale("symlog", linthresh=100)
    a.set_ylabel("unacked relays (cluster)")
    a.legend(loc="upper right", fontsize=8)
    xlabel(a)
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


def fig_wire(b, c, args, out):
    fig, axes = plt.subplots(3, 1, figsize=(10, 10), sharex=True)
    for ax in axes:
        shade(ax, args)
    a = axes[0]
    a.plot(b["t"], b["wire_x"], color="tab:blue", lw=1.6,
           label="burst run: relays written (outside: wchar / 4120 B)")
    a.plot(c["t"], c["wire_x"], color="tab:blue", lw=1.0, ls="--", alpha=0.7,
           label="no-burst control: relays written (outside)")
    if present(b, "recv_x"):
        a.plot(b["t"], b["recv_x"], color="tab:cyan", lw=1.0,
               label="burst run: relays read (outside: rchar / 4120 B)")
    first = [PEERS * MEMBERS * r for r in b["rounds"]]
    a.plot(b["t"], first, color="black", lw=1, ls=":",
           label=f"first relays needed: {PEERS * MEMBERS} × offered rounds/s")
    a.set_ylabel("relays/s, measured outside")
    a.legend(loc="upper right", fontsize=8)
    a.set_title(args.title or f"{args.prefix}: wire traffic measured outside", fontsize=11)
    a = axes[1]
    a.plot(b["t"], b["rss_mean"], color="tab:purple", lw=1.6, label="burst run: member RSS, mean")
    a.plot(b["t"], b["rss_max"], color="tab:purple", lw=1.0, alpha=0.6, label="burst run: member RSS, max")
    a.plot(c["t"], c["rss_mean"], color="tab:purple", lw=1.0, ls="--", alpha=0.7,
           label="no-burst control: member RSS, mean")
    if args.memory_mb:
        a.axhline(args.memory_mb, color="tab:red", ls=":", lw=1, label=f"memory limit ({args.memory_mb} MB)")
    a.set_ylabel("MB")
    a.legend(loc="upper left", fontsize=8)
    a = axes[2]
    a.plot(b["t"], b["cpu"], color="tab:green", lw=1.6, label="burst run: member CPU, mean (cores)")
    a.plot(c["t"], c["cpu"], color="tab:green", lw=1.0, ls="--", alpha=0.7,
           label="no-burst control: member CPU, mean")
    a.axhline(args.quota, color="tab:green", ls=":", lw=1, label=f"member quota ({args.quota:g})")
    a.set_ylabel("cores (clipped at 2× quota)")
    a.set_ylim(0, 2 * args.quota)
    a2 = a.twinx()
    a2.plot(b["t"], b["thr"], color="tab:gray", lw=1, label="burst run: fraction of periods throttled")
    a2.plot(c["t"], c["thr"], color="tab:gray", lw=1, ls="--", alpha=0.7,
            label="no-burst control: throttled")
    a2.set_ylim(0, 1.05)
    a2.set_ylabel("throttled fraction")
    h1, l1 = a.get_legend_handles_labels()
    h2, l2 = a2.get_legend_handles_labels()
    a.legend(h1 + h2, l1 + l2, loc="lower left", fontsize=8)
    xlabel(a)
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


STREAMS = [
    # key, label, colour
    ("wire", "wire (in band)", "tab:blue"),
    ("merged", "merged = delivered (in band)", "tab:orange"),
    ("acks_sent", "acks_sent (derived)", "tab:green"),
    ("wire_x", "wire, outside (wchar)", "navy"),
    ("recv_x", "merged input, outside (rchar)", "darkorange"),
]


IN_BAND = {"resent", "wire", "merged", "acks_sent"}


def present(s, key):
    return any(not math.isnan(x) and x != 0 for x in s[key])


def fig_streams(b, c, args, out):
    fig, ax = plt.subplots(figsize=(10, 5))
    shade(ax, args)
    for key, label, colour in STREAMS:
        if not present(c, key):
            continue
        ref = window_mean(c, key, BASELINE_FROM, c["t"][-1] + 1)
        if not ref or math.isnan(ref):
            continue
        lw = 1.0 if key in ("wire_x", "recv_x") else 1.6
        ax.plot(b["t"], [x / ref if x > 0 else math.nan for x in b[key]], color=colour, lw=lw,
                label=f"burst run: {label}")
    ax.axhline(1, color="black", ls=":", lw=1, label="control's mean = 1")
    ax.set_yscale("log")
    log_ticks(ax)
    ax.set_ylabel("÷ no-burst control's mean")
    mark_missing_reports(ax, b)
    ax.legend(loc="upper right", fontsize=8)
    ax.set_title(args.title or f"{args.prefix}: Cress's amplified streams, normalized to the control",
                 fontsize=11)
    xlabel(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


TAIL_STREAMS = [("resent", "re-sends")] + [(k, l) for k, l, _ in STREAMS]


def tail_streams(b, c):
    return [(k, l) for k, l in TAIL_STREAMS if present(b, k) or present(c, k)]


def dark(s, args):
    """Whether the in-band reports were incomplete in the tail window."""
    r = window_mean(s, "reports", *tail_window(args))
    return math.isnan(r) or r < MEMBERS - 0.5


def tail_ratios(s, args):
    a, z = tail_window(args)
    out = {}
    for key, _ in TAIL_STREAMS:
        base = window_mean(s, key, BASELINE_FROM, args.trigger_start)
        tail = window_mean(s, key, a, z)
        out[key] = (base, tail, tail / base if base else math.nan)
    return out


def fig_tail(b, c, args, out):
    rb, rc = tail_ratios(b, args), tail_ratios(c, args)
    streams = tail_streams(b, c)
    fig, ax = plt.subplots(figsize=(10, 4.5))
    xs = range(len(streams))
    w = 0.38
    clean = lambda v: v if v and not math.isnan(v) and v > 0 else 1e-3
    for s, r, off, colour, label in ((b, rb, -w / 2, "tab:red", "burst run"),
                                     (c, rc, w / 2, "tab:gray", "no-burst control")):
        is_dark = dark(s, args)
        for i, (x, (k, _)) in enumerate(zip(xs, streams)):
            d = is_dark and k in IN_BAND
            ax.bar(x + off, clean(r[k][2]), w, color="white" if d else colour, edgecolor=colour,
                   hatch="//" if d else None, label=label if i == 0 else None)
            v = r[k][2]
            txt = "dark" if d else ("n/a" if math.isnan(v) else (f"{v:.0f}×" if v >= 10 else f"{v:.2f}×"))
            ax.text(x + off, clean(v) * 1.1, txt, ha="center", fontsize=7)
    ax.axhline(1, color="black", ls=":", lw=1)
    ax.set_yscale("log")
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo, hi * 3)
    log_ticks(ax)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([l for _, l in streams], fontsize=8, rotation=12)
    a, z = tail_window(args)
    ax.set_ylabel(f"tail [{a:g},{z:g}) mean ÷ pre-trigger mean")
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    ax.set_title(args.title or f"{args.prefix}: tail against baseline", fontsize=11)
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)
    return rb, rc


def print_table(b, c, rb, rc, args):
    a, z = tail_window(args)
    rep = lambda s: window_mean(s, "reports", a, z)
    print(f"tail [{a:g},{z:g}): member reports/s, burst run {rep(b):.1f}, control {rep(c):.1f} "
          f"(of {MEMBERS})")
    print(f"| stream | burst: pre-trigger /s | burst: tail /s | burst: ratio "
          f"| control: pre-trigger /s | control: tail /s | control: ratio |")
    print("|---|---|---|---|---|---|---|")
    f = lambda v, d=1: "n/a" if math.isnan(v) else f"{v:.{d}f}"
    for k, label in tail_streams(b, c):
        bb, bt, br = rb[k]
        cb, ct, cr = rc[k]
        rbs = "dark" if dark(b, args) and k in IN_BAND else f"{f(br, 2)}×"
        rcs = "dark" if dark(c, args) and k in IN_BAND else f"{f(cr, 2)}×"
        print(f"| {label} | {f(bb)} | {f(bt)} | {rbs} | {f(cb)} | {f(ct)} | {rcs} |")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--burst", required=True, help="trace of the burst trial")
    p.add_argument("--control", required=True, help="trace of the paired no-burst control")
    p.add_argument("--out", required=True, help="output directory")
    p.add_argument("--prefix", required=True, help="file name prefix, e.g. bounded_f2")
    p.add_argument("--title", default=None)
    p.add_argument("--trigger-start", type=float, default=10, help="seconds (Sedge default 10)")
    p.add_argument("--trigger", type=float, default=5, help="trigger length, s (Sedge --trigger)")
    p.add_argument("--tail", type=float, default=60, help="Sedge --tail, s")
    p.add_argument("--memory-mb", type=float, default=None, help="per-node limit, for the RSS panel")
    p.add_argument("--quota", type=float, default=0.01, help="member CPU quota, cores")
    args = p.parse_args()
    os.makedirs(args.out, exist_ok=True)
    b, c = series(args.burst), series(args.control)
    o = lambda name: os.path.join(args.out, f"{args.prefix}_{name}.png")
    fig_resends(b, c, args, o("resends"))
    fig_wire(b, c, args, o("wire"))
    fig_streams(b, c, args, o("streams"))
    rb, rc = fig_tail(b, c, args, o("tail"))
    print_table(b, c, rb, rc, args)


if __name__ == "__main__":
    main()
