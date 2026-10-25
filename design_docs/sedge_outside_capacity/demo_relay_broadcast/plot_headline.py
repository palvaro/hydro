"""One burst trial of relay_broadcast against its matched no-burst control, on one page: the
in-band re-sends, and the outside counters that show where the work goes.

usage:
  python3 plot_headline.py --burst TRACE.tsv --control TRACE.tsv --out OUT.png \
      [--title TEXT] [--trigger-start 10] [--trigger 0.5] [--quota 0.05] [--period-us 20000]

Panels, top to bottom (all per second, summed over the 9 members):
  1. re-sends (in band, `resent_n`); gaps are seconds in which no member report arrived
  2. relays written to the kernel (outside: `wchar_kb` / 4120 B), with the 72 first relays per
     offered request for reference
  3. relay-sized sends handed to the lossy send queues, and of those the ones dropped at a full
     queue (outside, in the transport: `sendq_attempted_kb`, `sendq_dropped_kb` / 4120 B; 3 s
     centred mean, because a starved member publishes these counters late)
  4. member CPU as a fraction of the quota, and the fraction of CFS periods throttled

The phases are found from the burst trace alone, after the trigger ends:
  storm      attempted sends (3 s mean) above 1.5x their pre-trigger mean [5, 10) s
  black hole relays written (3 s mean) below 0.1x their pre-trigger mean
and the title of each shaded span gives the span's mean writes and attempts against baseline.
Nothing is re-run.
"""

import argparse
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import plot_amplification as pa  # noqa: E402


def smooth(xs, k=3):
    return pa.smooth(xs, k)


def spans(t, flags):
    """Maximal runs of consecutive True seconds, as (start, end) pairs."""
    out, start = [], None
    for tt, f in zip(t, flags):
        if f and start is None:
            start = tt
        if not f and start is not None:
            out.append((start, tt))
            start = None
    if start is not None:
        out.append((start, t[-1] + 1))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--burst", required=True)
    p.add_argument("--control", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--title", default=None)
    p.add_argument("--trigger-start", type=float, default=10)
    p.add_argument("--trigger", type=float, default=0.5)
    p.add_argument("--quota", type=float, default=0.05)
    p.add_argument("--period-us", type=float, default=20000)
    a = p.parse_args()
    pps = 1e6 / a.period_us
    b, c = pa.series(a.burst, pps), pa.series(a.control, pps)
    t = b["t"]
    t0, t1 = a.trigger_start, a.trigger_start + a.trigger
    base = lambda s, k: pa.window_mean(s, k, pa.BASELINE_FROM, t0)
    b_att, b_wx = base(b, "att_x"), base(b, "wire_x")
    att_s, wx_s = smooth(b["att_x"]), smooth(b["wire_x"])
    after = [tt >= t1 for tt in t]
    hole = [f and w < 0.1 * b_wx for f, w in zip(after, wx_s)]
    storm = [f and x > 1.5 * b_att and not h for f, x, h in zip(after, att_s, hole)]
    phases = [(s, e, "storm") for s, e in spans(t, storm) if e - s >= 3]
    phases += [(s, e, "black hole") for s, e in spans(t, hole) if e - s >= 3]
    phases.sort()

    fig, axes = plt.subplots(4, 1, figsize=(11, 11.5), sharex=True,
                             gridspec_kw={"height_ratios": [1, 1, 1.15, 0.8]})
    for ax in axes:
        ax.axvspan(t0, max(t1, t0 + 0.6), color="orange", alpha=0.35, lw=0)
        for s, e, kind in phases:
            ax.axvspan(s, e, color="tab:red" if kind == "black hole" else "gold",
                       alpha=0.10, lw=0)
        ax.grid(alpha=0.3)
    # Phase labels with their means, on the top panel.
    for s, e, kind in phases:
        w = pa.window_mean(b, "wire_x", s, e) / b_wx
        at = pa.window_mean(b, "att_x", s, e) / b_att
        d = pa.window_mean(b, "drop_x", s, e) / pa.window_mean(b, "att_x", s, e)
        axes[0].text((s + e) / 2, 1.02, f"{kind}, {s:g}–{e:g} s\nwrites {w:.2g}×, attempts "
                     f"{at:.2g}×, {100 * d:.1f}% dropped", transform=axes[0].get_xaxis_transform(),
                     ha="center", va="bottom", fontsize=8)

    ax = axes[0]
    ax.plot(t, b["resent"], color="tab:red", lw=1.4, label="burst run: re-sends/s (in band)")
    ax.plot(c["t"], c["resent"], color="tab:red", lw=1.0, ls="--", alpha=0.6,
            label="no-burst control: re-sends/s")
    pa.mark_missing_reports(ax, b)
    ax.set_yscale("symlog", linthresh=10)
    ax.set_ylabel("re-sends/s\n(in band)")
    ax.legend(loc="center right", fontsize=8)

    ax = axes[1]
    ax.plot(t, b["wire_x"], color="tab:blue", lw=1.4, label="burst run: relays written (wchar / 4120 B)")
    ax.plot(c["t"], c["wire_x"], color="tab:blue", lw=1.0, ls="--", alpha=0.6,
            label="no-burst control: relays written")
    ax.plot(t, [pa.PEERS * pa.MEMBERS * r for r in b["rounds"]], color="black", lw=0.8, ls=":",
            label=f"first relays needed: {pa.PEERS * pa.MEMBERS} × offered rounds/s")
    ax.set_yscale("symlog", linthresh=100)
    ax.set_ylabel("relays/s written\n(outside, kernel)")
    pa.log_ticks(ax)
    ax.legend(loc="lower left", fontsize=8)

    ax = axes[2]
    ax.plot(t, att_s, color="tab:red", lw=1.4, label="burst run: sends handed to the lossy queues")
    ax.plot(t, smooth(b["drop_x"]), color="tab:orange", lw=1.2,
            label="burst run: of those, dropped at a full queue")
    ax.plot(c["t"], smooth(c["att_x"]), color="tab:red", lw=1.0, ls="--", alpha=0.6,
            label="no-burst control: sends handed to the lossy queues")
    ax.set_yscale("symlog", linthresh=100)
    ax.set_ylabel("relay-sized sends/s\n(outside, transport; 3 s mean)")
    ax.legend(loc="lower left", fontsize=8)

    ax = axes[3]
    ax.plot(t, [x / a.quota for x in b["cpu"]], color="tab:green", lw=1.4,
            label="burst run: member CPU / quota")
    ax.plot(c["t"], [x / a.quota for x in c["cpu"]], color="tab:green", lw=1.0, ls="--", alpha=0.6,
            label="no-burst control: member CPU / quota")
    ax.plot(t, b["thr"], color="tab:gray", lw=1.0, label="burst run: fraction of periods throttled")
    ax.set_ylim(0, 1.15)
    ax.set_ylabel("member CPU / quota,\nthrottled fraction")
    ax.legend(loc="lower right", fontsize=8)
    ax.set_xlabel("seconds since start (orange: trigger; gold: storm on the wire; "
                  "red: black hole)")
    fig.suptitle(a.title or os.path.basename(a.burst), fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.985))
    fig.savefig(a.out, dpi=110)
    plt.close(fig)
    for s, e, kind in phases:
        print(f"{kind}\t{s:g}-{e:g}\twrites {pa.window_mean(b, 'wire_x', s, e) / b_wx:.3f}x\t"
              f"attempts {pa.window_mean(b, 'att_x', s, e) / b_att:.2f}x\t"
              f"dropped {100 * pa.window_mean(b, 'drop_x', s, e) / pa.window_mean(b, 'att_x', s, e):.1f}%\t"
              f"resent_in {pa.window_mean(b, 'resent', s, e):.0f}/s\t"
              f"cpu/q {pa.window_mean(b, 'cpu', s, e) / a.quota:.2f}")


if __name__ == "__main__":
    main()
