"""Plots Sedge's per-second traces and verdicts for relay_broadcast (reliable_resend_n9_4k).

usage: python3 plot_sedge.py RELAY_DIR OUT_DIR

RELAY_DIR is `design_docs/sedge_outside_capacity/relay_broadcast` (results.tsv, traces/,
mechanism/). Every input is a committed file; nothing is re-run.
"""

import csv
import json
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RD = sys.argv[1]
OUT = sys.argv[2]
MEMBERS = 9
PERIODS_PER_S = 10  # 100 ms CFS period
BURST = (10, 15)
TAIL = (45, 75)  # Sedge's verdict window with --tail 60


def load(path):
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    def col(name):
        out = []
        for r in rows:
            v = r.get(name)
            out.append(float("nan") if v in (None, "-", "") else float(v))
        return out

    return rows, col


def rss_series(path):
    """Mean and max member RSS per sample, aligned to the trace's clock.

    The sampler's clock starts when the sampler starts, not when the trial does. Trace second 0
    is taken to be the first sample at which the members together have used CPU (they are
    capped, so they start spending it as soon as the feed starts); this is accurate to about a
    second.
    """
    samples = [json.loads(l) for l in open(path)]
    t0 = None
    for s in samples:
        if len(s["members"]) == MEMBERS and sum(m["cpu_s"] for m in s["members"]) > 0:
            t0 = s["t"] - 1.0
            break
    t, mean, mx = [], [], []
    for s in samples:
        if len(s["members"]) != MEMBERS:
            continue  # start-up and teardown
        r = [m["rss_mb"] for m in s["members"]]
        t.append(s["t"] - t0)
        mean.append(sum(r) / len(r))
        mx.append(max(r))
    return t, mean, mx


def plot_trial(trace, title, out, rss=None, note=None, control=False):
    rows, col = load(trace)
    t = col("second")
    panels = 7
    fig, axes = plt.subplots(panels, 1, figsize=(10, 15), sharex=True)
    for ax in axes:
        if control:  # no burst: outline the window where the burst would have been
            ax.axvspan(*BURST, facecolor="none", edgecolor="orange", hatch="//", alpha=0.5, lw=0.8)
        else:
            ax.axvspan(*BURST, color="orange", alpha=0.2, lw=0)
        ax.axvspan(*TAIL, color="grey", alpha=0.08, lw=0)
        ax.grid(alpha=0.3)

    a = axes[0]
    a.plot(t, col("offered"), color="black", lw=1.5, label="offered (requests/s, open loop)")
    a.set_ylabel("requests/s")
    a.legend(loc="upper right", fontsize=8)
    a.set_title(title, fontsize=11)
    if note:
        a.text(0.01, 0.05, note, transform=a.transAxes, fontsize=8, va="bottom")

    a = axes[1]
    a.plot(t, col("delivered_n"), color="tab:green", lw=1.3, label="delivered (arrivals merged/s, all members)")
    a.set_ylabel("merged/s")
    a.legend(loc="upper right", fontsize=8)

    a = axes[2]
    a.plot(t, col("wire_n"), color="tab:blue", lw=1.3, label="wire (relays sent/s: first sends + re-sends)")
    a.set_ylabel("relays/s")
    a.legend(loc="upper right", fontsize=8)

    a = axes[3]
    a.plot(t, col("resent_n"), color="tab:red", lw=1.3, label="re-sends/s")
    a.set_ylabel("re-sends/s")
    a.legend(loc="upper right", fontsize=8)

    a = axes[4]
    a.plot(t, col("unacked_v"), color="tab:brown", lw=1.5, label="unacked relays (cluster total, last report)")
    a.set_ylim(bottom=0)
    a.set_ylabel("unacked")
    a2 = a.twinx()
    a2.step(t, col("unacked_n"), where="mid", color="grey", lw=0.8, alpha=0.8,
            label="member reports received that second (of 9)")
    a2.set_ylim(0, 30)
    a2.set_ylabel("reports")
    h1, l1 = a.get_legend_handles_labels()
    h2, l2 = a2.get_legend_handles_labels()
    a.legend(h1 + h2, l1 + l2, loc="center right", fontsize=7.5)

    a = axes[5]
    fr = [[x / PERIODS_PER_S for x in col(f"nr_throttled_Member_{i}")] for i in range(MEMBERS)]
    mean = [sum(v) / MEMBERS for v in zip(*fr)]
    lo = [min(v) for v in zip(*fr)]
    hi = [max(v) for v in zip(*fr)]
    a.fill_between(t, lo, hi, color="tab:purple", alpha=0.2, lw=0, label="min-max over members")
    a.plot(t, mean, color="tab:purple", lw=1.3, label="mean over members")
    a.set_ylim(0, 1.05)
    a.set_ylabel("throttled\nfraction")
    a.legend(loc="lower right", fontsize=8)

    a = axes[6]
    if rss:
        rt, rmean, rmax = rss_series(rss)
        a.plot(rt, rmean, color="tab:olive", lw=1.5, label="member RSS, mean of 9 (MB)")
        a.plot(rt, rmax, color="tab:olive", lw=0.8, ls="--", label="member RSS, max (MB)")
        a.legend(loc="upper left", fontsize=8)
    else:
        a.text(0.5, 0.5, "member RSS was not sampled in this run", ha="center", va="center",
               transform=a.transAxes, fontsize=9, color="grey")
    a.set_ylabel("RSS (MB)")
    a.set_xlabel("seconds since start (orange: 2μ burst; grey: Sedge's verdict window)")
    a.set_xlim(0, 75)
    fig.tight_layout()
    fig.savefig(out, dpi=100)
    plt.close(fig)


T = os.path.join(RD, "traces")
M = os.path.join(RD, "mechanism")
plot_trial(
    os.path.join(M, "mech_f1.5.trace.tsv"),
    "reliable_resend_n9_4k, burst, f = 1.5: COLLAPSED (μ = 123.4/s, λ = 82.3/s; seed 7)",
    os.path.join(OUT, "burst_f1.5_collapsed.png"),
    rss=os.path.join(M, "mech_f1.5.members.jsonl"),
)
plot_trial(
    os.path.join(T, "burst_bisect", "factor2.5.trial2.tsv"),
    "reliable_resend_n9_4k, burst, f = 2.5: slow recovery (Sedge: DRAINING; λ = 49.4/s; seed 3)",
    os.path.join(OUT, "burst_f2.5_slow_recovery.png"),
)
plot_trial(
    os.path.join(T, "burst_other", "factor4.trial0.tsv"),
    "reliable_resend_n9_4k, burst, f = 4: RECOVERED (λ = 30.8/s; seed 1)",
    os.path.join(OUT, "burst_f4_recovered.png"),
)
plot_trial(
    os.path.join(T, "ctl_f1.5_a", "factor1.5.trial0.tsv"),
    "reliable_resend_n9_4k, NO burst (control), f = 1.5: flat (Sedge: DRAINING, slope noise; λ = 82.3/s)",
    os.path.join(OUT, "control_f1.5_flat.png"),
    note="no trigger: the hatched window runs at λ like the rest",
    control=True,
)

# ---- Summary: outcome vs f, bursts and controls -------------------------------------------
with open(os.path.join(RD, "results.tsv")) as f:
    res = list(csv.DictReader(f, delimiter="\t"))
colors = {"RECOVERED": "tab:green", "DRAINING": "gold", "COLLAPSED": "tab:red", "INVALID": "grey"}
factors = sorted({float(r["factor"]) for r in res})
kinds = [("no-burst control", lambda r: r["run"].startswith("ctl_")),
         ("burst 2μ for 5 s", lambda r: not r["run"].startswith("ctl_"))]


def final_unacked(r):
    """Mean unacked over the last 15 s of the trace (the gauge holds its last report)."""
    run, fac, trial = r["run"], r["factor"], r["trial"]
    if run.startswith("mech"):
        p = os.path.join(M, f"{run}.trace.tsv")
    else:
        p = os.path.join(T, run, f"factor{fac}.trial{trial}.tsv")
    _, col = load(p)
    v = [x for x in col("unacked_v")[60:75] if not math.isnan(x)]
    return sum(v) / len(v)


fig, (ax, bx) = plt.subplots(2, 1, figsize=(9, 7.5), gridspec_kw={"height_ratios": [1, 1.2]})
for yi, (name, pick) in enumerate(kinds):
    for xi, fac in enumerate(factors):
        rs = [r for r in res if pick(r) and float(r["factor"]) == fac]
        n = len(rs)
        for k, r in enumerate(rs):
            dx = (k - (n - 1) / 2) * 0.13
            ax.scatter(xi + dx, yi, s=150, color=colors[r["outcome"]], edgecolor="black", zorder=3)
        if not rs:
            ax.text(xi, yi, "not run", ha="center", va="center", fontsize=7, color="grey")
ax.annotate("flat trace; DRAINING is\nslope noise at the end", (factors.index(1.5), 0),
            xytext=(factors.index(1.5) + 0.25, -0.45), fontsize=7, arrowprops=dict(arrowstyle="-", lw=0.5))
ax.text(factors.index(1.5) + 0.5, 1.3, "grey INVALID: open-loop check fired;\nthe program's own reading was COLLAPSED",
        fontsize=7, ha="center")
ax.set_xticks(range(len(factors)))
ax.set_xticklabels([f"{f:g}" for f in factors])
ax.set_yticks([0, 1])
ax.set_yticklabels([k for k, _ in kinds])
ax.set_ylim(-0.6, 1.6)
ax.set_xlim(-0.6, len(factors) - 0.4)
ax.axvline(factors.index(2.0) + 0.5, color="black", ls=":", lw=1)
ax.text(factors.index(2.0) + 0.55, 1.4, "boundary: 2 < f < 2.5", fontsize=8)
for o, c in colors.items():
    ax.scatter([], [], color=c, edgecolor="black", s=60, label=o)
ax.legend(loc="upper left", ncol=4, fontsize=7.5)
ax.set_title("Sedge verdicts per trial (9 members at 0.01 cpu; μ = 123.4 rounds/s)", fontsize=10)
ax.set_xlabel("headroom f = μ/λ")

for name, pick, marker, color in [("burst", kinds[1][1], "o", "tab:red"),
                                  ("no-burst control", kinds[0][1], "s", "tab:blue")]:
    xs, ys = [], []
    for r in res:
        if pick(r):
            xs.append(float(r["factor"]))
            ys.append(max(final_unacked(r), 1))
    bx.scatter(xs, ys, marker=marker, color=color, edgecolor="black", s=45, label=name, zorder=3)
bx.set_xscale("log")
bx.set_yscale("log")
bx.set_xticks(factors)
bx.set_xticklabels([f"{f:g}" for f in factors])
bx.minorticks_off()
bx.axvline(math.sqrt(2.0 * 2.5), color="black", ls=":", lw=1)
bx.grid(alpha=0.3)
bx.set_xlabel("headroom f = μ/λ")
bx.set_ylabel("unacked relays, mean over 60-75 s")
bx.legend(loc="upper right", fontsize=8)
bx.set_title("Where each run ends: still growing (≥ 10⁵) or back to baseline (≤ 10³)", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "summary_outcomes.png"), dpi=110)
print("ok")
