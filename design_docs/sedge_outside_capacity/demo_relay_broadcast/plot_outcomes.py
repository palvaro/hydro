"""Outcome fractions for the relay_broadcast round-3 runs (isolated slots), from the traces.

usage: python3 plot_outcomes.py RUNS_DIR OUT_PNG

Reads every `r3_S*` run group under RUNS_DIR (relay_broadcast_lossy/runs: one directory per
group, with Sedge's results.tsv and one trace per trial, `*.factor<F>.trial<N>.tsv`). Groups:
  r3_S2_pe_f4_m<M>_d<D>[_t180]_s5      per_entry, f = 4, trigger M·μ for D s (60 s tail, or 180)
  r3_S2_pe_f4_ctl_d<D>[_t180]_s5       its no-burst control
  r3_S1_pe3_f<F>[_t180]_s5             per_entry3, trigger 2μ for 5 s, at f = F
  r3_S1_pe3_f<F>_s5_ctl                its no-burst control

Every trial is classified the same way, from the outside counters only (the in-band reports
go dark in exactly the runs that matter), over the second half of the tail:
  black hole  relays written to the kernel below 0.2x the pre-trigger mean [5, 10) s, with
              member CPU above 0.9 of the quota
  storm       relay-sized sends handed to the lossy queues above 1.5x the pre-trigger mean
  clear       otherwise
and, from the whole run, the storm's length: the seconds after the trigger in which the 3 s mean
of attempted sends is above 1.5x the pre-trigger mean (the storm "ends" at the last such second).

Left: per_entry at f = 4, end state per trigger. Right: per_entry3, each trial's storm length
against 3λ/μ, the peak re-send load relative to capacity if every relay is sent three times.
The horizontal bars show 3λ/μ over the measured range of μ (654.3–778.1 rounds/s). Prints the
table. Nothing is re-run.
"""

import csv
import math
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RELAY = 4120.0
QUOTA = 0.05
MU_RANGE = (654.3, 778.1)
COLOUR = {"black hole": "black", "storm": "tab:red", "clear": "tab:green"}
PE = re.compile(r"^r3_S2_pe_f4_(?:m(?P<m>[0-9.]+)|(?P<ctl>ctl))_d(?P<d>[0-9.]+)(?P<t180>_t180)?_s5$")
PE3 = re.compile(r"^r3_S1_pe3_f(?P<f>[0-9.]+)(?P<t180>_t180)?_s5(?P<ctl>_ctl)?$")


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return math.nan


def mean(xs):
    xs = [x for x in xs if not math.isnan(x)]
    return sum(xs) / len(xs) if xs else math.nan


def trial(path, trig, tail, before=10.0):
    rows = list(csv.DictReader(open(path), delimiter="\t"))
    t = [num(r["second"]) for r in rows]

    def fam(prefix, scale=1024 / RELAY):
        ks = [k for k in rows[0] if k.startswith(prefix + "Member_")]
        out = []
        for r in rows:
            vs = [num(r[k]) for k in ks]
            vs = [v for v in vs if not math.isnan(v)]
            out.append(sum(vs) * scale if vs else math.nan)
        return out

    wx, att, cpu = fam("wchar_kb_"), fam("sendq_attempted_kb_"), fam("cpu_", 1 / 9)
    w = lambda xs, a, b: mean([x for tt, x in zip(t, xs) if a <= tt < b])
    end = before + trig
    a, b = end + tail / 2, end + tail
    bw, ba = w(wx, before / 2, before), w(att, before / 2, before)
    wr, ar, cq = w(wx, a, b) / bw, w(att, a, b) / ba, w(cpu, a, b) / QUOTA
    state = "black hole" if wr < 0.2 and cq > 0.9 else ("storm" if ar > 1.5 else "clear")
    sm = [mean(att[max(0, i - 1):i + 2]) for i in range(len(att))]
    hot = [tt for tt, x in zip(t, sm) if tt >= end and x > 1.5 * ba]
    storm_end = max(hot) + 1 if hot else end
    return dict(state=state, wr=wr, ar=ar, cpu=cq, storm=storm_end - end,
                stuck=bool(hot) and storm_end >= t[-1] + 1)


def rows(runs):
    """(group, results row, trace path) for every trial under RUNS_DIR."""
    for g in sorted(os.listdir(runs)):
        d = os.path.join(runs, g)
        res = os.path.join(d, "results.tsv")
        if not os.path.isfile(res):
            continue
        for r in csv.DictReader(open(res), delimiter="\t"):
            suffix = f".factor{r['factor']}.trial{r['trial']}.tsv"
            tr = [x for x in os.listdir(d) if x.endswith(suffix)]
            if tr:
                yield g, r, os.path.join(d, tr[0])


def main():
    runs, out = sys.argv[1:3]
    pe, pe3 = [], []
    for g, r, path in rows(runs):
        m, m3 = PE.match(g), PE3.match(g)
        if not (m or m3):
            continue
        tail = 180 if (m or m3)["t180"] else 60
        trig = float(m["d"]) if m else 5.0
        o = trial(path, trig, tail)
        o.update(seed=r["seed"], sedge=r["outcome"], tail=tail, mu=float(r["mu"]))
        if m:
            o.update(trigger=(f"no burst ({m['d']} s window)" if m["ctl"] else f"{m['m']}μ × {m['d']} s"), d=float(m["d"]),
                     mult=0.0 if m["ctl"] else float(m["m"]))
            pe.append(o)
        else:
            o.update(f=float(m3["f"]), ctl=bool(m3["ctl"]))
            pe3.append(o)

    # Table.
    print("per_entry, f = 4 (slot S2, μ = 713.5)")
    print("| trigger | tail | trials | black hole | storm | clear | Sedge's verdicts |")
    print("|---|---|---|---|---|---|---|")
    keys = sorted({(o["trigger"], o["tail"], o["mult"], o["d"]) for o in pe},
                  key=lambda k: (k[2] == 0, k[2], k[3], k[1]))
    for trig, tail, _, _ in keys:
        os_ = [o for o in pe if o["trigger"] == trig and o["tail"] == tail]
        n = lambda s: sum(o["state"] == s for o in os_)
        verdicts = ", ".join(f"{v} {sum(o['sedge'] == v for o in os_)}"
                             for v in ("COLLAPSED", "INVALID", "DRAINING", "RECOVERED")
                             if any(o["sedge"] == v for o in os_))
        print(f"| {trig} | {tail} s | {len(os_)} | {n('black hole')} | {n('storm')} | {n('clear')} | {verdicts} |")
    print()
    print("per_entry3, 2μ × 5 s (slot S1, μ = 654.3)")
    print("| f | 3λ/μ (μ 654.3) | 3λ/μ (μ 778.1) | tail | trials | still storming at the end | storm length, s | Sedge's verdicts |")
    print("|---|---|---|---|---|---|---|---|")
    for f, tail in sorted({(o["f"], o["tail"]) for o in pe3 if not o["ctl"]}):
        os_ = [o for o in pe3 if o["f"] == f and o["tail"] == tail and not o["ctl"]]
        lam = MU_RANGE[0] / f
        verdicts = ", ".join(f"{v} {sum(o['sedge'] == v for o in os_)}"
                             for v in ("COLLAPSED", "INVALID", "DRAINING", "RECOVERED")
                             if any(o["sedge"] == v for o in os_))
        lens = sorted(o["storm"] for o in os_)
        print(f"| {f:g} | {3 * lam / MU_RANGE[0]:.2f} | {3 * lam / MU_RANGE[1]:.2f} | {tail} s | {len(os_)} | "
              f"{sum(o['stuck'] for o in os_)} | {', '.join(f'{x:g}' if not o else f'≥{x:g}' for x, o in zip(lens, [False] * len(lens)))} | {verdicts} |")
    ctl = [o for o in pe + [o for o in pe3 if o["ctl"]] if o.get("ctl") or o.get("mult") == 0.0]
    print(f"\ncontrols: {len(ctl)} trials, end states: "
          + ", ".join(f"{s} {sum(o['state'] == s for o in ctl)}" for s in COLOUR))

    fig, (ax, bx) = plt.subplots(1, 2, figsize=(13, 5.2), gridspec_kw={"width_ratios": [1, 1.25]})
    bursts = [k for k in keys if k[2] > 0]
    xs = range(len(bursts))
    for i, (trig, tail, _, _) in enumerate(bursts):
        os_ = [o for o in pe if o["trigger"] == trig and o["tail"] == tail]
        bottom = 0
        for s in ("black hole", "storm", "clear"):
            n = sum(o["state"] == s for o in os_)
            if n:
                ax.bar(i, n, bottom=bottom, color=COLOUR[s], edgecolor="white",
                       label=s if i == 0 or s not in ax.get_legend_handles_labels()[1] else None)
                ax.text(i, bottom + n / 2, str(n), ha="center", va="center", color="white", fontsize=9)
            bottom += n
    ax.set_xticks(list(xs))
    ax.set_xticklabels([f"{t}\n{tail} s tail" for t, tail, _, _ in bursts], fontsize=8)
    ax.set_ylabel("trials (5 seeds each)")
    ax.set_title("per_entry, f = 4: end state in the second half of the tail", fontsize=10)
    h, l = ax.get_legend_handles_labels()
    seen = {}
    for hh, ll in zip(h, l):
        seen.setdefault(ll, hh)
    ax.legend(seen.values(), seen.keys(), fontsize=8, loc="lower right")
    ax.set_ylim(0, 5.6)

    seen_x = {}
    for o in pe3:
        if o["ctl"]:
            continue
        x = 3 / o["f"]
        k = (o["f"], o["tail"])
        j = seen_x.get(k, 0)
        seen_x[k] = j + 1
        x += (j - 2) * 0.006 + (0.012 if o["tail"] == 180 else 0)
        mk = "s" if o["tail"] == 180 else "o"
        col = "tab:red" if o["stuck"] else "tab:green"
        bx.scatter(x, max(o["storm"], 0.5), marker=mk, s=40, color=col, edgecolor="black", lw=0.4,
                   zorder=3)
    for f in sorted({o["f"] for o in pe3 if not o["ctl"]}):
        x_hi = 3 / f
        x_lo = 3 * MU_RANGE[0] / f / MU_RANGE[1]
        ys = sorted(o["storm"] for o in pe3 if o["f"] == f and not o["ctl"] and o["tail"] == 60)
        y = ys[len(ys) // 2]
        bx.plot([x_lo, x_hi], [y, y], color="gray", lw=3, alpha=0.35, zorder=1,
                solid_capstyle="butt")
    bx.axhline(60, color="tab:red", ls="--", lw=0.8)
    bx.text(0.76, 63, "end of a 60 s tail", fontsize=7, color="tab:red")
    bx.set_ylim(3, 200)
    bx.axvline(1, color="black", ls=":", lw=1)
    bx.set_yscale("log")
    bx.set_xlabel("3λ/μ at μ = 654.3 (grey bars: the same λ over μ = 654.3 … 778.1)")
    bx.set_ylabel("storm length after the trigger, s (attempted > 1.5× baseline)")
    bx.set_title("per_entry3, 2μ × 5 s: storm length per trial", fontsize=10)
    from matplotlib.lines import Line2D
    bx.legend(handles=[
        Line2D([], [], marker="o", ls="", color="tab:green", mec="black", label="ended (60 s tail)"),
        Line2D([], [], marker="o", ls="", color="tab:red", mec="black", label="still storming at the end (60 s tail)"),
        Line2D([], [], marker="s", ls="", color="tab:green", mec="black", label="ended (180 s tail)"),
        Line2D([], [], marker="s", ls="", color="tab:red", mec="black", label="still storming (180 s tail)"),
    ], fontsize=8, loc="upper left")
    bx.grid(alpha=0.3, which="both")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)


if __name__ == "__main__":
    main()
