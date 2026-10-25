"""Outcome against f for the relay_broadcast lossy-buffer runs, one row per policy.

usage: python3 plot_summary.py RUNS_DIR OUT_PNG [GROUP_PREFIX ...]

RUNS_DIR is `relay_broadcast_lossy/runs` (one directory per run group, with Sedge's results.tsv
and one trace per trial, `*.factor<F>.trial<N>.tsv`). Only groups whose name starts with one of
GROUP_PREFIX (default `p_`) are drawn. A group name is `<prefix><policy>_<burst|ctl>
_f<F>`, with policy `pe`, `pe3` or `rr`.

Top: Sedge's outcome per (policy, f), burst run and no-burst control, one marker per trial.
Bottom: the burst runs' tail [45,75) against their pre-trigger baseline, read from outside:
relay-sized sends handed to the lossy queues (`sendq_attempted_kb`) and relays written to the
kernel (`wchar`), both / 4120 B. Nothing is re-run.
"""

import csv
import math
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

RELAY_BYTES = 4120.0
BASE = (5, 10)
TAIL = (45, 75)
POLICIES = [
    ("pe", "per_entry (max_attempts none)"),
    ("pe3", "per_entry3 (max_attempts 3)"),
    ("rr", "reliable_resend (per peer)"),
]
OUTCOMES = ["RECOVERED", "DRAINING", "COLLAPSED", "INVALID"]
COLOUR = {"RECOVERED": "tab:green", "DRAINING": "tab:orange", "COLLAPSED": "tab:red",
          "INVALID": "tab:gray"}
NAME = re.compile(r"^(?P<prefix>[a-z0-9]+_)(?P<pol>pe3|pe|rr)_(?P<kind>burst|ctl)_f(?P<f>[0-9.]+)$")


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return math.nan


def window(path, cols, a, b):
    """Mean over seconds [a, b) of the per-second sum of the given column families."""
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    out = {}
    for fam in cols:
        ks = [k for k in rows[0] if k.startswith(fam + "Member_")]
        xs = []
        for r in rows:
            t = num(r["second"])
            if a <= t < b and ks:
                vs = [num(r[k]) for k in ks]
                vs = [v for v in vs if not math.isnan(v)]
                if vs:
                    xs.append(sum(vs) * 1024 / RELAY_BYTES)
        out[fam] = sum(xs) / len(xs) if xs else math.nan
    return out


def main():
    runs, out = sys.argv[1:3]
    prefixes = sys.argv[3:] or ["p_"]
    pts = []  # (pol, kind, f, trial, outcome, att_ratio, wire_ratio)
    for g in sorted(os.listdir(runs)):
        res = os.path.join(runs, g, "results.tsv")
        m = NAME.match(g)
        if not m or not os.path.isfile(res) or not any(g.startswith(p) for p in prefixes):
            continue
        for r in csv.DictReader(open(res), delimiter="\t"):
            f = float(r["factor"])
            suffix = f".factor{r['factor']}.trial{r['trial']}.tsv"
            cand = [x for x in os.listdir(os.path.join(runs, g)) if x.endswith(suffix)]
            tr = os.path.join(runs, g, cand[0]) if cand else ""
            att = wire = math.nan
            if m["kind"] == "burst" and os.path.exists(tr):
                fams = ["sendq_attempted_kb_", "wchar_kb_"]
                b, t = window(tr, fams, *BASE), window(tr, fams, *TAIL)
                att = t[fams[0]] / b[fams[0]] if b[fams[0]] else math.nan
                wire = t[fams[1]] / b[fams[1]] if b[fams[1]] else math.nan
            pts.append((m["pol"], m["kind"], f, int(r["trial"]), r["outcome"], att, wire))
    fs = sorted({p[2] for p in pts})
    xi = {f: i for i, f in enumerate(fs)}

    fig, (ax, bx) = plt.subplots(2, 1, figsize=(10, 7.5), sharex=True,
                                 gridspec_kw={"height_ratios": [1, 1.2]})
    seen = set()
    for row, (pol, label) in enumerate(POLICIES):
        for kind, dy, marker in (("burst", 0.15, "o"), ("ctl", -0.15, "s")):
            ps = [p for p in pts if p[0] == pol and p[1] == kind]
            for f in fs:
                here = [p for p in ps if p[2] == f]
                for j, p in enumerate(sorted(here, key=lambda p: p[3])):
                    x = xi[f] + (j - (len(here) - 1) / 2) * 0.09
                    lab = None
                    if (p[4], marker) not in seen:
                        seen.add((p[4], marker))
                        lab = f"{p[4]} ({'burst run' if kind == 'burst' else 'no-burst control'})"
                    ax.scatter(x, -row + dy, marker=marker, s=70, color=COLOUR.get(p[4], "k"),
                               edgecolor="black", lw=0.5, label=lab, zorder=3)
    ax.set_yticks([-i for i in range(len(POLICIES))])
    ax.set_yticklabels([l for _, l in POLICIES], fontsize=9)
    ax.set_ylim(-len(POLICIES) + 0.5, 0.5)
    ax.grid(alpha=0.3)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.32), ncol=3, fontsize=8)
    ax.set_title("Sedge outcome after a 2μ, 5 s burst (circles) and without it (squares)",
                 fontsize=11, pad=58)

    for pol, label in POLICIES:
        colour = {"pe": "tab:red", "pe3": "tab:purple", "rr": "tab:blue"}[pol]
        ps = sorted((p for p in pts if p[0] == pol and p[1] == "burst"), key=lambda p: p[2])
        if not ps:
            continue
        xs = [xi[p[2]] for p in ps]
        bx.plot(xs, [p[5] for p in ps], "-o", color=colour, lw=1.6,
                label=f"{label}: sends attempted (lossy queue)")
        bx.plot(xs, [p[6] for p in ps], "--^", color=colour, lw=1.0,
                label=f"{label}: relays written (wchar)")
    bx.axhline(1, color="black", ls=":", lw=1)
    bx.set_yscale("log")
    bx.set_ylabel("burst run: tail [45,75) ÷ pre-trigger, outside")
    bx.grid(alpha=0.3, which="both")
    bx.legend(fontsize=7, loc="lower left", bbox_to_anchor=(0.0, 0.3), ncol=2)
    bx.set_xticks(range(len(fs)))
    bx.set_xticklabels([f"{f:g}" for f in fs])
    bx.set_xlabel("f = μ / λ (μ = 565.7 rounds/s)")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)
    for p in sorted(pts):
        print("\t".join(str(x) if not isinstance(x, float) else f"{x:.3g}" for x in p))


if __name__ == "__main__":
    main()
