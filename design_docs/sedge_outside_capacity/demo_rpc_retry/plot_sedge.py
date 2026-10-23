"""Plots Sedge's per-second traces and verdicts for rpc_with_retries.

usage: python3 plot_sedge.py TRACES_DIR OUT_DIR RESULTS_TSV
"""

import csv, sys, os, math
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TR = sys.argv[1]   # traces dir
OUT = sys.argv[2]  # output dir
RES = sys.argv[3]  # results.tsv

def load(path):
    with open(path) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    def col(name, default=0.0):
        out = []
        for r in rows:
            v = r.get(name)
            if v is None or v in ("-", ""):
                out.append(float("nan") if default is None else default)
            else:
                out.append(float(v))
        return out
    return rows, col

def plot_trial(cfg, factor, trial, title, out):
    path = os.path.join(TR, f"hydro_test.rpc_with_retries.{cfg}.factor{factor}.trial{trial}.tsv")
    rows, col = load(path)
    t = [float(r["second"]) for r in rows]
    fig, axes = plt.subplots(4, 1, figsize=(10, 11), sharex=True)
    for ax in axes:
        ax.axvspan(10, 15, color="orange", alpha=0.18, lw=0)
        ax.axvspan(37, 60, color="grey", alpha=0.08, lw=0)
        ax.grid(alpha=0.3)
    a = axes[0]
    a.plot(t, col("offered"), label="offered (requests/s)", color="black", lw=1.5)
    a.plot(t, col("completed"), label="goodput: first responses (/s)", color="tab:green", lw=1.5)
    a.plot(t, col("retried_n"), label="re-sends (/s)", color="tab:red", lw=1.5)
    a.plot(t, col("served_n"), label="served by server (/s)", color="tab:blue", lw=1, alpha=0.7)
    a.plot(t, col("abandoned"), label="abandoned (/s)", color="tab:purple", lw=1, ls="--")
    a.set_ylabel("rate (/s)")
    a.legend(loc="upper right", fontsize=8, ncol=2)
    a.set_title(title, fontsize=11)
    a = axes[1]
    a.plot(t, col("outstanding_v", None), color="tab:brown", lw=1.5, label="client outstanding table")
    a.axhline(254, color="tab:brown", ls=":", lw=1, label="max when closed-loop (≈254)")
    a.set_ylabel("requests")
    a.legend(loc="upper right", fontsize=8)
    a = axes[2]
    a.plot(t, col("p50_ms", None), color="tab:cyan", lw=1.5, label="p50 latency (ms)")
    a.plot(t, col("p99_ms", None), color="tab:blue", lw=1.5, label="p99 latency (ms)")
    a.axhline(2000, color="tab:red", ls=":", lw=1, label="client timeout (2 s)")
    a.set_yscale("log")
    a.set_ylabel("latency (ms)")
    a.legend(loc="upper right", fontsize=8)
    a = axes[3]
    a.plot(t, col("cpu_Server", None), color="tab:blue", lw=1.5, label="server CPU (cores)")
    a.axhline(0.01, color="tab:blue", ls=":", lw=1, label="server quota (0.01)")
    a.plot(t, col("cpu_Client", None), color="tab:orange", lw=1.5, label="client CPU (cores, uncapped)")
    a.set_yscale("log")
    a.set_ylim(1e-3, 2)
    a.set_ylabel("CPU (cores)")
    a2 = a.twinx()
    a2.plot(t, [x / 10.0 for x in col("nr_throttled_Server")], color="tab:gray", lw=1, ls="--", label="server: fraction of CFS periods throttled")
    a2.set_ylim(0, 1.05)
    a2.set_ylabel("throttled fraction")
    h1, l1 = a.get_legend_handles_labels()
    h2, l2 = a2.get_legend_handles_labels()
    a.legend(h1 + h2, l1 + l2, loc="center right", fontsize=8)
    a.set_xlabel("seconds since start (orange: 2μ burst; grey: tail verdict window)")
    fig.tight_layout()
    fig.savefig(out, dpi=110)
    plt.close(fig)

plot_trial("three_attempts", "1.5", 0, "three_attempts, f = 1.5: COLLAPSED (μ = 1414/s, λ = 943/s)", os.path.join(OUT, "three_attempts_f1.5_collapsed.png"))
plot_trial("three_attempts", "4", 0, "three_attempts, f = 4: DRAINING (μ = 1414/s, λ = 354/s)", os.path.join(OUT, "three_attempts_f4_draining.png"))
plot_trial("three_attempts", "8", 0, "three_attempts, f = 8: RECOVERED (μ = 1414/s, λ = 177/s)", os.path.join(OUT, "three_attempts_f8_recovered.png"))
plot_trial("one_attempt", "1.5", 0, "one_attempt (control), f = 1.5: RECOVERED (μ = 1542/s, λ = 1028/s)", os.path.join(OUT, "one_attempt_f1.5_recovered.png"))

# Summary: outcome counts per factor.
with open(RES) as f:
    res = list(csv.DictReader(f, delimiter="\t"))
factors = sorted({float(r["factor"]) for r in res})
cfgs = ["one_attempt", "three_attempts"]
colors = {"RECOVERED": "tab:green", "DRAINING": "gold", "COLLAPSED": "tab:red", "INVALID": "grey"}
fig, ax = plt.subplots(figsize=(8, 4))
for yi, cfg in enumerate(cfgs):
    for xi, fac in enumerate(factors):
        outs = [r["outcome"] for r in res if r["configuration"] == cfg and float(r["factor"]) == fac]
        for k, o in enumerate(outs):
            ax.scatter(xi + (k - 1) * 0.18, yi, s=260, color=colors[o], edgecolor="black", zorder=3)
        txt = ", ".join(f"{outs.count(o)} {o}" for o in ["RECOVERED", "DRAINING", "COLLAPSED", "INVALID"] if outs.count(o))
        ax.text(xi, yi - 0.32, txt, ha="center", fontsize=8)
# f = 3 on the (log-spaced) factor axis, between f = 2 and f = 4.
ax.axvline(factors.index(2.0) + math.log(3 / 2) / math.log(4 / 2), color="black", ls=":", lw=1)
ax.set_xticks(range(len(factors)))
ax.set_xticklabels([f"f = μ/λ = {f:g}" for f in factors])
ax.set_yticks(range(len(cfgs)))
ax.set_yticklabels(cfgs)
ax.set_ylim(-0.6, 1.5)
ax.set_xlim(-0.6, len(factors) - 0.4)
for o, c in colors.items():
    ax.scatter([], [], color=c, edgecolor="black", s=80, label=o)
ax.legend(loc="upper center", ncol=4, fontsize=8, bbox_to_anchor=(0.5, -0.12))
ax.text(1.65, 1.33, "f = 3: Cress's A (recovery needs f > 3)", fontsize=8)
ax.set_title("Sedge verdicts, 3 trials per factor (burst 2μ for 5 s; server capped at 0.01 cpu)", fontsize=10)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "summary_outcomes.png"), dpi=110)
print("ok")
