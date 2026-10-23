"""Plots Cress's hold sweep for rpc_with_retries from its text reports.

usage: python3 plot_cress.py REPORTS_DIR OUT_DIR
"""

import glob
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HOLDS = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 999]


def section(path, stream, hook):
    """The row for `hook` in the `count by hold length` table of `stream`."""
    on = False
    for line in open(path).read().splitlines():
        if re.match(rf"^{stream} \(", line):
            on = True
            continue
        if on and not line.startswith("    "):
            on = False
        if on and hook in line:
            return [int(x) for x in line.split()[-11:]]
    raise SystemExit(f"no {stream} row for {hook} in {path}")


def main():
    reports, out = sys.argv[1], sys.argv[2]
    fig, ax = plt.subplots(figsize=(8, 4))
    for cfg, color in [("three_attempts", "tab:red"), ("one_attempt", "tab:green")]:
        path = glob.glob(os.path.join(reports, f"hydro_test.rpc_with_retries.{cfg}.*.txt"))[0]
        sends = section(path, "outgoing", "rpc_retry.rs:410")
        abandoned = section(path, "abandoned", "rpc_retry.rs:410")
        ax.plot(HOLDS, sends, "o-", color=color, label=f"{cfg}: client sends (outgoing)")
        ax.plot(HOLDS, abandoned, "s--", color=color, alpha=0.6, label=f"{cfg}: abandoned")
    ax.axhline(250, color="black", ls=":", lw=1, label="unheld run: 250 sends")
    ax.axvline(40, color="grey", ls=":", lw=1)
    ax.text(42, 600, "timeout\n(40 ticks)", fontsize=8, color="grey")
    ax.set_xscale("log")
    ax.set_xlabel("hold of the server's arrival batch (rpc_retry.rs:410), in rounds")
    ax.set_ylabel("count over the run")
    ax.set_title(
        "Cress: holding the server's arrivals makes three_attempts send 2.98x (746 / 250)",
        fontsize=10,
    )
    ax.legend(fontsize=8, loc="center left")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "cress_holds.png"), dpi=110)


if __name__ == "__main__":
    main()
