"""Plots Cress's hold sweep for relay_broadcast from its text reports.

usage: python3 plot_cress.py REPORTS_DIR OUT_DIR

For each configuration and each of the two decision points that react (the members' arrival
batch and their ack batch), plots the busiest sender's messages under each hold, relative to
the unheld run: 1 means no extra work.
"""

import glob
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HOLDS = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 999]
HOOKS = [("relay_broadcast.rs:526, batch of Arrival", "arrival batch (:526)", "o-"),
         ("relay_broadcast.rs:528, batch of Ack", "ack batch (:528)", "s--")]
CONFIGS = [("echo_only", "Echo, 5 members", "tab:green"),
           ("reliable_resend", "ReliableResend, 5 members", "tab:orange"),
           ("reliable_resend_n9_4k", "ReliableResend, 9 members, 4 KiB", "tab:red")]


def curve(text, hook):
    """(extra messages from the busiest sender per hold, unheld sends of that sender), or None."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.endswith(hook) and not line.startswith(" "):
            extra, base = None, None
            for l in lines[i + 1:]:
                if not l.startswith("    "):
                    break
                if l.strip().startswith("extra messages, busiest sender"):
                    extra = [int(x) for x in l.split()[-11:]]
                m = re.search(r"multiplier: [0-9.]+× the unheld run's (\d+) sends", l)
                if m:
                    base = int(m.group(1))
            return extra, base
    return None, None


def main():
    reports, out = sys.argv[1], sys.argv[2]
    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    for cfg, label, color in CONFIGS:
        path = glob.glob(os.path.join(reports, f"hydro_test.relay_broadcast.{cfg}.*.txt"))[0]
        text = open(path).read()
        for hook, hook_label, style in HOOKS:
            extra, base = curve(text, hook)
            if extra is None:  # "no extra work at any hold length"
                ys = [1.0] * len(HOLDS)
            else:
                ys = [1 + e / base for e in extra]
            ax.plot(HOLDS, ys, style, color=color, ms=4, label=f"{label}: {hook_label}")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("hold length, in rounds (every hold starts at round 1)")
    ax.set_ylabel("busiest sender's messages\n÷ unheld run's")
    ax.set_title("Cress: delaying a member's arrivals or acks makes ReliableResend re-send; Echo never does",
                 fontsize=10)
    ax.grid(alpha=0.3, which="both")
    ax.legend(fontsize=7.5, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(out, "cress_holds.png"), dpi=110)
    print("ok")


if __name__ == "__main__":
    main()
