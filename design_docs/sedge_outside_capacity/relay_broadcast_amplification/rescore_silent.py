#!/usr/bin/env python3
"""Re-scores Sedge runs of `relay_broadcast` under the silent-tail rule as it now stands, and
prints the original label beside the new one.

    python3 rescore_silent.py RUN_DIR [RUN_DIR ...]

The rule (sedge/src/harness.rs, `outside_reading`): a trial whose tail has no in-band record in
more than 20 % of its seconds, with every node alive and the open-loop check passed, is judged
from outside. Its capped nodes' kernel writes (`wchar_kb_Member_*`, summed) through the *second
half* of the tail, against the second half of the pre-trigger baseline:
at most 50 % is COLLAPSED, otherwise INVALID ("cannot be read"). Before this change the writes
were averaged over the whole tail. Only trials Sedge judged through this rule change; every other
label is printed unchanged.
"""
import csv
import math
import os
import re
import sys

SILENT_RE = re.compile(r"the program went silent.*?(\d+) of the tail's (\d+) seconds")


def load(path):
    with open(path) as f:
        head = f.readline().rstrip("\n").split("\t")
        rows = [l.rstrip("\n").split("\t") for l in f if l.strip()]

    def num(v):
        try:
            return float(v)
        except ValueError:
            return math.nan

    return {h: [num(r[i]) if i < len(r) else math.nan for r in rows] for i, h in enumerate(head)}, len(rows)


def mean(xs):
    xs = [x for x in xs if not math.isnan(x)]
    return sum(xs) / len(xs) if xs else math.nan


def writes(cols, a, b):
    ms = [k for k in cols if k.startswith("wchar_kb_Member_")]
    return mean([sum(cols[k][s] for k in ms) for s in range(a, b)])


def main():
    print("\t".join(["run", "trial", "original", "rescored", "writes_whole_tail", "writes_last_half"]))
    for d in sys.argv[1:]:
        res = os.path.join(d, "results.tsv")
        if not os.path.exists(res):
            continue
        traces = [x for x in os.listdir(d) if x.endswith(".tsv") and x != "results.tsv"]
        tdir = d
        if not traces and os.path.isdir(os.path.join(d, "traces")):
            tdir = os.path.join(d, "traces")
            traces = os.listdir(tdir)
        for r in csv.DictReader(open(res), delimiter="\t"):
            orig = r["outcome"]
            m = SILENT_RE.search(r["reason"])
            new, whole, half = orig, "", ""
            if m:
                tail_len = int(m.group(2))
                tr = [x for x in traces if f"factor{r['factor']}.trial{r['trial']}" in x]
                cols, n = load(os.path.join(tdir, tr[0]))
                end = n - tail_len
                base = writes(cols, 5, 10)
                w_all = writes(cols, end, n) / base
                w_half = writes(cols, end + (n - end) // 2, n) / base
                whole, half = f"{w_all:.2f}", f"{w_half:.2f}"
                new = "COLLAPSED" if w_half <= 0.5 else "INVALID"
            print("\t".join([os.path.basename(os.path.normpath(d)), r["trial"], orig, new, whole, half]))


if __name__ == "__main__":
    main()
