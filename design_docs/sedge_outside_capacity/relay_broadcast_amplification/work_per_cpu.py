#!/usr/bin/env python3
"""Does a member's cost per unit of work grow with its unacked table?

    python3 work_per_cpu.py LABEL=TRACE.tsv [LABEL=TRACE.tsv ...]

For every second of a trace, take the members' in-band work, merged arrivals plus relays put on
the wire (`delivered_n + wire_n`), and divide it by the CPU-seconds the members used that
second (summed `cpu_Member_*`). Seconds are binned by the cluster's unacked count per member
(`unacked_v / 9`), and each bin prints the median work per CPU-millisecond. Where the trace has
the out-of-band columns, it also prints the members' kernel I/O (`rchar + wchar`, KiB) per
CPU-millisecond, which needs no member report. Seconds in which fewer than 9 member reports
arrived are dropped from the in-band column (their counts are incomplete).

If a tick's cost grew with the table, work per CPU-ms would fall as the table grows.
"""
import math
import sys

BINS = [0, 100, 300, 1000, 3000, 10000, 30000, 1e12]


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


def median(xs):
    xs = sorted(x for x in xs if not math.isnan(x))
    if not xs:
        return math.nan
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def main(args):
    print("trace\tunacked/member\tseconds\twork/cpu-ms (in band)\tio KiB/cpu-ms (outside)")
    for arg in args:
        label, path = arg.split("=", 1)
        cols, n = load(path)
        cpu = [k for k in cols if k.startswith("cpu_Member_")]
        io = [k for k in cols if k.startswith("wchar_kb_Member_") or k.startswith("rchar_kb_Member_")]
        # The gauge holds its last value between reports.
        last_u = math.nan
        per_bin = {}
        for s in range(n):
            u = cols["unacked_v"][s]
            if not math.isnan(u):
                last_u = u
            if math.isnan(last_u):
                continue
            c = sum(cols[k][s] for k in cpu) * 1e3  # cpu-ms
            if not c or math.isnan(c):
                continue
            reps = cols.get("unacked_n", [math.nan] * n)[s]
            work = cols["delivered_n"][s] + cols["wire_n"][s]
            w = work / c if reps >= 9 else math.nan
            k = sum(cols[k][s] for k in io) / c if io else math.nan
            per_member = last_u / 9
            b = next(i for i in range(len(BINS) - 1) if per_member < BINS[i + 1])
            per_bin.setdefault(b, []).append((w, k))
        for b in sorted(per_bin):
            xs = per_bin[b]
            lo, hi = BINS[b], BINS[b + 1]
            rng = f"[{lo:g},{hi:g})" if hi < 1e11 else f">={lo:g}"
            print(f"{label}\t{rng}\t{len(xs)}\t{median([x[0] for x in xs]):.1f}\t{median([x[1] for x in xs]):.1f}")


if __name__ == "__main__":
    main(sys.argv[1:])
