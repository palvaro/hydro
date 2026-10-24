#!/usr/bin/env python3
"""One line per Sedge run of `relay_broadcast`: the verdict and the tail against the baseline,
in band and from outside. For the lossy-buffer runs.

    python3 summarize.py --quota=0.05 RUN_DIR [RUN_DIR ...]

RUN_DIR is a Sedge results directory (results.tsv plus traces/). Per run:
  run        the directory name
  cfg f trig the configuration, μ/λ, and the offered rate in the trigger window as a multiple
             of μ (a no-burst control shows about 1/f)
  outcome    Sedge's label (INVALID reasons are kept in results.tsv)
  late       records that entered the program more than 50 ms late (the open-loop check)
  base: the second half of the pre-trigger baseline; tail: from 15 s after the trigger ends to
  the end of the run. Rates per second, summed over the 9 members:
  resent     in-band re-sends, baseline -> tail ("-": no report reached the harness)
  wire_x     relays the members wrote to the kernel (outside, wchar / 4120 B), baseline -> tail
  att_x      relay-sized sends the lossy queues were handed (outside, in the transport), and
  drop%      the share of them dropped at a full queue, in the tail
  intake     the source's writes to the members per offered request, in the tail (1.0 = all)
  cpu/q thr frz   member CPU against quota, throttled fraction, mean freeze ms, in the tail
  rss        mean member RSS at the end, MB; src_rss the source's
"""
import csv
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analyze2 import per_second, MEMBERS, PEERS  # noqa: E402


def mean(xs):
    xs = [x for x in xs if not math.isnan(x)]
    return sum(xs) / len(xs) if xs else math.nan


def fmt(x, spec=".0f"):
    return "-" if x is None or (isinstance(x, float) and math.isnan(x)) else format(x, spec)


def main():
    args = sys.argv[1:]
    quota, period = 0.05, 20000.0
    before, trigger = 10, 5
    while args and args[0].startswith("--"):
        k, v = args.pop(0)[2:].split("=", 1)
        if k == "quota":
            quota = float(v)
        elif k == "period-us":
            period = float(v)
    hdr = ["run", "cfg", "f", "trig", "outcome", "late", "resent", "wire_x", "att_x", "drop%",
           "intake", "cpu/q", "thr", "frz", "rss", "src_rss"]
    print("\t".join(hdr))
    for d in args:
        rows = list(csv.DictReader(open(os.path.join(d, "results.tsv")), delimiter="\t"))
        for r in rows:
            trace = r["trace"]
            if not os.path.isabs(trace) or not os.path.exists(trace):
                cand = [os.path.join(d, "traces", x) for x in os.listdir(os.path.join(d, "traces"))
                        if f"factor{r['factor']}.trial{r['trial']}" in x]
                trace = cand[0] if cand else None
            secs = per_second(trace, period) if trace else []
            n = len(secs)
            b0, b1 = before // 2, before
            t0, t1 = before + trigger + 15, n
            base = secs[b0:b1]
            tail = secs[t0:t1]
            m = lambda w, k: mean([x[k] for x in w])
            mu, lam = float(r["mu"]), float(r["lambda"])
            # The trigger is not in results.tsv: read the offered rate in the trigger window.
            tw = secs[before:before + trigger]
            trig = f"{m(tw, 'rounds') / mu:.2f}mu" if tw else "-"
            att_t = m(tail, "att_x")
            drop_t = m(tail, "drop_x")
            rounds_t = m(tail, "rounds")
            print("\t".join([
                os.path.basename(os.path.normpath(d)),
                r["configuration"].replace("_n9_4k", ""),
                r["factor"],
                trig,
                r["outcome"],
                r["feed_late"],
                f"{fmt(m(base, 'resent_in'))}->{fmt(m(tail, 'resent_in'))}",
                f"{fmt(m(base, 'wire_x'))}->{fmt(m(tail, 'wire_x'))}",
                fmt(att_t),
                fmt(100 * drop_t / att_t if att_t else math.nan, ".1f"),
                fmt(m(tail, "src_w") / rounds_t if rounds_t else math.nan, ".2f"),
                fmt(m(tail, "cpu") / quota, ".2f"),
                fmt(m(tail, "thr"), ".2f"),
                fmt(m(tail, "frz"), ".1f"),
                fmt(secs[-1]["rss"] if secs else math.nan, ".0f"),
                fmt(source_rss(trace), ".0f"),
            ]))


def source_rss(trace):
    if not trace:
        return math.nan
    with open(trace) as f:
        head = f.readline().rstrip("\n").split("\t")
        if "rss_mb_Source" not in head:
            return math.nan
        i = head.index("rss_mb_Source")
        last = math.nan
        for line in f:
            v = line.rstrip("\n").split("\t")
            try:
                x = float(v[i])
                if not math.isnan(x):
                    last = x
            except (ValueError, IndexError):
                pass
        return last


if __name__ == "__main__":
    main()
