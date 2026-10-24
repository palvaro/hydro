#!/usr/bin/env python3
"""Window summaries of a Sedge trace of `relay_broadcast`, with the out-of-band columns.

    python3 analyze.py TRACE.tsv [TRACE.tsv ...]

Per window it prints:
  rounds/s     offered requests per second
  wire_in      relays put on the wire per second, as the members report them (in band)
  resent_in    re-sends per second, in band
  rep          member reports that reached Sedge per second (9 = all members)
  unacked      cluster unacked relays, last in-band report in the window
  wchar        members' bytes written to the kernel, MB/s, read from /proc/<pid>/io outside
  wire_x       wchar / RELAY_BYTES: relays the members really wrote, per second
  wire_x/off   wire_x / (72 x rounds): 1.0 means one first relay per peer per request and no
               re-sends; the excess over 1 is a lower bound on the re-send share
  cpu          mean member cores used; thr = mean fraction of CFS periods throttled
  rss          mean member RSS, MB
"""
import math
import sys

RELAY_BYTES = 4120.0  # bincode Relay {u32, u64, 4096-byte body} plus framing
PEERS = 8
MEMBERS = 9


def load(path):
    with open(path) as f:
        head = f.readline().rstrip("\n").split("\t")
        rows = [l.rstrip("\n").split("\t") for l in f if l.strip()]

    def num(v):
        try:
            x = float(v)
            return x
        except ValueError:
            return math.nan

    cols = {h: [num(r[i]) if i < len(r) else math.nan for r in rows] for i, h in enumerate(head)}
    return cols, len(rows)


def mean(xs):
    xs = [x for x in xs if not math.isnan(x)]
    return sum(xs) / len(xs) if xs else math.nan


def members(cols, prefix):
    return sorted(k for k in cols if k.startswith(prefix + "Member_"))


def summarize(path, period_us=None):
    cols, n = load(path)
    mem_w = members(cols, "wchar_kb_")
    mem_cpu = members(cols, "cpu_")
    mem_thr = members(cols, "nr_throttled_")
    mem_rss = members(cols, "rss_mb_")
    periods = (1e6 / period_us) if period_us else None
    get = lambda name, s: cols.get(name, [math.nan] * n)[s]

    def per_second(s):
        w = sum(cols[c][s] for c in mem_w) * 1024 if mem_w else math.nan
        thr = mean([cols[c][s] for c in mem_thr])
        return {
            "rounds": get("rounds", s),
            "wire_in": get("wire_n", s),
            "resent_in": get("resent_n", s),
            "rep": get("unacked_n", s),
            "unacked": get("unacked_v", s),
            "wchar": w / 1e6,
            "wire_x": w / RELAY_BYTES,
            "cpu": mean([cols[c][s] for c in mem_cpu]),
            "thr": thr / periods if periods else thr,
            "rss": mean([cols[c][s] for c in mem_rss]),
        }

    secs = [per_second(s) for s in range(n)]
    windows = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 45), (45, 60), (60, 75), (75, n)]
    print(f"== {path}")
    hdr = ["window", "rounds/s", "wire_in", "resent_in", "rep", "unacked", "wchar", "wire_x",
           "wire_x/off", "cpu", "thr", "rss"]
    print("\t".join(hdr))
    for a, b in windows:
        if a >= n:
            break
        w = secs[a:b]
        m = {k: mean([x[k] for x in w]) for k in w[0]}
        last_unacked = [x["unacked"] for x in w if not math.isnan(x["unacked"])]
        ratio = m["wire_x"] / (PEERS * MEMBERS * m["rounds"]) if m["rounds"] else math.nan
        print("\t".join([
            f"[{a},{b})", f"{m['rounds']:.1f}", f"{m['wire_in']:.0f}", f"{m['resent_in']:.1f}",
            f"{m['rep']:.1f}", f"{last_unacked[-1]:.0f}" if last_unacked else "-",
            f"{m['wchar']:.2f}", f"{m['wire_x']:.0f}", f"{ratio:.2f}", f"{m['cpu']:.4f}",
            f"{m['thr']:.2f}", f"{m['rss']:.1f}",
        ]))


if __name__ == "__main__":
    args = sys.argv[1:]
    period = None
    if args and args[0].startswith("--period-us="):
        period = float(args[0].split("=", 1)[1])
        args = args[1:]
    for p in args:
        summarize(p, period)
