#!/usr/bin/env python3
"""Window summaries of a Sedge trace of `relay_broadcast` (per-member cap), for the lossy and
fine-cap runs. Like analyze.py, plus the CPU against the quota, the CFS freeze length and the
drops the lossy send queues imply.

    python3 analyze2.py --quota=0.05 TRACE.tsv [TRACE.tsv ...]
    python3 analyze2.py --quota=0.05 --windows=0,5,10,15,20,30,45,60,75 TRACE.tsv

Per window:
  rounds/s   offered requests per second
  first_in   first relays per second the dataflow emitted (wire - resent, in band)
  resent_in  re-sends per second the dataflow emitted (in band)
  rep        member reports that reached Sedge per second (9 = all members)
  unacked    cluster unacked relays, last in-band report in the window
  wire_x     relays per second the members really wrote to the kernel (wchar / 4120 B, outside)
  x/off      wire_x / (72 x rounds): 1.0 = one first relay per peer per request, nothing more
  drop~      wire_in - wire_x: sends the dataflow emitted that never reached the kernel (with the
             lossy buffer, dropped at a full queue; with an unbounded one, still queued)
  att_x      relay-sized sends per second the members' lossy send queues were handed
             (sendq_attempted_kb / 4120 B), counted in the transport, outside the program's
             reports; '-' without a lossy buffer
  drop_x     of those, dropped at a full queue, per second
  intake     the source's writes to the members (wchar_kb_Source / 4120 B / 9) per offered request:
             1.0 = every request reached every member's socket; below 1 the backlog sits in the
             source's send queues
  cpu/q      mean member CPU as a fraction of its quota
  thr        mean fraction of CFS periods in which a member was throttled
  frz_ms     mean length of one throttled stretch (throttled ms / throttled periods), per member
  rss        mean member RSS, MB
"""
import math
import sys

RELAY_BYTES = 4120.0
PEERS = 8
MEMBERS = 9


def load(path):
    with open(path) as f:
        head = f.readline().rstrip("\n").split("\t")
        rows = [l.rstrip("\n").split("\t") for l in f if l.strip()]

    def num(v):
        try:
            return float(v)
        except ValueError:
            return math.nan

    cols = {h: [num(r[i]) if i < len(r) else math.nan for r in rows] for i, h in enumerate(head)}
    return cols, len(rows)


def mean(xs):
    xs = [x for x in xs if not math.isnan(x)]
    return sum(xs) / len(xs) if xs else math.nan


def members(cols, prefix):
    return sorted(k for k in cols if k.startswith(prefix + "Member_"))


def per_second(path, period_us):
    cols, n = load(path)
    mw = members(cols, "wchar_kb_")
    mc = members(cols, "cpu_")
    mt = members(cols, "nr_throttled_")
    mm = members(cols, "throttled_ms_")
    mr = members(cols, "rss_mb_")
    ma = members(cols, "sendq_attempted_kb_")
    md = members(cols, "sendq_dropped_kb_")
    get = lambda name, s: cols.get(name, [math.nan] * n)[s]
    periods = 1e6 / period_us
    out = []
    for s in range(n):
        w = sum(cols[c][s] for c in mw) * 1024 if mw else math.nan
        freezes = []
        for t, m in zip(mt, mm):
            if cols[t][s] > 0:
                freezes.append(cols[m][s] / cols[t][s])
        att = sum(cols[c][s] for c in ma) * 1024 / RELAY_BYTES if ma else math.nan
        dropx = sum(cols[c][s] for c in md) * 1024 / RELAY_BYTES if md else math.nan
        src_w = get("wchar_kb_Source", s) * 1024 / RELAY_BYTES / MEMBERS
        wire = get("wire_n", s)
        resent = get("resent_n", s)
        out.append({
            "rounds": get("rounds", s),
            "first_in": wire - resent,
            "wire_in": wire,
            "resent_in": resent,
            "rep": get("unacked_n", s),
            "unacked": get("unacked_v", s),
            "wire_x": w / RELAY_BYTES,
            "cpu": mean([cols[c][s] for c in mc]),
            "thr": mean([cols[c][s] for c in mt]) / periods,
            "frz": mean(freezes) if freezes else 0.0,
            "rss": mean([cols[c][s] for c in mr]),
            "att_x": att,
            "drop_x": dropx,
            "src_w": src_w,
        })
    return out


def summarize(path, quota, period_us, windows):
    secs = per_second(path, period_us)
    n = len(secs)
    print(f"== {path}")
    hdr = ["window", "rounds/s", "first_in", "resent_in", "rep", "unacked", "wire_x", "x/off",
           "drop~", "att_x", "drop_x", "intake", "cpu/q", "thr", "frz_ms", "rss"]
    print("\t".join(hdr))
    for a, b in zip(windows, windows[1:] + [n]):
        if a >= n or b <= a:
            break
        w = secs[a:b]
        m = {k: mean([x[k] for x in w]) for k in w[0]}
        last = [x["unacked"] for x in w if not math.isnan(x["unacked"])]
        ratio = m["wire_x"] / (PEERS * MEMBERS * m["rounds"]) if m["rounds"] else math.nan
        drop = m["wire_in"] - m["wire_x"] if not math.isnan(m["wire_in"]) else math.nan
        print("\t".join([
            f"[{a},{b})", f"{m['rounds']:.0f}", f"{m['first_in']:.0f}", f"{m['resent_in']:.0f}",
            f"{m['rep']:.1f}", f"{last[-1]:.0f}" if last else "-", f"{m['wire_x']:.0f}",
            f"{ratio:.2f}", f"{drop:.0f}",
            "-" if math.isnan(m["att_x"]) else f"{m['att_x']:.0f}",
            "-" if math.isnan(m["drop_x"]) else f"{m['drop_x']:.0f}",
            f"{m['src_w'] / m['rounds']:.2f}" if m["rounds"] else "-",
            f"{m['cpu'] / quota:.2f}", f"{m['thr']:.2f}",
            f"{m['frz']:.1f}", f"{m['rss']:.1f}",
        ]))


if __name__ == "__main__":
    args = sys.argv[1:]
    quota, period, windows = 0.05, 20000.0, [0, 5, 10, 15, 20, 30, 45, 60, 75]
    while args and args[0].startswith("--"):
        k, v = args.pop(0)[2:].split("=", 1)
        if k == "quota":
            quota = float(v)
        elif k == "period-us":
            period = float(v)
        elif k == "windows":
            windows = [int(x) for x in v.split(",")]
    for p in args:
        summarize(p, quota, period, windows)
