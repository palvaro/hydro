#!/usr/bin/env python3
"""Samples every relay_broadcast Member process once a second: RSS, CPU time, and the kernel
queues (rx: bytes received but not yet read by the member; tx: bytes written but not yet acked
by the peer) of each of its TCP sockets. Writes one JSON object per second."""
import json, os, re, sys, time

out = open(sys.argv[1], "w")
pat = sys.argv[2] if len(sys.argv) > 2 else "relay_broadcast__Member"
t0 = time.time()


def members():
    res = []
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            cmd = open(f"/proc/{p}/cmdline", "rb").read().split(b"\0")[0].decode()
        except Exception:
            continue
        if pat in cmd and "docker" not in cmd and "/examples/" in cmd:
            res.append(int(p))
    return sorted(res)


def tcp(pid=None):
    rows = {}
    base = f"/proc/{pid}/net" if pid else "/proc/net"
    for f in (base + "/tcp", base + "/tcp6"):
        try:
            lines = open(f).read().splitlines()[1:]
        except Exception:
            continue
        for l in lines:
            x = l.split()
            lport = int(x[1].split(":")[1], 16)
            rport = int(x[2].split(":")[1], 16)
            st = x[3]
            tx, rx = (int(v, 16) for v in x[4].split(":"))
            rows[x[9]] = (lport, rport, st, tx, rx)
    return rows


while True:
    pids = members()
    if not pids:
        if time.time() - t0 > 3600:
            break
        time.sleep(0.5)
        continue
    rows = tcp(pids[0])
    errs = []
    sample = {"t": round(time.time() - t0, 2), "members": []}
    listen = {}
    per = []
    for pid in pids:
        try:
            st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
            cpu = (int(st[11]) + int(st[12])) / os.sysconf("SC_CLK_TCK")
            rss = 0
            for l in open(f"/proc/{pid}/status"):
                if l.startswith("VmRSS"):
                    rss = int(l.split()[1]) // 1024
            socks = []
            for fd in os.listdir(f"/proc/{pid}/fd"):
                try:
                    link = os.readlink(f"/proc/{pid}/fd/{fd}")
                except Exception as e:
                    errs.append(repr(e)[:80])
                    continue
                if link.startswith("socket:") and len(errs) < 3 and link[8:-1] not in rows:
                    errs.append("unmatched " + link)
                m = re.match(r"socket:\[(\d+)\]", link)
                if m and m.group(1) in rows:
                    socks.append(rows[m.group(1)])
        except Exception as e:
            errs.append(repr(e)[:80])
            continue
        for s in socks:
            if s[2] == "0A":
                listen[s[0]] = pid
        per.append((pid, cpu, rss, socks))
    for pid, cpu, rss, socks in per:
        est = [s for s in socks if s[2] == "01"]
        inbound = [s for s in est if s[0] in listen and listen[s[0]] == pid]
        outbound = [s for s in est if not (s[0] in listen and listen[s[0]] == pid)]
        sample["members"].append({
            "pid": pid, "cpu_s": cpu, "rss_mb": rss,
            # incoming connections, grouped by the listening port they arrived on
            "in": {str(p): [sum(s[4] for s in inbound if s[0] == p), sum(s[3] for s in inbound if s[0] == p)]
                   for p in sorted({s[0] for s in inbound})},
            # outgoing connections, grouped by the remote (listening) port they go to
            "out_tx": sum(s[3] for s in outbound), "out_rx": sum(s[4] for s in outbound),
            "out_by_rport": {str(p): sum(s[3] for s in outbound if s[1] == p) for p in sorted({s[1] for s in outbound})},
        })
    sample["errs"] = errs[:3]
    sample["nrows"] = len(rows)
    out.write(json.dumps(sample) + "\n")
    out.flush()
    time.sleep(max(0, 1 - (time.time() - t0) % 1))
