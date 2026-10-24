#!/usr/bin/env python3
"""Compare LDFI_RECORD tsvs: positional (A) vs auto-open (B). Pairs by (test,label,occurrence)."""
import sys, collections
COLS = ["test","label","model","exploration","verdict","norm","raw","hyps","scheds","wall","rss_mib","note","novel"]
def load(p):
    rows = collections.OrderedDict(); occ = collections.Counter()
    for line in open(p):
        f = line.rstrip("\n").split("\t")
        r = dict(zip(COLS, f))
        k = (r["test"], r["label"]); i = occ[k]; occ[k] += 1
        r["hyps"]=int(r["hyps"]); r["scheds"]=int(r["scheds"]); r["wall"]=float(r["wall"]); r["novel"]=int(r["novel"])
        rows[k+(i,)] = r
    return rows
A = load(sys.argv[1]); B = load(sys.argv[2])
short = lambda t: t.split("::")[-1] if len(t)<60 else "::".join(t.split("::")[-2:])
common = [k for k in A if k in B]
print(f"rows A={len(A)} B={len(B)} paired={len(common)}")
for k in A:
    if k not in B: print("  only in A:", k)
for k in B:
    if k not in A: print("  only in B:", k)
# verdicts
agree = [k for k in common if A[k]["verdict"]==B[k]["verdict"] and A[k]["norm"]==B[k]["norm"]]
vagree = [k for k in common if A[k]["verdict"]==B[k]["verdict"]]
print(f"\nverdict agree: {len(vagree)}/{len(common)}; verdict+normalized cex agree: {len(agree)}/{len(common)}")
print("\n## differences (verdict or normalized cex)")
for k in common:
    a,b=A[k],B[k]
    if (a["verdict"],a["norm"])!=(b["verdict"],b["norm"]):
        print(f"| {short(k[0])} | {k[1]}#{k[2]} | A {a['verdict']} {a['norm']} | B {b['verdict']} {b['norm']} ({b['model']}) |")
red = lambda r: r["note"].startswith("one schedule per hypothesis:")
R = [k for k in common if red(B[k])]
print(f"\n## reduced rows in B: {len(R)}  (B models: {collections.Counter(B[k]['model'] for k in common)})")
R.sort(key=lambda k: A[k]["scheds"]-B[k]["scheds"], reverse=True)
print("| test | label | A sched/hyp/wall | B sched/hyp/wall | B novel |")
for k in R[:12]:
    a,b=A[k],B[k]
    print(f"| {short(k[0])} | {k[1]}#{k[2]} | {a['scheds']}/{a['hyps']}/{a['wall']:.2f} | {b['scheds']}/{b['hyps']}/{b['wall']:.2f} | {b['novel']} |")
def tot(keys, D): return sum(D[k]["scheds"] for k in keys), sum(D[k]["hyps"] for k in keys), sum(D[k]["wall"] for k in keys)
for name, keys in [("all paired", common), ("reduced", R), ("not reduced", [k for k in common if k not in R])]:
    a=tot(keys,A); b=tot(keys,B)
    print(f"\ntotals {name} ({len(keys)}): A scheds={a[0]} hyps={a[1]} wall={a[2]:.1f}s | B scheds={b[0]} hyps={b[1]} wall={b[2]:.1f}s")
print(f"totals all rows (unpaired incl.): A wall={sum(r['wall'] for r in A.values()):.1f}s B wall={sum(r['wall'] for r in B.values()):.1f}s")
print("\n## soundness: reduced AND novel>0")
for k in R:
    if B[k]["novel"]>0: print("  FLAG", k, B[k]["novel"], B[k]["verdict"])
print("## any B row with novel>0")
for k,b in B.items():
    if b["novel"]>0: print(f"  {short(k[0])} {k[1]}#{k[2]} novel={b['novel']} reduced={red(b)} model={b['model']} verdict={b['verdict']}")
print("\n## specific")
for k,b in B.items():
    if k[1].startswith("abd.rs:150") or "commit_quorum_bug" in k[0]:
        a=A.get(k)
        print(f"  {k} B model={b['model']} reduced={red(b)} novel={b['novel']} verdict={b['verdict']} sched={b['scheds']} | A verdict={a and a['verdict']} sched={a and a['scheds']}\n     note: {b['note'][:200]}")
