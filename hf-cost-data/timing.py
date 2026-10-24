import sys, collections, statistics, os
T=os.environ['TMPDIR']
def ren(t):
    return t.replace('::both_agree_','::brute_force_agrees_')
def load(p):
    rows=collections.OrderedDict(); cnt=collections.Counter()
    if not os.path.exists(p): return None
    for line in open(p):
        t,l,v,s,sc,w=line.rstrip('\n').split('\t')
        t=ren(t); k=(t,l); i=cnt[k]; cnt[k]+=1
        rows[(t,l,i)]=(v,int(s),int(sc),int(w)/1000)
    return rows
runs={side:[r for r in (load(f"{T}/dump-{side}-t{n}.tsv") for n in (1,2,3)) if r] for side in ('lite','races')}
keys=list(runs['races'][0].keys())
for k in keys:
    R=[r[k] for r in runs['races'] if k in r]
    L=[r[k] for r in runs['lite'] if k in r]
    if not L or not R: 
        print("UNMATCHED", k); continue
    if R[0][1]==0: continue
    def summ(X):
        vs=set(x[0] for x in X); ss=set((x[1],x[2]) for x in X)
        w=[x[3] for x in X]
        return vs, ss, statistics.median(w), min(w), max(w)
    lv,ls,lm,lmin,lmax=summ(L); rv,rs,rm,rmin,rmax=summ(R)
    (l1,l2),(r1,r2)=list(ls)[0],list(rs)[0]
    det = "det" if len(ls)==1 and len(rs)==1 and len(lv)==1 and len(rv)==1 else f"NONDET L{ls}{lv} R{rs}{rv}"
    print(f"{k[0].split('::')[-1]} | {k[1]}#{k[2]} | lite {list(lv)[0]} {l1}/{l2} {lm:.0f}ms [{lmin:.0f}-{lmax:.0f}] n={len(L)} | races {list(rv)[0]} {r1}/{r2} {rm:.0f}ms [{rmin:.0f}-{rmax:.0f}] n={len(R)} | sets {r1/l1:.2f}x sched {r2/l2:.2f}x wall {rm/lm:.1f}x | {det}")
