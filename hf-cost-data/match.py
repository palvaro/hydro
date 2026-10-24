import sys, collections
def load(p, ren=lambda t:t):
    rows=collections.OrderedDict()
    for line in open(p):
        t,l,v,s,sc,w=line.rstrip('\n').split('\t')
        t=ren(t)
        rows.setdefault((t,l),[]).append((v,int(s),int(sc),int(w)/1000))
    return rows
def ren(t):
    t=t.replace('::both_agree_','::brute_force_agrees_')
    if t.endswith('_lite') and 'races_targets' in t: t=t[:-5]
    return t
L=load(sys.argv[1],ren); R=load(sys.argv[2])
out=[]
for k,rs in R.items():
    ls=L.get(k,[])
    for i,r in enumerate(rs):
        l=ls[i] if i<len(ls) else None
        out.append((k,i,l,r))
only_l=[(k,i) for k,ls in L.items() for i in range(len(ls)) if i>=len(R.get(k,[]))]
mode=sys.argv[3] if len(sys.argv)>3 else 'all'
tot=[0,0,0,0]
for k,i,l,r in out:
    if l is None:
        if mode=='all': print(f"RACES-ONLY\t{k[0]}\t{k[1]}#{i}\t{r[0]}\t{r[1]}/{r[2]}")
        continue
    sx=r[1]/l[1] if l[1] else float('nan'); scx=r[2]/l[2] if l[2] else float('nan')
    if l[1] and r[1]: tot[0]+=l[1];tot[1]+=r[1];tot[2]+=l[2];tot[3]+=r[2]
    flag='**' if (sx>2 or scx>2) else ''
    if mode=='all' or flag:
        print(f"{flag}\t{k[0]}\t{k[1]}#{i}\tlite {l[0]} {l[1]}/{l[2]} {l[3]:.0f}ms\traces {r[0]} {r[1]}/{r[2]} {r[3]:.0f}ms\t{sx:.2f}\t{scx:.2f}")
if mode=='all':
    for k,i in only_l: print(f"LITE-ONLY\t{k[0]}\t{k[1]}#{i}\t{L[k][i][0]}")
print("TOTAL sets",tot[0],tot[1],tot[1]/tot[0],"sched",tot[2],tot[3],tot[3]/tot[2])
