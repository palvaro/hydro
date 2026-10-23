import os,re,sys,collections
root=sys.argv[1]
mods=collections.OrderedDict()
budget={'observe':600,'spec':350,'fault':250,'lineage':700,'provenance (excl. sensitivity)':1600,'sensitivity':0,'search':650,'check':0,'check/brute_force':0,'report':600,'lamport':450,'lib':0}
def modof(rel):
    if rel=='lib.rs': return 'lib'
    if rel=='spec.rs': return 'spec'
    if rel.startswith('provenance/sensitivity'): return 'sensitivity'
    if rel.startswith('provenance'): return 'provenance (excl. sensitivity)'
    if rel=='check/brute_force.rs': return 'check/brute_force'
    return rel.split('/')[0]
tot=collections.defaultdict(lambda:[0,0,0,0])
for dp,dn,fn in os.walk(root):
    for f in fn:
        if not f.endswith('.rs'): continue
        p=os.path.join(dp,f); rel=os.path.relpath(p,root)
        if re.search(r'tests?\.rs$',f): 
            tot['(test files)'][3]+=sum(1 for _ in open(p)); continue
        L=open(p).read().split('\n')
        if L and L[-1]=='': L=L[:-1]
        # cut inline test module
        for i in range(len(L)-1):
            if L[i].strip()=='#[cfg(test)]' and re.match(r'\s*mod tests \{',L[i+1]):
                tot['(test files)'][3]+=len(L)-i; L=L[:i]; break
        m=modof(rel)
        for l in L:
            s=l.strip()
            if not s: tot[m][2]+=1
            elif s.startswith('///') or s.startswith('//!'): tot[m][1]+=1
            elif s.startswith('//'): tot[m][1]+=1
            else: tot[m][0]+=1
            tot[m][3]+=1
print(f"| module | total | code | comments | blank | plan | overrun (total) |")
print("|---|---|---|---|---|---|---|")
T=[0,0,0,0]
for m in list(budget)+['(test files)']:
    if m not in tot: continue
    c,d,b,t=tot[m]; bud=budget.get(m,'')
    if m!='(test files)':
        for i,v in enumerate((c,d,b,t)): T[i]+=v
        print(f"| {m} | {t} | {c} | {d} | {b} | {bud or '—'} | {('+' if t-(bud or 0)>=0 else '')}{t-(bud or 0)} |")
    else:
        print(f"| tests (files + inline) | {t} | | | | | |")
print(f"| **logic total** | {T[3]} | {T[0]} | {T[1]} | {T[2]} | 5100 | +{T[3]-5100} |")
