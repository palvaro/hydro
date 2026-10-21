import re,glob,os,sys,json
def topcount(s):
    # count top-level comma-separated items in "{...}"
    s=s.strip()
    if not s.startswith('{'): return None
    d=0;n=0;cur=False
    for ch in s[1:]:
        if ch in '{([': d+=1
        elif ch in '})]':
            if d==0: break
            d-=1
        elif ch==',' and d==0: n+=1; cur=False; continue
        if not ch.isspace(): cur=True
    return n+(1 if cur else 0)
res={}
for f in sorted(glob.glob(os.environ['TMPDIR']+'/logs/*__*.log')):
    b=os.path.basename(f)[:-4]; strat,mod,test=b.split('__',2)
    label='(spec)'; uni=None; bounds=None; cex=None; searches=[]
    lines=open(f,errors='replace').read().splitlines()
    for l in lines:
        m=re.search(r'hydrofrac: searching EventualConsistency at (\S+)',l)
        if m: label=m.group(1); uni=None; bounds=None; cex=None; continue
        m=re.search(r'(ldfi|blind): universe size (\d+)',l)
        if m: uni=int(m.group(2)); continue
        m=re.match(r'ldfi: universe (\{.*?)(; preserve points|$)',l)
        if m: uni=topcount(m.group(1)); continue
        m=re.search(r'(ldfi|blind): crash bounds: (.*)',l)
        if m: bounds=m.group(2); continue
        m=re.search(r'goal violated after \d+ hypothesis run\(s\) by (\{.*)',l)
        if m:
            if searches and searches[-1]['cex'] is None and searches[-1]['verdict']=='Counterexample': searches[-1]['cex']=m.group(1)[:150]
            continue
        m=re.search(r'MEASURE strategy=(\w+) verdict=(\w+) hypotheses=(\d+) schedules=(\d+) elapsed_ms=(\d+)',l)
        if m:
            searches.append(dict(label=label,verdict=m.group(2),hyp=int(m.group(3)),sch=int(m.group(4)),ms=int(m.group(5)),uni=uni,bounds=bounds,cex=cex))
            label='(spec)'; uni=None; cex=None
    tail=[l for l in lines if l.startswith('RC ')]
    rc,wall=(tail[-1].split()[1],float(tail[-1].split()[3])) if tail else ('running',None)
    nc=[re.match(r'EventualConsistency at (\S+) .*not checked: (.*)',l) for l in lines]
    nc=sorted(set((m.group(1),m.group(2)) for m in nc if m))
    res.setdefault((mod,test),{})[strat]=dict(searches=searches,rc=rc,wall=wall,notchecked=nc)
json.dump({f'{k[0]}::{k[1]}':v for k,v in res.items()},open(os.environ['TMPDIR']+'/parsed.json','w'),indent=1)
for (mod,test),v in res.items():
    print(f'## {mod}::{test}')
    for s,d in v.items():
        print(f'  {s} rc={d["rc"]} wall={d["wall"]} nc={len(d["notchecked"])}')
        for x in d['searches']:
            print(f'    {x["label"]:28} {x["verdict"]:14} hyp={x["hyp"]:6} sch={x["sch"]:7} ms={x["ms"]:8} U={x["uni"]}')
