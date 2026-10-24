#!/usr/bin/env python3
"""Baseline CPU headroom from the existing traces: per-window throttling/stall table, plus a fit of
member CPU against load over barely-throttled seconds. Run from the repo root."""
import csv,sys,glob,statistics as st,os
base='design_docs/sedge_outside_capacity/relay_broadcast_amplification/traces'
mu={'pe':126.1,'rr':136.3}
def load(p):
    with open(p) as f: return list(csv.DictReader(f,delimiter='\t'))
def num(x):
    try: return float(x)
    except: return float('nan')
rows=[]
for p in sorted(glob.glob(base+'/*/*.tsv')):
    g=p.split('/')[-2]; f=float(os.path.basename(p)[6:].split('.trial')[0])
    m=mu['pe'] if g.startswith('pe') else mu['rr']
    T=load(p)
    ctl = 'ctl' in g
    wins=[(2,10)] if not ctl else [(2,10),(10,40),(40,75)]
    if not ctl: wins += [(45,75)]
    for a,b in wins:
        S=[r for r in T if a<=num(r['second'])<b]
        if not S: continue
        mem=[k[4:] for k in T[0] if k.startswith('cpu_Member')]
        cpu=[];thr=[];tms=[];stall=[]
        for r in S:
            for k in mem:
                c=num(r['cpu_'+k]); n=num(r['nr_throttled_'+k]); t=num(r['throttled_ms_'+k])
                if c==c: cpu.append(c)
                if n==n: thr.append(n/10); tms.append(t)
                if n==n and n>0: stall.append(t/n)
        rnd=st.mean(num(r['rounds']) for r in S)
        res=st.mean(num(r['resent_n']) for r in S if num(r['resent_n'])==num(r['resent_n'])) if any(num(r['resent_n'])==num(r['resent_n']) for r in S) else float('nan')
        c=st.mean(cpu)
        rows.append((g,f,a,b,rnd,rnd/m,c,st.mean(thr),st.mean(tms),st.mean(stall) if stall else 0, c*1000/rnd if rnd else 0,res))
print('group\tf\twin\trounds/s\tload/mu\tmember_cpu\tthr_frac\tthrottled_ms/s\tms_per_stall\tcpu_ms/round\tresent/s')
for r in rows:
    print('%s\t%g\t[%d,%d)\t%.1f\t%.2f\t%.4f\t%.2f\t%.0f\t%.1f\t%.3f\t%.0f'%(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],r[11]))

# --- fit over barely-throttled seconds (nr_throttled <= 1 per second) ---
pts=[]
for p in glob.glob(base+'/*/*.tsv'):
    T=list(csv.DictReader(open(p),delimiter='\t'))
    for r in T:
        s=num(r['second'])
        if s<2: continue
        if 'burst' in p and 10<=s<45: continue
        if 'pe_burst' in p and s>=10: continue
        if 'pe_ctl_f1.25' in p: continue
        if num(r['resent_n'])!=num(r['resent_n']) or num(r['resent_n'])>100: continue
        for k in r:
            if k.startswith('cpu_Member'):
                m=k[4:]; n=num(r['nr_throttled_'+m]); c=num(r[k])
                if n==n and c==c and n<=1: pts.append((num(r['rounds']),c*1000))
print(len(pts))
import statistics as st
xs=[p[0] for p in pts]; ys=[p[1] for p in pts]
mx,my=st.mean(xs),st.mean(ys)
b=sum((x-mx)*(y-my) for x,y in pts)/sum((x-mx)**2 for x in xs); a=my-b*mx
print('cpu_ms/s = %.2f + %.3f * rounds/s  (x range %.0f-%.0f)'%(a,b,min(xs),max(xs)))
for lo,hi in [(0,25),(25,32),(32,40),(40,60)]:
    s=[y/x for x,y in pts if lo<=x<hi and x>0]
    if s: print(lo,hi,len(s),'%.3f ms/round'%st.median(s), 'cpu ms/s median %.2f'%st.median([y for x,y in pts if lo<=x<hi and x>0]))
