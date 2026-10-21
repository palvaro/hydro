import re,sys,glob,os,itertools
from collections import defaultdict
OUT=os.environ['OUT']
checked={'ec_demo_leader_merge':'replica','ec_demo_multi_paxos':'learner','ldfi_all_labels_break':'replica',
 'ldfi_all_labels_with':'participant','all_labels_reports_the_register':'replica'}
def split_items(s):
    items=[];depth=0;cur=''
    for ch in s:
        if ch in '({[': depth+=1
        if ch in ')}]': depth-=1
        if ch==',' and depth==0: items.append(cur.strip()); cur=''
        else: cur+=ch
    if cur.strip(): items.append(cur.strip())
    return items
def universe_items(line):
    m=re.match(r'ldfi: universe \{(.*?)\}; preserve points',line)
    if not m: m=re.match(r'ldfi: universe \{(.*)\}',line)
    return split_items(m.group(1)) if m and m.group(1) else []
def count(items,bounds,ck):
    crashes=defaultdict(int); om=0
    for it in items:
        if it.startswith('omit('): om+=1
        elif it.startswith('crash('):
            node=it[6:].split(' ')[0]; crashes[node]+=1
    budget=bounds.get('budget'); N=bounds['N']; cap=bounds['cap']
    # dp over nodes: dict (total,checked)->count
    dp={(0,0):1}
    for node,k in crashes.items():
        isck=node.split('@')[0]==ck if cap is not None else False
        nd=defaultdict(int)
        for (t,c),v in dp.items():
            nd[(t,c)]+=v
            nd[(t+1,c+(1 if isck else 0))]+=v*k
        dp=nd
    tot=0
    for (t,c),v in dp.items():
        if budget is not None and t>budget: continue
        if t>=N: continue
        if cap is not None and c>cap: continue
        tot+=v
    return tot*(2**om), dict(crashes), om
rows=[]
for f in sorted(glob.glob(OUT+'/*.txt')):
    name=os.path.basename(f)[:-4]
    ck='member'
    for k,v in checked.items():
        if name.startswith(k): ck=v
    lines=open(f).read().split('\n')
    searches=[];cur=None
    for l in lines:
        if l.startswith('ldfi: crash bounds'):
            cur={'label':None,'bounds':l,'univ':[],'verdict':None};searches.append(cur)
            for back in range(1,6):
                pass
        elif cur is not None and l.startswith('ldfi: universe'):
            cur['univ']=universe_items(l)
        elif cur is not None and (l.startswith('LDFI:') or l.startswith('uniform(') or 'EventualConsistency at' in l) and cur['verdict'] is None:
            cur['verdict']=l[:220]
    # labels: preceding "hydrofrac: searching"
    labs=[l.split('searching ')[1] for l in lines if l.startswith('hydrofrac: searching')]
    for i,s in enumerate(searches):
        b=s['bounds']
        m=re.search(r'at most (\d+) crash',b); budget=int(m.group(1)) if m else None
        N=int(re.search(r'not all (\d+) nodes',b).group(1))
        m=re.search(r"at most (\d+) of the checked cluster's (\d+)",b); cap=int(m.group(1)) if m else None
        sz,cr,om=count(s['univ'],{'budget':budget,'N':N,'cap':cap},ck)
        rows.append((name, i, sz, len(s['univ']), cr, om, budget, N, cap, s['verdict']))
for r in rows:
    print(f"{r[0]} [search {r[1]}] naive={r[2]} universe={r[3]} crashpts={r[4]} omits={r[5]} budget={r[6]} N={r[7]} cap={r[8]}\n    -> {r[9]}")
