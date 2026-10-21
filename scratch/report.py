import json,os,re
T=os.environ['TMPDIR']
d=json.load(open(T+'/parsed.json'))
order=[l.split()[1] for l in open(T+'/tests.txt')]
def key(t): return t.split('::')[-3]+'::'+t.split('::')[-1]
equiv={'Certificate':'C','Exhausted':'C','Counterexample':'X','Vacuous':'V','Budget':'B'}
short={'Certificate':'cert','Exhausted':'exhausted','Counterexample':'cex','Vacuous':'vacuous','Budget':'budget'}
rows=[];tot={'L':[0,0],'B':[0,0]};wins={'L':0,'B':0,'tie':0};dis=[];cnt=0
for t in order:
    k=key(t); v=d[k]; L=v['lineage']['searches']; B=v['blind']['searches']
    assert len(L)==len(B),(k,len(L),len(B))
    for i,(l,b) in enumerate(zip(L,B)):
        cnt+=1
        assert l['label']==b['label']
        ag = equiv[l['verdict']]==equiv[b['verdict']]
        if not ag: dis.append((k,l['label']))
        tot['L'][0]+=l['hyp'];tot['B'][0]+=b['hyp'];tot['L'][1]+=l['ms'];tot['B'][1]+=b['ms']
        w='tie' if l['hyp']==b['hyp'] else ('L' if l['hyp']<b['hyp'] else 'B'); wins[w]+=1
        lab=l['label'] if l['label']!='(spec)' else f'spec #{i+1}'
        cexs=''
        if l['cex'] or b['cex']:
            cexs=f"L {l['cex'] or '-'} / B {b['cex'] or '-'}"
        rows.append(f"| `{k}` | `{lab}` | {short[l['verdict']]} / {short[b['verdict']]}{'' if ag else ' **DISAGREE**'} | {l['hyp']} / {b['hyp']} | {l['sch']} / {b['sch']} | {l['ms']} / {b['ms']} | {l['uni']} / {b['uni']} | {w} | {cexs.replace('|','/')} |")
print(cnt,tot,wins,dis)
open(T+'/rows.md','w').write('\n'.join(rows)+'\n')
