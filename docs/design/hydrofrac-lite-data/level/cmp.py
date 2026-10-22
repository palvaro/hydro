import sys,collections
def load(p):
    d=collections.OrderedDict(); c=collections.Counter()
    for l in open(p):
        f=l.rstrip('\n').split('\t')
        if len(f)<10: continue
        k=(f[0],f[1]); i=c[k]; c[k]+=1
        d[k+(i,)]=f
    return d
P=load(sys.argv[1]); E=load(sys.argv[2])
print("rows",len(P),len(E),"unpaired",set(P)^set(E))
tot=collections.defaultdict(lambda:[0,0,0.0,0,0,0.0]); agree=0; n=0
V={'certificate':'cert','counterexample':'cx'}
for k,p in P.items():
    e=E.get(k)
    if not e: continue
    n+=1
    samp='Fuzz' in p[3]
    t=k[0].split('::')[-1]
    vp,ve=V.get(p[4],p[4]),V.get(e[4],e[4])
    agree+= vp==ve
    flag='' if vp==ve else ' <<VERDICT'
    cxd='' if p[5]==e[5] else f' cxP={p[5]} cxE={e[5]}'
    print(f"{t[:48]}|{k[1]}{'#'+str(k[2]) if k[2] else ''}|{'S' if samp else ''}|{vp}/{p[7]}/{p[8]}/{p[9]}|{ve}/{e[7]}/{e[8]}/{e[9]}{flag}{cxd}")
    for g in (['all'] + ([] if samp else ['det'])):
        T=tot[g]; T[0]+=int(p[7]);T[1]+=int(p[8]);T[2]+=float(p[9]);T[3]+=int(e[7]);T[4]+=int(e[8]);T[5]+=float(e[9])
print("agree",agree,"/",n)
for g,T in tot.items(): print(g,"P hyp/sch/wall",T[0],T[1],round(T[2],2),"E",T[3],T[4],round(T[5],2))
