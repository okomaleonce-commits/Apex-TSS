import json,math,re,unicodedata
R=json.load(open('results2.json')); F=json.load(open('fixt2.json'))
def norm(s):
    s=unicodedata.normalize('NFKD',s).encode('ascii','ignore').decode().lower()
    return re.sub(r'[^a-z0-9]','',s)
LG={}
for lg,gs in R.items():
    teams={}; THG=TAG=0
    for g in gs:
        THG+=g['hg']; TAG+=g['ag']
        for side,t in (('h',g['h']),('a',g['a'])):
            d=teams.setdefault(t,{'gp':0,'gf':0,'ga':0,'pts':0,'hgp':0,'hgf':0,'hga':0,'agp':0,'agf':0,'aga':0,'res':[]})
            gf,ga=(g['hg'],g['ag']) if side=='h' else (g['ag'],g['hg'])
            d['gp']+=1; d['gf']+=gf; d['ga']+=ga
            d['pts']+= 3 if gf>ga else (1 if gf==ga else 0)
            d[side+'gp']+=1; d[side+'gf']+=gf; d[side+'ga']+=ga
            d['res'].append(('W' if gf>ga else 'D' if gf==ga else 'L', f"{gf}-{ga}", 'H' if side=='h' else 'A'))
    n=len(gs); T_obs=(THG+TAG)/n
    w2=n/(n+35.0); T=w2*T_obs+(1-w2)*2.85
    w1=n/(n+70.0); share=w1*((THG/n)/T_obs)+(1-w1)*0.545
    LG[lg]={'teams':teams,'n':n,'HG':T*share,'AG':T*(1-share),'Lm':T/2.0}
def find(lg,name):
    ts=LG[lg]['teams']
    if name in ts: return name
    nn=norm(name)
    for t in ts:
        if norm(t)==nn: return t
    c=[t for t in ts if nn in norm(t) or norm(t) in nn]
    if len(c)==1: return c[0]
    def tri(s): return {s[i:i+3] for i in range(max(1,len(s)-2))}
    best=None;bs=0
    for t in ts:
        s=len(tri(norm(t))&tri(nn))/max(1,len(tri(norm(t))|tri(nn)))
        if s>bs: bs=s;best=t
    return best if bs>0.3 else None
K=4.5; KV=4.0
def ratings(lg,t):
    D=LG[lg]; d=D['teams'][t]; L=D['Lm']; Lh,La=D['HG'],D['AG']
    att=(d['gf']+K*L)/((d['gp']+K)*L); dfn=(d['ga']+K*L)/((d['gp']+K)*L)
    ath=(d['hgf']+KV*Lh)/((d['hgp']+KV)*Lh) if d['hgp'] else 1.0
    dfh=(d['hga']+KV*La)/((d['hgp']+KV)*La) if d['hgp'] else 1.0
    ata=(d['agf']+KV*La)/((d['agp']+KV)*La) if d['agp'] else 1.0
    dfa=(d['aga']+KV*Lh)/((d['agp']+KV)*Lh) if d['agp'] else 1.0
    return {'att_h':att**0.75*ath**0.25,'def_h':dfn**0.75*dfh**0.25,
            'att_a':att**0.75*ata**0.25,'def_a':dfn**0.75*dfa**0.25,'d':d}
RHO=-0.08
def dc(lh,la,N=11):
    ph=[math.exp(-lh)*lh**i/math.factorial(i) for i in range(N)]
    pa=[math.exp(-la)*la**i/math.factorial(i) for i in range(N)]
    M=[[ph[i]*pa[j] for j in range(N)] for i in range(N)]
    M[0][0]*=(1-lh*la*RHO); M[0][1]*=(1+lh*RHO); M[1][0]*=(1+la*RHO); M[1][1]*=(1-RHO)
    s=sum(map(sum,M)); return [[v/s for v in r] for r in M]
def res1x2(M):
    N=len(M); p1=sum(M[i][j] for i in range(N) for j in range(N) if i>j)
    px=sum(M[i][i] for i in range(N)); return p1,px,1-p1-px
def markets(M):
    N=len(M); m={}
    m['1'],m['X'],m['2']=res1x2(M)
    m['1X']=m['1']+m['X']; m['X2']=m['X']+m['2']; m['12']=m['1']+m['2']
    for L in (1.5,2.5,3.5):
        m['O%.1f'%L]=sum(M[i][j] for i in range(N) for j in range(N) if i+j>L); m['U%.1f'%L]=1-m['O%.1f'%L]
    m['BTTS']=sum(M[i][j] for i in range(1,N) for j in range(1,N)); m['NOBTTS']=1-m['BTTS']
    m['H-1']=sum(M[i][j] for i in range(N) for j in range(N) if i-j>=2)
    m['A-1']=sum(M[i][j] for i in range(N) for j in range(N) if j-i>=2)
    m['H+1']=sum(M[i][j] for i in range(N) for j in range(N) if i-j>=-1)
    m['A+1']=sum(M[i][j] for i in range(N) for j in range(N) if j-i>=-1)
    m['H_CS']=sum(M[i][0] for i in range(N)); m['A_CS']=sum(M[0][j] for j in range(N))
    return m
ODDS=json.load(open('odds2.json'))
def tri(s): return {s[i:i+3] for i in range(max(1,len(s)-2))}
def oddsfor(h,a):
    nh,na=norm(h),norm(a); best=None;bs=0
    for o in ODDS:
        s1=len(tri(norm(o['h']))&tri(nh))/max(1,len(tri(norm(o['h']))|tri(nh)))
        s2=len(tri(norm(o['a']))&tri(na))/max(1,len(tri(norm(o['a']))|tri(na)))
        s=(s1+s2)/2
        if s>bs: bs=s;best=o
    return (best,bs) if bs>0.48 else (None,bs)
W=0.62
def fit(tg):
    best=None
    for t in [x/40 for x in range(48,200)]:
        for sup in [x/40 for x in range(-80,81)]:
            lh=(t+sup)/2; la=(t-sup)/2
            if lh<=0.05 or la<=0.05: continue
            p=res1x2(dc(lh,la)); e=sum((p[k]-tg[k])**2 for k in range(3))
            if best is None or e<best[0]: best=(e,lh,la)
    return best[1],best[2]
ORDER=['england','italy','spain','france','germany','netherlands','portugal','scotland',
       'netherlands2','germany2','spain2','italy2','belgium','turkey','denmark']
OUT=[];i=0
for lg in ORDER:
    for h,a in F.get(lg,[]):
        i+=1
        fh,fa=find(lg,h),find(lg,a)
        if not fh or not fa: print("MISS",lg,h,a); continue
        rh,ra=ratings(lg,fh),ratings(lg,fa); D=LG[lg]
        lh=D['HG']*rh['att_h']*ra['def_a']; la=D['AG']*ra['att_a']*rh['def_h']
        pm=res1x2(dc(lh,la)); o,sim=oddsfor(h,a)
        if o:
            inv=1/o['o1']+1/o['oX']+1/o['o2']
            pk=(1/o['o1']/inv,1/o['oX']/inv,1/o['o2']/inv)
            raw=[(pm[k]**(1-W))*(pk[k]**W) for k in range(3)]; s=sum(raw); tg=[x/s for x in raw]
            lf,af=fit(tg); T=0.5*(lf+af)+0.5*(lh+la); sup=lf-af
        else:
            pk=None; T=0.8*(lh+la)+0.2*(D['HG']+D['AG']); sup=lh-la
        lh2,la2=max((T+sup)/2,0.15),max((T-sup)/2,0.15)
        M=dc(lh2,la2); mk=markets(M)
        sc=sorted(((M[x][y],x,y) for x in range(8) for y in range(8)),reverse=True)[:6]
        OUT.append({'i':i,'lg':lg,'h':fh,'a':fa,'lh':round(lh2,2),'la':round(la2,2),
          'lh0':round(lh,2),'la0':round(la,2),'mk':{k:round(v,4) for k,v in mk.items()},
          'top':[[f"{x}-{y}",round(p*100,1)] for p,x,y in sc],'hs':rh['d'],'as':ra['d'],
          'odds':o,'pk':pk,'sim':round(sim,2),'pm':[round(x,4) for x in pm],
          'lgT':round(D['HG']+D['AG'],2),'lgn':D['n']})
json.dump(OUT,open('final2.json','w'),default=str)
print('OK',len(OUT))
for lg in ORDER: print(f"  {lg:14s} n={LG[lg]['n']:3d} baseline H{LG[lg]['HG']:.2f}/A{LG[lg]['AG']:.2f}")
