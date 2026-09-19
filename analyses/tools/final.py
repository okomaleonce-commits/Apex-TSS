import json,math,re,unicodedata
exec(open('model.py').read().split('# ---------- calibrated league params')[0])

for lg,D in LG.items():
    n=D['n']; T_obs=D['hgb']+D['agb']
    w2=n/(n+35.0); T=w2*T_obs+(1-w2)*2.85
    w1=n/(n+70.0); share=w1*(D['hgb']/T_obs)+(1-w1)*0.545
    D['HG']=T*share; D['AG']=T*(1-share); D['Lm']=T/2.0

K=4.5; KV=4.0
def ratings(lg,t):
    D=LG[lg]; d=D['teams'][t]; L=D['Lm']
    att=(d['gf']+K*L)/((d['gp']+K)*L); dfn=(d['ga']+K*L)/((d['gp']+K)*L)
    Lh,La=D['HG'],D['AG']
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
    M[0][0]*= (1-lh*la*RHO); M[0][1]*=(1+lh*RHO); M[1][0]*=(1+la*RHO); M[1][1]*=(1-RHO)
    s=sum(map(sum,M)); return [[v/s for v in r] for r in M]

def res1x2(M):
    N=len(M)
    p1=sum(M[i][j] for i in range(N) for j in range(N) if i>j)
    px=sum(M[i][i] for i in range(N))
    return p1,px,1-p1-px

def markets(M):
    N=len(M); m={}
    m['1'],m['X'],m['2']=res1x2(M)
    m['1X']=m['1']+m['X']; m['X2']=m['X']+m['2']; m['12']=m['1']+m['2']
    for line in (0.5,1.5,2.5,3.5,4.5):
        m['O%.1f'%line]=sum(M[i][j] for i in range(N) for j in range(N) if i+j>line)
        m['U%.1f'%line]=1-m['O%.1f'%line]
    m['BTTS']=sum(M[i][j] for i in range(1,N) for j in range(1,N)); m['NOBTTS']=1-m['BTTS']
    m['H-1']=sum(M[i][j] for i in range(N) for j in range(N) if i-j>=2)
    m['A-1']=sum(M[i][j] for i in range(N) for j in range(N) if j-i>=2)
    m['H+1']=sum(M[i][j] for i in range(N) for j in range(N) if i-j>=-1)
    m['A+1']=sum(M[i][j] for i in range(N) for j in range(N) if j-i>=-1)
    m['H_CS']=sum(M[i][0] for i in range(N)); m['A_CS']=sum(M[0][j] for j in range(N))
    return m

ODDS=json.load(open('odds.json'))
BAD={45}
def oddsfor(idx,h,a):
    if idx in BAD: return None,0
    nh,na=norm(h),norm(a); best=None;bs=0
    def tri(s): return {s[i:i+3] for i in range(max(1,len(s)-2))}
    for o in ODDS:
        s1=len(tri(norm(o['h']))&tri(nh))/max(1,len(tri(norm(o['h']))|tri(nh)))
        s2=len(tri(norm(o['a']))&tri(na))/max(1,len(tri(norm(o['a']))|tri(na)))
        s=(s1+s2)/2
        if s>bs: bs=s;best=o
    return (best,bs) if bs>0.48 else (None,bs)

W_MKT=0.62   # market weight in the blend (market margin ~0% => sharp)
def fit(target,lh0,la0):
    best=None
    for t in [x/40 for x in range(48,200)]:          # total goals 1.20 -> 5.00
        for sup in [x/40 for x in range(-80,81)]:    # supremacy -2.0 -> +2.0
            lh=(t+sup)/2; la=(t-sup)/2
            if lh<=0.05 or la<=0.05: continue
            p=res1x2(dc(lh,la))
            e=sum((p[k]-target[k])**2 for k in range(3))
            if best is None or e<best[0]: best=(e,lh,la)
    return best[1],best[2]

OUT=[]
for idx,lg,h,a in MATCHES:
    fh,fa=find(lg,h),find(lg,a)
    rh,ra=ratings(lg,fh),ratings(lg,fa); D=LG[lg]
    lh=D['HG']*rh['att_h']*ra['def_a']; la=D['AG']*ra['att_a']*rh['def_h']
    Mm=dc(lh,la); pm=res1x2(Mm)
    o,sim=oddsfor(idx,h,a)
    if o:
        inv=1/o['o1']+1/o['oX']+1/o['o2']
        pk=(1/o['o1']/inv,1/o['oX']/inv,1/o['o2']/inv)
        raw=[ (pm[k]**(1-W_MKT))*(pk[k]**W_MKT) for k in range(3)]
        s=sum(raw); tgt=[x/s for x in raw]
        lf,af=fit(tgt,lh,la)
        # garde la supremacie issue du blend marche/modele, mais moyenne le total buts
        T=0.5*(lf+af)+0.5*(lh+la); sup=lf-af
        lh2,la2=(T+sup)/2,(T-sup)/2
    else:
        pk=None; tgt=pm
        T=0.8*(lh+la)+0.2*(D['HG']+D['AG']); sup=lh-la
        lh2,la2=(T+sup)/2,(T-sup)/2
    lh2=max(lh2,0.15); la2=max(la2,0.15)
    M=dc(lh2,la2); mk=markets(M)
    sc=sorted(((M[i][j],i,j) for i in range(8) for j in range(8)),reverse=True)[:6]
    OUT.append({'i':idx,'lg':lg,'h':fh,'a':fa,'lh':round(lh2,2),'la':round(la2,2),
      'lh0':round(lh,2),'la0':round(la,2),'mk':{k:round(v,4) for k,v in mk.items()},
      'top':[[f"{i}-{j}",round(p*100,1)] for p,i,j in sc],
      'hs':rh['d'],'as':ra['d'],'odds':o,'pk':pk,'sim':round(sim,2),
      'pm':[round(x,4) for x in pm],'lgT':round(D['HG']+D['AG'],2)})
json.dump(OUT,open('final.json','w'),default=str)
print('done',len(OUT))
