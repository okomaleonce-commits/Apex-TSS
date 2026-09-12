import json,math
L=json.load(open('leagues.json'))
RHO={'sweden':-0.05,'norway':-0.05,'brazil':-0.10,'russia':-0.06,'romania':-0.06}
FIX=[
 ('sweden',1635909132,'IFK Goteborg','Halmstad','IFK Goteborg vs Halmstads','15:30'),
 ('norway',1635809701,'Lillestrom','Valerenga','Lillestrom vs Valerenga','14:00'),
 ('norway',1635794000,'Rosenborg','Tromso','Rosenborg vs Tromso','16:00'),
 ('brazil',1635255607,'Gremio','Vasco da Gama','Gremio vs Vasco','19:00'),
 ('brazil',1635255916,'Atletico MG','Fluminense','Atletico MG vs Fluminense','19:00'),
 ('brazil',1635255917,'Chapecoense','Internacional','Chapecoense vs Internacional','20:00'),
 ('brazil',1635255919,'Palmeiras','Sao Paulo','Palmeiras vs Sao Paulo','21:30'),
 ('brazil',1635255918,'Botafogo','Bragantino','Botafogo vs Bragantino','23:30'),
 ('russia',1635091687,'Dinamo Moscow','Orenburg','Dinamo Moscow vs Orenburg','15:30'),
 ('russia',1635071615,'CSKA Moscow','Rubin Kazan','CSKA vs Rubin Kazan','17:45'),
 ('romania',1635909134,'Sepsi OSK','CFR Cluj','Sepsi vs CFR Cluj','15:30'),
 ('romania',1635919063,'Rapid Bucharest','Voluntari','Rapid vs Voluntari','18:30'),
]
def dec(p): return 1+p/100 if p>0 else 1-100/p
def tau(x,y,lh,la,rho):
    if x==0 and y==0: return 1-lh*la*rho
    if x==0 and y==1: return 1+lh*rho
    if x==1 and y==0: return 1+la*rho
    if x==1 and y==1: return 1-rho
    return 1.0
def pois(k,l): return math.exp(-l)*l**k/math.factorial(k)
def grid(lh,la,rho,N=11):
    M=[[tau(i,j,lh,la,rho)*pois(i,lh)*pois(j,la) for j in range(N)] for i in range(N)]
    s=sum(sum(r) for r in M); return [[v/s for v in r] for r in M]
def devig(*o):
    inv=[1/x for x in o]; s=sum(inv); return [i/s for i in inv], s-1

def odds(mid):
    d=json.load(open(f'm_{mid}.json')); r={'ml':None,'tot':{},'ah':{}}
    for m in d:
        if str(m.get('matchupId'))!=str(mid) or m['period']!=0: continue
        pr=m['prices']
        if m['type']=='moneyline' and m.get('isAlternate') is False:
            r['ml']={p['designation']:dec(p['price']) for p in pr}
        elif m['type']=='total':
            r['tot'][pr[0]['points']]={p['designation']:dec(p['price']) for p in pr}
        elif m['type']=='spread':
            k=[p['points'] for p in pr if p['designation']=='home'][0]
            r['ah'][k]={p['designation']:dec(p['price']) for p in pr}
    return r

print(f"{'MATCH':<34}{'lH':>5}{'lA':>5}{'tot':>6} | {'mod 1/X/2':>20} | {'marche 1/X/2':>20} | {'edge':>22}")
res=[]
for lg,mid,h,a,label,ko in FIX:
    d=L[lg]
    if h not in d['home'] or a not in d['away']:
        print('MAP FAIL',label,h in d['home'],a in d['away']); continue
    H,A=d['home'][h],d['away'][a]
    ah=(H['gf']/H['gp'])/d['avg_home']; dh=(H['ga']/H['gp'])/d['avg_away']
    aa=(A['gf']/A['gp'])/d['avg_away']; da=(A['ga']/A['gp'])/d['avg_home']
    lh=ah*da*d['avg_home']; la=aa*dh*d['avg_away']
    M=grid(lh,la,RHO[lg])
    pH=sum(M[i][j] for i in range(11) for j in range(11) if i>j)
    pD=sum(M[i][i] for i in range(11)); pA=1-pH-pD
    o25=sum(M[i][j] for i in range(11) for j in range(11) if i+j>2)
    btts=sum(M[i][j] for i in range(1,11) for j in range(1,11))
    o=odds(mid)
    if not o['ml']: print('NO ML',label); continue
    mk,marg=devig(o['ml']['home'],o['ml']['draw'],o['ml']['away'])
    res.append(dict(lg=lg,label=label,ko=ko,lh=lh,la=la,pH=pH,pD=pD,pA=pA,o25=o25,btts=btts,
                    mk=mk,marg=marg,odds=o))
    print(f"{label:<34}{lh:5.2f}{la:5.2f}{lh+la:6.2f} | {pH*100:5.1f}/{pD*100:4.1f}/{pA*100:5.1f}%    | {mk[0]*100:5.1f}/{mk[1]*100:4.1f}/{mk[2]*100:5.1f}%    | {(pH-mk[0])*100:+6.1f}/{(pD-mk[1])*100:+5.1f}/{(pA-mk[2])*100:+6.1f}")
json.dump([{k:v for k,v in r.items() if k!='odds'} for r in res],open('res.json','w'))
import pickle; pickle.dump(res,open('res.pkl','wb'))

print('\n\n==================== MARCHES TOTALS / AH ====================')
import math
for r in res:
    o=r['odds']; M=None
    print('#'*70)
    print(f"{r['label']}  (KO {r['ko']} UTC)  lambda {r['lh']:.2f}/{r['la']:.2f} tot {r['lh']+r['la']:.2f}")
    print(f"  1X2 marche {o['ml']['home']:.2f}/{o['ml']['draw']:.2f}/{o['ml']['away']:.2f}  marge {r['marg']*100:.1f}%")
    print(f"  fair modele 1X2 : {1/r['pH']:.2f} / {1/r['pD']:.2f} / {1/r['pA']:.2f}")
    # recompute grid for totals
    lg=r['lg']
    lh,la=r['lh'],r['la']
    def tau2(x,y):
        rho=RHO[lg]
        if x==0 and y==0: return 1-lh*la*rho
        if x==0 and y==1: return 1+lh*rho
        if x==1 and y==0: return 1+la*rho
        if x==1 and y==1: return 1-rho
        return 1.0
    G=[[tau2(i,j)*pois(i,lh)*pois(j,la) for j in range(11)] for i in range(11)]
    s=sum(sum(x) for x in G); G=[[v/s for v in row] for row in G]
    for line in sorted(o['tot']):
        if line not in (1.5,2.0,2.25,2.5,2.75,3.0,3.25,3.5): continue
        po=sum(G[i][j] for i in range(11) for j in range(11) if i+j>line)
        mk,marg=devig(o['tot'][line]['over'],o['tot'][line]['under'])
        eo=(po-mk[0])*100; eu=((1-po)-mk[1])*100
        tag=''
        if eo>=5: tag=' <== OVER'
        if eu>=5: tag=' <== UNDER'
        print(f"   O/U {line:<4} : cote O {o['tot'][line]['over']:.2f} U {o['tot'][line]['under']:.2f} | marche O {mk[0]*100:5.1f}% | modele O {po*100:5.1f}% | edge O {eo:+5.1f} U {eu:+5.1f}{tag}")
    for k in sorted(o['ah']):
        if abs(k)>2.5: continue
        print(f"   AH {k:+.2f} : home {o['ah'][k]['home']:.2f} / away {o['ah'][k]['away']:.2f}")
