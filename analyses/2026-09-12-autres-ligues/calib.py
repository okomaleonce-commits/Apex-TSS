import json,math
L=json.load(open('leagues.json'))
exec(open('price.py').read().split("print(f\"{'MATCH'")[0].split('L=json')[0]) if False else None
FIX=[
 ('sweden',1635909132,'IFK Goteborg','Halmstad','IFK Goteborg-Halmstad'),
 ('norway',1635809701,'Lillestrom','Valerenga','Lillestrom-Valerenga'),
 ('norway',1635794000,'Rosenborg','Tromso','Rosenborg-Tromso'),
 ('brazil',1635255607,'Gremio','Vasco da Gama','Gremio-Vasco'),
 ('brazil',1635255916,'Atletico MG','Fluminense','AtleticoMG-Fluminense'),
 ('brazil',1635255917,'Chapecoense','Internacional','Chapecoense-Internacional'),
 ('brazil',1635255919,'Palmeiras','Sao Paulo','Palmeiras-SaoPaulo'),
 ('brazil',1635255918,'Botafogo','Bragantino','Botafogo-Bragantino'),
]
def dec(p): return 1+p/100 if p>0 else 1-100/p
def mkt_total(mid):
    d=json.load(open(f'm_{mid}.json')); best=None
    for m in d:
        if str(m.get('matchupId'))!=str(mid) or m['period']!=0 or m['type']!='total': continue
        pr={p['designation']:dec(p['price']) for p in m['prices']}
        pts=m['prices'][0]['points']
        io,iu=1/pr['over'],1/pr['under']; po=io/(io+iu)
        d2=abs(po-0.5)
        if best is None or d2<best[0]: best=(d2,pts,po)
    return best
def lam(lg,h,a,k):
    d=L[lg]; H,a_=d['home'][h],d['away'][a]
    sh=lambda r: 1+k*(r-1)
    ah=sh((H['gf']/H['gp'])/d['avg_home']); dh=sh((H['ga']/H['gp'])/d['avg_away'])
    aa=sh((a_['gf']/a_['gp'])/d['avg_away']); da=sh((a_['ga']/a_['gp'])/d['avg_home'])
    return ah*da*d['avg_home'], aa*dh*d['avg_away']
print(f"{'MATCH':<26}{'ligne marche':>13}{'lamb k=1':>10}{'k=.75':>8}{'k=.5':>8}{'k=.35':>8}")
rows=[]
for lg,mid,h,a,lab in FIX:
    b=mkt_total(mid)
    vals=[sum(lam(lg,h,a,k)) for k in (1.0,0.75,0.5,0.35)]
    rows.append((lab,b[1],vals))
    print(f"{lab:<26}{b[1]:>13.2f}{vals[0]:>10.2f}{vals[1]:>8.2f}{vals[2]:>8.2f}{vals[3]:>8.2f}")
import statistics
for i,k in enumerate((1.0,0.75,0.5,0.35)):
    err=[r[2][i]-r[1] for r in rows]
    print(f'k={k}: biais moyen {statistics.mean(err):+.2f}  MAE {statistics.mean(abs(e) for e in err):.2f}')
