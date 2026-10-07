import math
AVG_TEAM=1.53; RHO=-0.08; WCH=0.93
# prior = ancrage 2025/26 (moteur APEX v2.0) ; cur = 2026/27 (2 matchs)
teams={
 'Freiburg':   dict(pGF=1.60,pGA=1.60,cGF=2.50,cGA=0.50,ha=1.15),
 'Gladbach':   dict(pGF=1.35,pGA=1.85,cGF=1.50,cGA=3.50,ha=1.15),
 'Mainz':      dict(pGF=1.45,pGA=1.60,cGF=2.50,cGA=0.00,ha=1.15),
 'Frankfurt':  dict(pGF=1.75,pGA=1.85,cGF=2.00,cGA=3.50,ha=1.15),
 'Dortmund':   dict(pGF=2.06,pGA=1.00,cGF=2.50,cGA=1.00,ha=1.19),
 'Paderborn':  dict(pGF=1.10,pGA=1.85,cGF=0.00,cGA=0.50,ha=1.08),
 'Hoffenheim': dict(pGF=2.00,pGA=1.50,cGF=2.00,cGA=3.00,ha=1.15),
 'Stuttgart':  dict(pGF=2.05,pGA=1.40,cGF=2.50,cGA=3.00,ha=1.15),
 'Augsburg':   dict(pGF=1.40,pGA=1.70,cGF=3.50,cGA=0.50,ha=1.15),
 'Leverkusen': dict(pGF=2.00,pGA=1.40,cGF=3.00,cGA=1.50,ha=1.15),
}
W=0.65  # poids prior (n=2 → shrinkage fort)
for t,d in teams.items():
    d['GF']=W*d['pGF']+(1-W)*d['cGF']; d['GA']=W*d['pGA']+(1-W)*d['cGA']
    d['att']=d['GF']/AVG_TEAM; d['deff']=d['GA']/AVG_TEAM

def tau(x,y,lh,la,rho):
    if x==0 and y==0: return 1-lh*la*rho
    if x==0 and y==1: return 1+lh*rho
    if x==1 and y==0: return 1+la*rho
    if x==1 and y==1: return 1-rho
    return 1.0
def pois(k,l): return math.exp(-l)*l**k/math.factorial(k)

def grid(lh,la,N=11):
    M=[[tau(i,j,lh,la,RHO)*pois(i,lh)*pois(j,la) for j in range(N)] for i in range(N)]
    s=sum(sum(r) for r in M)
    return [[v/s for v in r] for r in M]

def analyse(h,a,ais_h=1.0,ais_a=1.0):
    H,A=teams[h],teams[a]
    lh=H['att']*A['deff']*AVG_TEAM*H['ha']*WCH*ais_h
    la=A['att']*H['deff']*AVG_TEAM*WCH*ais_a
    M=grid(lh,la)
    pH=sum(M[i][j] for i in range(11) for j in range(11) if i>j)
    pD=sum(M[i][i] for i in range(11))
    pA=1-pH-pD
    o25=sum(M[i][j] for i in range(11) for j in range(11) if i+j>2)
    o35=sum(M[i][j] for i in range(11) for j in range(11) if i+j>3)
    o15=sum(M[i][j] for i in range(11) for j in range(11) if i+j>1)
    btts=sum(M[i][j] for i in range(1,11) for j in range(1,11))
    # top scores
    sc=sorted(((M[i][j],i,j) for i in range(6) for j in range(6)),reverse=True)[:3]
    return dict(lh=lh,la=la,pH=pH,pD=pD,pA=pA,o15=o15,o25=o25,o35=o35,btts=btts,top=sc)

def dm(*odds):
    inv=[1/o for o in odds]; s=sum(inv)
    return [i/s for i in inv], s-1

fixtures=[('Freiburg','Gladbach',(1.699,4.26,4.80),(1.962,1.943,3.25)),
          ('Mainz','Frankfurt',(1.80,4.18,4.27),(1.901,2.01,3.25)),
          ('Dortmund','Paderborn',(1.264,6.75,10.16),(1.917,1.971,3.5)),
          ('Hoffenheim','Stuttgart',(2.26,3.96,3.00),(1.935,1.971,3.5)),
          ('Augsburg','Leverkusen',(3.83,4.11,1.909),(1.943,1.962,3.5))]
for h,a,ml,tot in fixtures:
    r=analyse(h,a)
    p,marg=dm(*ml)
    po,mo=dm(tot[0],tot[1])
    print('='*74)
    print(f'{h} vs {a}   lambda H={r["lh"]:.2f} A={r["la"]:.2f}  total={r["lh"]+r["la"]:.2f}')
    print(f'  MODELE : H {r["pH"]*100:.1f}%  D {r["pD"]*100:.1f}%  A {r["pA"]*100:.1f}%   O2.5 {r["o25"]*100:.1f}%  O3.5 {r["o35"]*100:.1f}%  BTTS {r["btts"]*100:.1f}%')
    print(f'  MARCHE : H {p[0]*100:.1f}%  D {p[1]*100:.1f}%  A {p[2]*100:.1f}%   (marge {marg*100:.1f}%)')
    print(f'  EDGE   : H {(r["pH"]-p[0])*100:+.1f}pts  D {(r["pD"]-p[1])*100:+.1f}pts  A {(r["pA"]-p[2])*100:+.1f}pts')
    print(f'  Ligne {tot[2]} : marche O {po[0]*100:.1f}% / U {po[1]*100:.1f}%  (marge {mo*100:.1f}%)')
    print(f'  Fair odds modele : H {1/r["pH"]:.2f}  D {1/r["pD"]:.2f}  A {1/r["pA"]:.2f}  O2.5 {1/r["o25"]:.2f}  U2.5 {1/(1-r["o25"]):.2f}  BTTS {1/r["btts"]:.2f}')
    print('  Scores probables :',', '.join(f'{i}-{j} ({v*100:.1f}%)' for v,i,j in r['top']))
