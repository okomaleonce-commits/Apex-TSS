import json
O=json.load(open('final.json'))
LBL={'england':'Premier League','italy':'Serie A','spain':'La Liga','france':'Ligue 1','germany':'Bundesliga',
'netherlands':'Eredivisie','portugal':'Liga Portugal','scotland':'Scottish Prem','netherlands2':'Eerste Divisie',
'germany2':'2. Bundesliga','france2':'Ligue 2','spain2':'LaLiga Hypermotion','italy2':'Serie B','belgium':'Pro League',
'turkey':'Süper Lig','denmark':'Superliga'}
def pick(r):
    m=r['mk']; c=[]
    c.append(('1 (dom.)',m['1'])); c.append(('2 (ext.)',m['2'])); c.append(('X',m['X']))
    c.append(('1X',m['1X'])); c.append(('X2',m['X2'])); c.append(('12',m['12']))
    c.append(('Over 1.5',m['O1.5'])); c.append(('Under 3.5',m['U3.5']))
    c.append(('Over 2.5',m['O2.5'])); c.append(('Under 2.5',m['U2.5']))
    c.append(('BTTS Oui',m['BTTS'])); c.append(('BTTS Non',m['NOBTTS']))
    c.append(('Dom -1 (gagne par 2+)',m['H-1'])); c.append(('Ext -1',m['A-1']))
    c.append(('Dom +1 AH',m['H+1'])); c.append(('Ext +1 AH',m['A+1']))
    return sorted(c,key=lambda x:-x[1])
for r in O:
    m=r['mk']; d=r['hs']; e=r['as']
    print('='*104)
    print(f"#{r['i']:2d} {LBL[r['lg']]:20s} {r['h']} vs {r['a']}")
    print(f"  DOM  GP{d['gp']} {d['pts']}pts {d['gf']}:{d['ga']}  dom {d['hgp']}m {d['hgf']}:{d['hga']}  form {''.join(x[0] for x in d['res'])}  {' '.join(x[1]+x[2] for x in d['res'])}")
    print(f"  EXT  GP{e['gp']} {e['pts']}pts {e['gf']}:{e['ga']}  ext {e['agp']}m {e['agf']}:{e['aga']}  form {''.join(x[0] for x in e['res'])}  {' '.join(x[1]+x[2] for x in e['res'])}")
    print(f"  lambda modele {r['lh0']}-{r['la0']} -> retenu {r['lh']}-{r['la']} (tot {r['lh']+r['la']:.2f}, ligue {r['lgT']})")
    print(f"  1={m['1']*100:.0f} X={m['X']*100:.0f} 2={m['2']*100:.0f} | 1X={m['1X']*100:.0f} X2={m['X2']*100:.0f} 12={m['12']*100:.0f}")
    print(f"  O1.5={m['O1.5']*100:.0f} O2.5={m['O2.5']*100:.0f} U2.5={m['U2.5']*100:.0f} U3.5={m['U3.5']*100:.0f} BTTS={m['BTTS']*100:.0f} noBTTS={m['NOBTTS']*100:.0f}")
    print(f"  H-1={m['H-1']*100:.0f} A-1={m['A-1']*100:.0f} H+1={m['H+1']*100:.0f} A+1={m['A+1']*100:.0f} CSdom={m['H_CS']*100:.0f} CSext={m['A_CS']*100:.0f}")
    print(f"  SCORES {r['top']}")
    if r['odds']:
        o=r['odds']; pk=r['pk']
        print(f"  COTES {o['o1']}/{o['oX']}/{o['o2']}  marche {pk[0]*100:.0f}/{pk[1]*100:.0f}/{pk[2]*100:.0f}  modele-brut {r['pm'][0]*100:.0f}/{r['pm'][1]*100:.0f}/{r['pm'][2]*100:.0f}")
        print(f"  EV  1={(m['1']*o['o1']-1)*100:+.0f}%  X={(m['X']*o['oX']-1)*100:+.0f}%  2={(m['2']*o['o2']-1)*100:+.0f}%")
    else:
        print("  COTES: indisponibles (ligue non couverte par la source publique)")
    print("  TOP MARCHES: "+" | ".join(f"{n} {p*100:.0f}%" for n,p in pick(r)[:5]))
