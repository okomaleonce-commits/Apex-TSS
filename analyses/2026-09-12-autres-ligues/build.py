import re,html,json
def clean(x): return re.sub(r'\s+',' ',html.unescape(re.sub(r'<[^>]+>',' ',x))).strip()
def tables(f):
    s=open(f,encoding='utf-8',errors='ignore').read()
    s=re.sub(r'<script.*?</script>','',s,flags=re.S); s=re.sub(r'<style.*?</style>','',s,flags=re.S)
    res=[]
    for tb in re.findall(r'<table[^>]*>.*?</table>',s,flags=re.S):
        rows=re.findall(r'<tr[^>]*>(.*?)</tr>',tb,flags=re.S)
        if len(rows)<10: continue
        c0=[clean(x) for x in re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>',rows[0],flags=re.S)]
        if not ' | '.join(x for x in c0 if x).startswith('GP | W | D | L | GF | GA'): continue
        d={}
        for r in rows[1:]:
            c=[clean(x) for x in re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>',r,flags=re.S) ]
            c=[x for x in c if x]
            if len(c)<10: continue
            team=c[1]
            try: gp,w,dr,l,gf,ga=int(c[2]),int(c[3]),int(c[4]),int(c[5]),int(c[6]),int(c[7])
            except: continue
            d[team]=dict(gp=gp,gf=gf,ga=ga)
        if d: res.append(d)
        if len(res)==2: break
    return res
out={}
for lg in ['sweden','norway','brazil','russia','romania']:
    h,a=tables(f'ha_{lg}.html')
    GPh=sum(v['gp'] for v in h.values()); GFh=sum(v['gf'] for v in h.values()); GAh=sum(v['ga'] for v in h.values())
    out[lg]=dict(home=h,away=a,avg_home=GFh/GPh,avg_away=GAh/GPh,matches=GPh,
                 teams=len(h))
    print(f"{lg:<9} matchs={GPh:3d}  buts dom/m={GFh/GPh:.2f}  ext/m={GAh/GPh:.2f}  total={(GFh+GAh)/GPh:.2f}  equipes={len(h)}  J~{GPh*2//len(h)}")
json.dump(out,open('leagues.json','w'))
