import re,html,glob,json,os
res={}; fixt={}
for f in sorted(glob.glob('lg2/*.html')):
    lg=os.path.basename(f)[:-5]
    h=open(f,encoding='utf-8',errors='ignore').read()
    t=re.sub(r'<script.*?</script>','',h,flags=re.S|re.I)
    t=re.sub(r'</tr>','\n',t,flags=re.I); t=re.sub(r'</t[dh]>','|',t,flags=re.I)
    t=re.sub(r'<[^>]+>','',t); t=html.unescape(t); t=re.sub(r'[ \t\xa0]+',' ',t)
    flat=re.sub(r'\s*\n\s*',' ',t)
    pat=re.compile(r'(\d{1,2} [A-Z][a-z]{2}) \| ([^|]{2,30}?) - ([^|]{2,30}?) \| (\d{1,2}):(\d{1,2}) \|')
    seen=set(); games=[]
    for m in pat.finditer(flat):
        k=(m.group(1),m.group(2).strip(),m.group(3).strip(),int(m.group(4)),int(m.group(5)))
        if k in seen: continue
        seen.add(k); games.append({'d':k[0],'h':k[1],'a':k[2],'hg':k[3],'ag':k[4]})
    res[lg]=games
    # fixtures today (no score)
    fp=re.compile(r'(20 Sep) \| ([^|]{2,30}?) - ([^|]{2,30}?) \| - \|')
    fs=[]; s2=set()
    for m in fp.finditer(flat):
        k=(m.group(2).strip(),m.group(3).strip())
        if k in s2: continue
        s2.add(k); fs.append(k)
    fixt[lg]=fs
    print(f"{lg:14s} {len(games):3d} joués | {len(fs)} matchs aujourd'hui: "+", ".join(f"{a} v {b}" for a,b in fs))
json.dump(res,open('results2.json','w')); json.dump(fixt,open('fixt2.json','w'))
