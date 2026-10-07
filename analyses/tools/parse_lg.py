import re,html,glob,json,os
res={}
for f in sorted(glob.glob('lg/*.html')):
    lg=os.path.basename(f)[:-5]
    h=open(f,encoding='utf-8',errors='ignore').read()
    t=re.sub(r'<script.*?</script>','',h,flags=re.S|re.I)
    t=re.sub(r'</tr>','\n',t,flags=re.I)
    t=re.sub(r'</t[dh]>','|',t,flags=re.I)
    t=re.sub(r'<[^>]+>','',t)
    t=html.unescape(t)
    t=re.sub(r'[ \t\xa0]+',' ',t)
    # collapse newlines inside rows: each result row spans many lines; rejoin whole doc
    flat=re.sub(r'\s*\n\s*',' ',t)
    pat=re.compile(r'(\d{1,2} [A-Z][a-z]{2}) \| ([^|]{2,30}?) - ([^|]{2,30}?) \| (\d{1,2}):(\d{1,2}) \|')
    seen=set(); games=[]
    for m in pat.finditer(flat):
        d,a,b,x,y=m.group(1),m.group(2).strip(),m.group(3).strip(),int(m.group(4)),int(m.group(5))
        k=(d,a,b,x,y)
        if k in seen: continue
        seen.add(k); games.append({'d':d,'h':a,'a':b,'hg':x,'ag':y})
    res[lg]=games
    print(lg, len(games))
json.dump(res,open('results.json','w'))
