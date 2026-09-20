import re,html,json
out=[]
for f in ['odds/n_2026-09-20.html','odds/n_2026-09-21.html']:
    h=open(f,encoding='utf-8',errors='ignore').read()
    t=re.sub(r'<script.*?</script>','',h,flags=re.S|re.I)
    t=re.sub(r'<style.*?</style>','',t,flags=re.S|re.I)
    t=re.sub(r'<[^>]+>','\n',t); t=html.unescape(t)
    s=' ~ '.join([l.strip() for l in t.split('\n') if l.strip()])
    for m in re.finditer(r'(\d{2}:\d{2}) ~ (.+?) ~ (.+?) ~ 1 ~ (\d+,\d+) ~ X ~ (\d+,\d+) ~ 2 ~ (\d+,\d+)',s):
        ho,aw=m.group(2).strip(),m.group(3).strip()
        if len(ho)>40 or len(aw)>40: continue
        out.append({'time':m.group(1),'h':ho,'a':aw,'o1':float(m.group(4).replace(',','.')),
                    'oX':float(m.group(5).replace(',','.')),'o2':float(m.group(6).replace(',','.'))})
seen=set(); ded=[]
for o in out:
    k=(o['h'],o['a'])
    if k in seen: continue
    seen.add(k); ded.append(o)
json.dump(ded,open('odds2.json','w'),ensure_ascii=False)
print(len(ded))
for o in ded: print(o['time'],o['h'],'vs',o['a'],o['o1'],o['oX'],o['o2'])
