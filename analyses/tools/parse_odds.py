import re,html,glob,json
out=[]
for f in sorted(glob.glob('odds/*.html')):
    h=open(f,encoding='utf-8',errors='ignore').read()
    t=re.sub(r'<script.*?</script>','',h,flags=re.S|re.I)
    t=re.sub(r'<style.*?</style>','',t,flags=re.S|re.I)
    t=re.sub(r'<[^>]+>','\n',t); t=html.unescape(t)
    lines=[l.strip() for l in t.split('\n') if l.strip()]
    s=' ~ '.join(lines)
    # pattern: HH:MM ~ Home ~ Away ~ 1 ~ o1 ~ X ~ oX ~ 2 ~ o2
    pat=re.compile(r'(\d{2}:\d{2}) ~ (.+?) ~ (.+?) ~ 1 ~ (\d+,\d+) ~ X ~ (\d+,\d+) ~ 2 ~ (\d+,\d+)')
    for m in pat.finditer(s):
        ho=m.group(2).strip(); aw=m.group(3).strip()
        if len(ho)>40 or len(aw)>40: continue
        out.append({'date':f[5:15],'time':m.group(1),'h':ho,'a':aw,
                    'o1':float(m.group(4).replace(',','.')),
                    'oX':float(m.group(5).replace(',','.')),
                    'o2':float(m.group(6).replace(',','.'))})
seen=set(); ded=[]
for o in out:
    k=(o['h'],o['a'])
    if k in seen: continue
    seen.add(k); ded.append(o)
json.dump(ded,open('odds.json','w'),ensure_ascii=False)
print(len(ded))
for o in ded: print(o['date'],o['time'],o['h'],'vs',o['a'],o['o1'],o['oX'],o['o2'])
