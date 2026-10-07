"""APEX — pricing de la journée du 26/09/2026 (pause internationale).

Clubs (MLS, League One, League Two) : ratings attaque/défense régressés vers la
moyenne de ligue (K=4.5), Poisson Dixon-Coles, fusion 62/38 avec le marché si
des cotes existent (MLS).
Sélections (Ligue des Nations, qualifs CAN) : espérance Elo (eloratings.net,
+100 à domicile) -> lambdas, fusion 62/38 avec le marché si cotes disponibles.

Entrées dans le même dossier : results.json, odds.json, elo.json
Sortie : final.json
"""
import json, math, re, unicodedata

RHO = -0.08
W_MKT = 0.62
K = 4.5


def dc(lh, la, N=11):
    ph = [math.exp(-lh) * lh ** i / math.factorial(i) for i in range(N)]
    pa = [math.exp(-la) * la ** i / math.factorial(i) for i in range(N)]
    M = [[ph[i] * pa[j] for j in range(N)] for i in range(N)]
    M[0][0] *= 1 - lh * la * RHO; M[0][1] *= 1 + lh * RHO
    M[1][0] *= 1 + la * RHO; M[1][1] *= 1 - RHO
    s = sum(map(sum, M))
    return [[v / s for v in r] for r in M]


def res1x2(M):
    N = len(M)
    p1 = sum(M[i][j] for i in range(N) for j in range(N) if i > j)
    px = sum(M[i][i] for i in range(N))
    return p1, px, 1 - p1 - px


def markets(M):
    N = len(M); m = {}
    m['1'], m['X'], m['2'] = res1x2(M)
    m['1X'] = m['1'] + m['X']; m['X2'] = m['X'] + m['2']; m['12'] = m['1'] + m['2']
    for line in (0.5, 1.5, 2.5, 3.5, 4.5):
        o = sum(M[i][j] for i in range(N) for j in range(N) if i + j > line)
        m['O%.1f' % line] = o; m['U%.1f' % line] = 1 - o
    m['BTTS'] = sum(M[i][j] for i in range(1, N) for j in range(1, N)); m['NOBTTS'] = 1 - m['BTTS']
    m['H-1'] = sum(M[i][j] for i in range(N) for j in range(N) if i - j >= 2)
    m['A-1'] = sum(M[i][j] for i in range(N) for j in range(N) if j - i >= 2)
    m['H_CS'] = sum(M[i][0] for i in range(N)); m['A_CS'] = sum(M[0][j] for j in range(N))
    return m


def fit(target, total=None):
    """lambdas reproduisant au mieux un vecteur 1X2 (total libre ou imposé)."""
    best = None
    totals = [total] if total else [x / 40 for x in range(48, 200)]
    for t in totals:
        for sup in [x / 40 for x in range(-180, 181)]:
            lh, la = (t + sup) / 2, (t - sup) / 2
            if lh <= 0.05 or la <= 0.05: continue
            p = res1x2(dc(lh, la, 9))
            e = sum((p[k] - target[k]) ** 2 for k in range(3))
            if best is None or e < best[0]: best = (e, lh, la)
    return best[1], best[2]


def demargin(o):
    inv = 1 / o['o1'] + 1 / o['oX'] + 1 / o['o2']
    return (1 / o['o1'] / inv, 1 / o['oX'] / inv, 1 / o['o2'] / inv)


def blend(pm, pk):
    raw = [(pm[k] ** (1 - W_MKT)) * (pk[k] ** W_MKT) for k in range(3)]
    s = sum(raw); return [x / s for x in raw]


def norm(s):
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode().lower()
    s = re.sub(r'\b(fc|sc|cf|afc|united|utd|city|town)\b', ' ', s)
    return re.sub(r'[^a-z]', '', s)


def tri(s): return {s[i:i + 3] for i in range(max(1, len(s) - 2))}


def sim(a, b):
    A, B = tri(norm(a)), tri(norm(b)); return len(A & B) / max(1, len(A | B))


# ---------------- clubs
RES = json.load(open('results.json'))
ODDS = json.load(open('odds.json'))
PRIOR_T = {'usa': 3.0, 'england3': 2.65, 'england4': 2.6}


def league(lg):
    G = RES[lg]; n = len(G)
    hg = sum(g['hg'] for g in G); ag = sum(g['ag'] for g in G)
    w = n / (n + 35.0); T = w * (hg + ag) / n + (1 - w) * PRIOR_T[lg]
    w1 = n / (n + 70.0); sh = w1 * hg / (hg + ag) + (1 - w1) * 0.555
    teams = {}
    for g in G:
        for side, t, f, a in (('h', g['h'], g['hg'], g['ag']), ('a', g['a'], g['ag'], g['hg'])):
            d = teams.setdefault(t, dict(gp=0, gf=0, ga=0, pts=0, hgp=0, hgf=0, hga=0, agp=0, agf=0, aga=0, res=[]))
            d['gp'] += 1; d['gf'] += f; d['ga'] += a
            d['pts'] += 3 if f > a else 1 if f == a else 0
            d[side + 'gp'] += 1; d[side + 'gf'] += f; d[side + 'ga'] += a
            d['res'].append(('W' if f > a else 'D' if f == a else 'L', f'{f}-{a}', side.upper()))
    return dict(n=n, HG=T * sh, AG=T * (1 - sh), L=T / 2, teams=teams)


LG = {lg: league(lg) for lg in RES}


def find(lg, name):
    return max(LG[lg]['teams'], key=lambda t: sim(t, name))


def rat(D, t):
    d = D['teams'][t]; L = D['L']
    return (d['gf'] + K * L) / ((d['gp'] + K) * L), (d['ga'] + K * L) / ((d['gp'] + K) * L), d


def odds_for(h, a):
    best = max(ODDS, key=lambda o: sim(o['h'], h) + sim(o['a'], a))
    return best if sim(best['h'], h) + sim(best['a'], a) > 0.7 else None


def price_club(lg, h, a):
    D = LG[lg]; fh, fa = find(lg, h), find(lg, a)
    ath, dfh, dh = rat(D, fh); ata, dfa, da = rat(D, fa)
    lh = D['HG'] * ath * dfa; la = D['AG'] * ata * dfh
    pm = res1x2(dc(lh, la)); o = odds_for(h, a) if lg == 'usa' else None
    if o:
        pk = demargin(o); lf, af = fit(blend(pm, pk))
        T = 0.5 * (lf + af) + 0.5 * (lh + la); sup = lf - af
    else:
        pk = None; T = 0.8 * (lh + la) + 0.2 * (D['HG'] + D['AG']); sup = lh - la
    return dict(h=fh, a=fa, lh0=lh, la0=la, pm=pm, odds=o, pk=pk, T=T, sup=sup,
                hs=dh, as_=da, lgT=D['HG'] + D['AG'])


# ---------------- sélections
ELO = json.load(open('elo.json'))


def price_nat(h, a, home_adv=100, base_T=2.45):
    dr = ELO[h] - ELO[a] + home_adv
    we = 1 / (10 ** (-dr / 400) + 1)
    T = base_T + 4.2 * (we - 0.5) ** 2
    best = None
    for sup in [x / 40 for x in range(-200, 201)]:
        lh, la = (T + sup) / 2, (T - sup) / 2
        if lh <= 0.05 or la <= 0.05: continue
        p = res1x2(dc(lh, la, 9)); e = abs(p[0] + 0.5 * p[1] - we)
        if best is None or e < best[0]: best = (e, lh, la)
    lh, la = best[1], best[2]; pm = res1x2(dc(lh, la))
    o = odds_for(h, a)
    if o:
        pk = demargin(o); lf, af = fit(blend(pm, pk))
        T2 = 0.5 * (lf + af) + 0.5 * (lh + la); sup = lf - af
    else:
        pk = None; T2 = lh + la; sup = lh - la
    return dict(h=h, a=a, lh0=lh, la0=la, pm=pm, odds=o, pk=pk, T=T2, sup=sup,
                elo=(ELO[h], ELO[a]), we=we)


MATCHES = json.load(open('matches.json'))
OUT = []
for i, (comp, lg, h, a) in enumerate(MATCHES, 1):
    r = price_nat(h, a, **({'home_adv': 100, 'base_T': 2.2} if lg == 'afcon' else {})) \
        if lg in ('nations', 'afcon') else price_club(lg, h, a)
    lh = max((r['T'] + r['sup']) / 2, 0.12); la = max((r['T'] - r['sup']) / 2, 0.12)
    M = dc(lh, la); mk = markets(M)
    top = sorted(((M[x][y], x, y) for x in range(8) for y in range(8)), reverse=True)[:5]
    r.update(i=i, comp=comp, lg=lg, lh=round(lh, 2), la=round(la, 2),
             mk={k: round(v, 4) for k, v in mk.items()},
             top=[[f'{x}-{y}', round(p * 100, 1)] for p, x, y in top])
    OUT.append(r)
json.dump(OUT, open('final.json', 'w'), default=str, ensure_ascii=False)
print('ok', len(OUT))
