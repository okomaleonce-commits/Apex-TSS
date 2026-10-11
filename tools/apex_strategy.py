#!/usr/bin/env python3
"""APEX-STRATEGY — moteur de backtest de règles de pari.

Transforme une règle précise en VERDICT CHIFFRÉ sur l'historique réel
(football-data.co.uk en cache, cotes Pinnacle = book le plus serré). C'est le
moteur qui remplace les scripts jetables : « un jour ça va prendre » → un nombre.

Ce qu'il mesure, honnêtement :
  - taille d'échantillon, taux de victoire, taux de nul ;
  - ROI à mise plate, cote moyenne ;
  - en marché DNB : nul remboursé (cote DNB dé-viggée + marge paramétrable) ;
  - stabilité : ventilation par saison ;
  - seuil de rentabilité (taux de victoire nécessaire).

Garde-fous anti-illusion :
  - ROI calculé sur cotes Pinnacle (marge ~2,5 %). Un book soft (betclic, 1xBet…)
    prélève 6–10 % : la réalité est DONC pire que ce que sort le moteur. Affiché.
  - Échantillon < 200 paris → résultat marqué BRUIT.
  - Le moteur ne price pas, n'émet aucun pari (gel). Il mesure une hypothèse.
  - Anti-invention : aucune donnée fabriquée ; un trou reste un trou.

Règle = CLI, ou fichier JSON via --spec. Exemple de spec :
  {"fav_min":2.00,"fav_max":2.35,"opp_min":2.35,"opp_max":3.20,"pick":"fav","market":"DNB"}
"""
import argparse, csv, glob, json, os, sys, re
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = os.path.join(ROOT, "data", "history")
DNB_MARGIN = 0.025  # marge 2-voix type Pinnacle, appliquée au DNB dérivé


def _f(x):
    try:
        return float(x)
    except Exception:
        return None


def load():
    """Charge tous les matchs : (div, season, home, away, oH,oD,oA, res, hg, ag)."""
    out = []
    for path in glob.glob(os.path.join(HIST, "*.csv")):
        base = os.path.basename(path)
        m = re.match(r"([A-Z]+\d*)_?(\d{4})?", base)
        div = m.group(1) if m else base.split("_")[0]
        season = (m.group(2) if m and m.group(2) else "")
        try:
            rdr = csv.DictReader(open(path, encoding="latin-1"))
        except Exception:
            continue
        for r in rdr:
            oH = _f(r.get("PSH") or r.get("B365H")); oD = _f(r.get("PSD") or r.get("B365D")); oA = _f(r.get("PSA") or r.get("B365A"))
            res = r.get("FTR")
            if None in (oH, oD, oA) or res not in ("H", "D", "A"):
                continue
            out.append(dict(div=div, season=season, home=(r.get("HomeTeam") or "").strip(),
                            away=(r.get("AwayTeam") or "").strip(), oH=oH, oD=oD, oA=oA, res=res,
                            hg=_f(r.get("FTHG")), ag=_f(r.get("FTAG"))))
    return out


def passes(mtc, s):
    oH, oA, oD = mtc["oH"], mtc["oA"], mtc["oD"]
    lo, hi = min(oH, oA), max(oH, oA)
    if s.get("fav_min") is not None and lo < s["fav_min"]:
        return False
    if s.get("fav_max") is not None and lo > s["fav_max"]:
        return False
    if s.get("opp_min") is not None and hi < s["opp_min"]:
        return False
    if s.get("opp_max") is not None and hi > s["opp_max"]:
        return False
    diff = abs(oH - oA)
    if s.get("max_diff") is not None and diff > s["max_diff"]:
        return False
    if s.get("min_diff") is not None and diff < s["min_diff"]:
        return False
    if s.get("draw_min") is not None and oD < s["draw_min"]:
        return False
    if s.get("draw_max") is not None and oD > s["draw_max"]:
        return False
    if s.get("divs") and mtc["div"] not in s["divs"]:
        return False
    if s.get("seasons") and mtc["season"] not in s["seasons"]:
        return False
    tl = s.get("only_teams"); xl = s.get("exclude_teams")
    if tl and not (mtc["home"] in tl or mtc["away"] in tl):
        return False
    if xl and (mtc["home"] in xl or mtc["away"] in xl):
        return False
    return True


def pick_side(mtc, pick):
    if pick == "fav":
        return "H" if mtc["oH"] <= mtc["oA"] else "A"
    if pick == "dog":
        return "H" if mtc["oH"] > mtc["oA"] else "A"
    if pick in ("home", "H"):
        return "H"
    if pick in ("away", "A"):
        return "A"
    if pick in ("draw", "X", "D"):
        return "D"
    raise SystemExit(f"pick inconnu: {pick}")


def odds_for(mtc, side):
    return {"H": mtc["oH"], "D": mtc["oD"], "A": mtc["oA"]}[side]


def dnb_odds(mtc, side):
    """Cote Draw No Bet dérivée des 1X2 dé-viggés (nul retiré), marge DNB_MARGIN."""
    if side == "D":
        return None
    pH, pD, pA = 1 / mtc["oH"], 1 / mtc["oD"], 1 / mtc["oA"]
    s = pH + pD + pA
    pH, pA = pH / s, pA / s
    p = pH / (pH + pA) if side == "H" else pA / (pH + pA)
    return (1 / p) * (1 - DNB_MARGIN)


def backtest(rows, s):
    market = s.get("market", "1X2").upper()
    pick = s.get("pick", "fav")
    bets = []
    for mtc in rows:
        if not passes(mtc, s):
            continue
        side = pick_side(mtc, pick)
        if market == "DNB" and side == "D":
            continue
        bets.append((mtc, side))
    return market, pick, bets


def score(bets, market):
    n = len(bets)
    win = draws = 0
    ret = 0.0
    odds_sum = 0.0
    by_season = defaultdict(lambda: [0, 0.0])  # season -> [n, ret]
    for mtc, side in bets:
        res = mtc["res"]
        if market == "DNB":
            o = dnb_odds(mtc, side)
            odds_sum += o
            if res == side:
                win += 1; ret += o; by_season[mtc["season"]][1] += o
            elif res == "D":
                draws += 1; ret += 1; by_season[mtc["season"]][1] += 1
        else:
            o = odds_for(mtc, side)
            odds_sum += o
            if res == side:
                win += 1; ret += o; by_season[mtc["season"]][1] += o
            if res == "D":
                draws += 1
        by_season[mtc["season"]][0] += 1
    return dict(n=n, win=win, draws=draws, ret=ret, odds_avg=(odds_sum / n if n else 0),
                roi=((ret - n) / n * 100 if n else 0), by_season=by_season)


def main():
    ap = argparse.ArgumentParser(description="APEX-STRATEGY — backtest d'une règle de pari sur l'historique réel")
    ap.add_argument("--spec", help="fichier JSON décrivant la règle")
    ap.add_argument("--fav-min", type=float); ap.add_argument("--fav-max", type=float)
    ap.add_argument("--opp-min", type=float); ap.add_argument("--opp-max", type=float)
    ap.add_argument("--max-diff", type=float); ap.add_argument("--min-diff", type=float)
    ap.add_argument("--draw-min", type=float); ap.add_argument("--draw-max", type=float)
    ap.add_argument("--pick", default="fav", help="fav|dog|home|away|draw")
    ap.add_argument("--market", default="1X2", help="1X2|DNB")
    ap.add_argument("--divs", help="codes ligue séparés par des virgules (E0,SP1,I1,D1,F1...)")
    ap.add_argument("--seasons", help="saisons football-data séparées par des virgules (2425,2526...)")
    ap.add_argument("--only-teams"); ap.add_argument("--exclude-teams",
                    help="fichier texte : une équipe par ligne")
    ap.add_argument("--by-season", action="store_true", help="afficher la ventilation par saison")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    s = {}
    if a.spec:
        s.update(json.load(open(a.spec)))
    for k in ("fav_min", "fav_max", "opp_min", "opp_max", "max_diff", "min_diff", "draw_min", "draw_max"):
        v = getattr(a, k.replace("_", "_"))
        if v is not None:
            s[k] = v
    if a.pick: s.setdefault("pick", a.pick)
    if a.market: s.setdefault("market", a.market)
    if a.divs: s["divs"] = set(a.divs.split(","))
    if a.seasons: s["seasons"] = set(a.seasons.split(","))
    for opt, key in (("only_teams", "only_teams"), ("exclude_teams", "exclude_teams")):
        path = getattr(a, opt)
        if path:
            s[key] = set(l.strip() for l in open(path) if l.strip())

    rows = load()
    market, pick, bets = backtest(rows, s)
    r = score(bets, market)

    if a.json:
        r2 = {k: v for k, v in r.items() if k != "by_season"}
        r2["by_season"] = {k: {"n": v[0], "roi": (v[1] - v[0]) / v[0] * 100 if v[0] else 0}
                           for k, v in r["by_season"].items()}
        print(json.dumps({"regle": {k: (sorted(v) if isinstance(v, set) else v) for k, v in s.items()},
                          "market": market, "pick": pick, "resultats": r2}, ensure_ascii=False, indent=1))
        return

    print(f"APEX-STRATEGY — backtest (base : {len(rows)} matchs, cotes Pinnacle)")
    print(f"Règle : {json.dumps({k:(sorted(v) if isinstance(v,set) else v) for k,v in s.items()}, ensure_ascii=False)}")
    print(f"Marché : {market} | camp : {pick}\n")
    if r["n"] == 0:
        print("0 pari — règle trop restrictive ou hors base."); return
    be = (1 / r["odds_avg"]) * 100 if r["odds_avg"] else 0
    flag = "  [BRUIT : échantillon < 200]" if r["n"] < 200 else ""
    print(f"Paris            : {r['n']}{flag}")
    print(f"Taux de victoire : {r['win']/r['n']*100:.1f}%   (nul {r['draws']/r['n']*100:.1f}%)")
    print(f"Cote moyenne     : {r['odds_avg']:.2f}  (seuil de rentabilité ≈ {be:.1f}% de réussite)")
    print(f"ROI (mise plate) : {r['roi']:+.1f}%   — sur cotes Pinnacle (marge ~2,5 %)")
    print(f"                   NB : sur un book soft (betclic/1xBet, marge 6–10 %), retrancher ~4 à 8 points.")
    if a.by_season and r["by_season"]:
        print("\nStabilité par saison :")
        for seas in sorted(r["by_season"]):
            n2, ret2 = r["by_season"][seas]
            if n2:
                print(f"  {seas or '----':6} : {n2:4d} paris | ROI {(ret2-n2)/n2*100:+6.1f}%")
    verdict = ("EDGE POSSIBLE (à confirmer : CLV + gros échantillon + book réel)"
               if r["roi"] > 2 and r["n"] >= 200 else
               "PAS D'EDGE (ROI ≤ marge ; perte réelle sur book soft)")
    print(f"\nVerdict : {verdict}. Gel maintenu — mesure, pas un pari.")


if __name__ == "__main__":
    main()
