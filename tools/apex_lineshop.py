#!/usr/bin/env python3
"""APEX-LINESHOP — le « line shopping » bat-il le marché ?

Hypothèse testée : la cote juste de Pinnacle en avant-match (démarginée) est la meilleure estimation
de vérité disponible (le backtest APEX-BSM l'a confirmé). Parier chez un autre bookmaker QUAND il paie
au-dessus de cette cote juste d'au moins un seuil donné produit-il un rendement positif sur 5 saisons ?

Sources de prix comparées (avant-match) : Bet365 (B365), Betway (BW), meilleure cote du marché (Max),
cote moyenne du marché (Avg). Marchés : 1X2 et Over/Under 2,5.
Contrôle : la cote prise bat-elle la cote de CLÔTURE de Pinnacle (Closing Line Value) ?

Aucune information future : la cote juste vient de Pinnacle AVANT le match, jamais de la clôture ni du résultat.
Règle fixée à l'avance (pas d'optimisation a posteriori) : mise de 1 unité par pari, seuil d'EV constant.

Usage : python3 tools/apex_lineshop.py [--divs E0,...] [--thresholds 0,0.02,0.03,0.05]
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
HIST = ROOT / "data" / "history"
OUT = ROOT / "backtests"


def demargin(o):
    p = np.array([1.0 / x for x in o])
    return p / p.sum()


def val(r, *cols):
    out = []
    for c in cols:
        try:
            v = float(r.get(c, "") or "nan")
        except ValueError:
            return None
        if not v > 1.0:
            return None
        out.append(v)
    return out


def load(divs, seasons):
    rows = []
    for d in divs:
        for s in seasons:
            p = HIST / f"{d}_{s}.csv"
            if not p.exists():
                continue
            for r in csv.DictReader(io.StringIO(p.read_text(encoding="utf-8-sig", errors="replace"))):
                if (r.get("FTHG") or "") == "" or not r.get("HomeTeam"):
                    continue
                try:
                    hg, ag = int(float(r["FTHG"])), int(float(r["FTAG"]))
                except ValueError:
                    continue
                ps = val(r, "PSH", "PSD", "PSA")
                if not ps:                       # sans Pinnacle avant-match, pas de référence
                    continue
                rows.append({"div": d, "season": s, "hg": hg, "ag": ag,
                             "res": 0 if hg > ag else 1 if hg == ag else 2,
                             "over": hg + ag > 2,
                             "ps": ps, "psc": val(r, "PSCH", "PSCD", "PSCA"),
                             "b365": val(r, "B365H", "B365D", "B365A"), "bw": val(r, "BWH", "BWD", "BWA"),
                             "max": val(r, "MaxH", "MaxD", "MaxA"), "avg": val(r, "AvgH", "AvgD", "AvgA"),
                             "ps_ou": val(r, "P>2.5", "P<2.5"), "psc_ou": val(r, "PC>2.5", "PC<2.5"),
                             "max_ou": val(r, "Max>2.5", "Max<2.5"), "avg_ou": val(r, "Avg>2.5", "Avg<2.5"),
                             "b365_ou": val(r, "B365>2.5", "B365<2.5")})
    return rows


def cluster_boot(bets, n=3000, seed=11):
    """IC95 du rendement, ré-échantillonnage par match (les paris d'un même match sont corrélés)."""
    if not bets:
        return [float("nan")] * 2
    by = {}
    for b in bets:
        by.setdefault(b["mid"], []).append(b["pnl"])
    keys = list(by)
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(n):
        pick = [by[keys[i]] for i in rng.integers(0, len(keys), len(keys))]
        flat = [x for grp in pick for x in grp]
        means.append(np.mean(flat))
    return [round(float(np.quantile(means, .025)), 4), round(float(np.quantile(means, .975)), 4)]


def strat(rows, source, thr):
    """Parie 1u sur chaque issue où source paie ≥ (1+thr) × cote juste Pinnacle. Renvoie la liste des paris."""
    bets = []
    for i, r in enumerate(rows):
        # 1X2
        o = r.get(source)
        if o:
            pf = demargin(r["ps"])
            for k in range(3):
                ev = o[k] * pf[k] - 1
                if ev >= thr:
                    won = r["res"] == k
                    clv = None
                    if r["psc"]:
                        clv = o[k] * demargin(r["psc"])[k] - 1     # >0 = on a battu la clôture Pinnacle
                    bets.append({"mid": f"{r['div']}{i}", "market": "1X2", "ev": ev,
                                 "pnl": o[k] - 1 if won else -1.0, "clv": clv})
        # Over/Under 2,5
        ou = r.get(f"{source}_ou")
        if ou and r["ps_ou"]:
            pf = demargin(r["ps_ou"])
            for k, won in ((0, r["over"]), (1, not r["over"])):
                ev = ou[k] * pf[k] - 1
                if ev >= thr:
                    clv = None
                    if r["psc_ou"]:
                        clv = ou[k] * demargin(r["psc_ou"])[k] - 1
                    bets.append({"mid": f"{r['div']}{i}", "market": "OU2.5", "ev": ev,
                                 "pnl": ou[k] - 1 if won else -1.0, "clv": clv})
    return bets


def summ(bets):
    if not bets:
        return {"n": 0}
    pnl = np.array([b["pnl"] for b in bets])
    clv = [b["clv"] for b in bets if b["clv"] is not None]
    return {"n": len(bets), "rendement": round(float(pnl.mean()), 4), "IC95": cluster_boot(bets),
            "pnl_total": round(float(pnl.sum()), 1),
            "clv_moyen": round(float(np.mean(clv)), 4) if clv else None,
            "part_clv_positif": round(float(np.mean([c > 0 for c in clv])), 3) if clv else None}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--divs", default="E0,E1,E2,E3,SP1,I1,D1,F1")
    ap.add_argument("--seasons", default="2122,2223,2324,2425,2526")
    ap.add_argument("--thresholds", default="0,0.01,0.02,0.03,0.05,0.07")
    a = ap.parse_args()
    divs = a.divs.split(","); seasons = a.seasons.split(",")
    thrs = [float(x) for x in a.thresholds.split(",")]
    rows = load(divs, seasons)
    SOURCES = {"Bet365": "b365", "Betway": "bw", "Meilleure cote marché (Max)": "max", "Cote moyenne (Avg)": "avg"}
    report = {"divs": divs, "seasons": seasons, "n_matchs": len(rows),
              "note_verite": "cote juste = Pinnacle avant-match démarginée ; contrôle CLV vs clôture Pinnacle",
              "regle": "1u par issue à EV ≥ seuil ; seuils fixés a priori, aucune optimisation",
              "resultats": {}}
    for name, src in SOURCES.items():
        report["resultats"][name] = {f"seuil_{t:g}": summ(strat(rows, src, t)) for t in thrs}
    # meilleure combinaison (Max, seuil 3 %) détaillée par saison et par marché
    best = strat(rows, "max", 0.03)
    by_season, by_market = {}, {}
    for b, r in [(b, None) for b in best]:
        pass
    # par saison : refaire en gardant la saison
    def strat_tagged(src, thr):
        out = []
        for i, r in enumerate(rows):
            o = r.get(src)
            if o:
                pf = demargin(r["ps"])
                for k in range(3):
                    if o[k] * pf[k] - 1 >= thr:
                        out.append({"mid": f"{r['div']}{i}", "market": "1X2", "season": r["season"],
                                    "pnl": o[k] - 1 if r["res"] == k else -1.0,
                                    "clv": o[k] * demargin(r["psc"])[k] - 1 if r["psc"] else None})
            ou = r.get(f"{src}_ou")
            if ou and r["ps_ou"]:
                pf = demargin(r["ps_ou"])
                for k, won in ((0, r["over"]), (1, not r["over"])):
                    if ou[k] * pf[k] - 1 >= thr:
                        out.append({"mid": f"{r['div']}{i}", "market": "OU2.5", "season": r["season"],
                                    "pnl": ou[k] - 1 if won else -1.0,
                                    "clv": ou[k] * demargin(r["psc_ou"])[k] - 1 if r["psc_ou"] else None})
        return out
    tagged = strat_tagged("max", 0.03)
    for s in seasons:
        by_season[s] = summ([b for b in tagged if b["season"] == s])
    for m in ("1X2", "OU2.5"):
        by_market[m] = summ([b for b in tagged if b["market"] == m])
    report["detail_max_seuil3pct"] = {"par_saison": by_season, "par_marche": by_market}

    OUT.mkdir(exist_ok=True)
    (OUT / "lineshop.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(render(report))


def render(R):
    L = [f"# Line shopping vs Pinnacle — {R['n_matchs']} matchs, {len(R['seasons'])} saisons",
         f"Ligues : {', '.join(R['divs'])} · saisons {', '.join(R['seasons'])}",
         f"Vérité : {R['note_verite']}", f"Règle : {R['regle']}", "",
         "## Rendement net par unité selon le bookmaker et le seuil d'EV", "",
         "| Bookmaker | seuil | n paris | rendement/u | IC95 | CLV moyen | % CLV>0 |",
         "|---|---|---|---|---|---|---|"]
    for name, d in R["resultats"].items():
        for st, s in d.items():
            if s["n"] == 0:
                continue
            t = st.replace("seuil_", "")
            L.append(f"| {name} | {t} | {s['n']} | {s['rendement']:+.3f} | {s['IC95']} | "
                     f"{s['clv_moyen'] if s['clv_moyen'] is not None else '—'} | {s['part_clv_positif'] if s['part_clv_positif'] is not None else '—'} |")
    d = R["detail_max_seuil3pct"]
    L += ["", "## Meilleure cote du marché, seuil 3 % — détail", "", "### Par saison", "",
          "| Saison | n | rendement/u | IC95 |", "|---|---|---|---|"]
    for s, v in d["par_saison"].items():
        if v["n"]:
            L.append(f"| {s} | {v['n']} | {v['rendement']:+.3f} | {v['IC95']} |")
    L += ["", "### Par marché", "", "| Marché | n | rendement/u | IC95 | CLV moyen |", "|---|---|---|---|---|"]
    for m, v in d["par_marche"].items():
        if v["n"]:
            L.append(f"| {m} | {v['n']} | {v['rendement']:+.3f} | {v['IC95']} | {v['clv_moyen']} |")
    L += ["", "**Lecture** : un rendement dont l'IC95 est entièrement > 0 est un vrai avantage. "
          "Un CLV moyen > 0 signifie qu'on a pris de meilleures cotes que la clôture de Pinnacle — "
          "le signe le plus fiable, à long terme, qu'une stratégie bat le marché."]
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    main()
