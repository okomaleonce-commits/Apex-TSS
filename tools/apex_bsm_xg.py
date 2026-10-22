#!/usr/bin/env python3
"""APEX-BSM-xG — banc d'essai (DIAGNOSTIC) : le xG réalisé passé améliore-t-il le moteur ?

Hypothèse : buts = xG + bruit de finition/gardien ; noter attaque/défense sur le xG de matchs
ANTÉRIEURS est un estimateur de force moins bruité. On ne change QUE la cible du fit (mélange
buts↔xG, poids w). Ce module ne price pas, n'émet aucun pari, ne touche NI au moteur gelé NI à
latest_params ; il écrit un rapport JSON de diagnostic (`promotion_authorized=false`).

Correctifs d'audit (2026‑10‑22) :
  • Benchmark EXPLICITE sans repli : `--benchmark prematch|closing` (aucun mélange des deux dans un ΔLL).
  • Pinnacle exclu à partir du 23/07/2025 (géré en amont par apex_bsm.fetch_matches : o1x2/close1x2 ne
    reprennent plus Pinnacle sur la période concernée).
  • Jointure football‑data↔xG : date + score final identiques ET identité des DEUX équipes ; tout
    appariement ambigu (≥2 candidats) ou réutilisé est REFUSÉ, jamais deviné. Couverture + refus publiés.
  • xG non finis / négatifs rejetés à l'ingestion ; donnée absente conservée absente (repli sur buts).
  • Inférence : **bootstrap groupé par semaine** (les matchs d'une même fenêtre restent ensemble) ;
    publication SÉPARÉE de 3 deltas — LL_buts−LL_marché, LL_xG−LL_marché, et LL_xG−LL_buts (apport
    incrémental du xG, apparié par match). Verdict : MEILLEUR (IC95<0) / PIRE (IC95>0) / NON_CONCLUANT
    (IC95 contient 0) ; < 5 semaines → DONNEES_INSUFFISANTES. IC chevauchant 0 ≠ équivalence.
  • Échec explicite si les paramètres gelés (ξ,K,ρ,σ) sont absents/invalides (pas de défaut silencieux).
  • Découpage validation/test disjoint et chronologique ; poids finis dans [0;1] ; témoin w=0 obligatoire.

Aucun statut VALIDÉ n'est délivré ici, et aucune saison déjà observée (≤ 2526) ne peut promouvoir le
moteur après cette correction (test déjà vu). EC et toute ligue sans xG restent HORS PORTÉE, écrites
absentes — jamais fabriquées.

Usage :
  python3 tools/apex_bsm_xg.py eval --div E0 --seasons 2122,2223,2324,2425,2526 \
      --val 2324,2425 --test 2526 --benchmark prematch   # (ou closing)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import json
import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import apex_bsm as B  # noqa: E402

FS_DIR = ROOT / "data" / "footystats"
MIN_WEEKS = 5                     # sous ce seuil : DONNEES_INSUFFISANTES, jamais « égalité »
WEIGHTS_DEFAULT = (0.0, 0.25, 0.5, 0.75, 1.0)


# ───────────────────────────── identité d'équipe & jointure ─────────────────────────────

def _norm(s: str) -> str:
    s = (s or "").lower()
    for a, b in (("&", " and "), ("'", ""), ("-", " "), (".", " ")):
        s = s.replace(a, b)
    return " ".join(s.split())


def _same_team(a: str, b: str) -> bool:
    """Identité robuste entre nom court (football‑data) et nom long (FootyStats) :
    « Wolves » ↔ « Wolverhampton Wanderers », « Man United » ↔ « Manchester United ».
    Vrai si ratio de séquence ≥ 0,6, OU tokens partagés (Jaccard ≥ 0,5), OU premier token commun
    sur ≥ 4 caractères (préfixe), OU inclusion d'un nom dans l'autre."""
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return False
    if na in nb or nb in na:
        return True
    if difflib.SequenceMatcher(None, na, nb).ratio() >= 0.60:
        return True
    ta, tb = set(na.split()), set(nb.split())
    if ta and tb and len(ta & tb) / len(ta | tb) >= 0.5:
        return True
    fa, fb = na.split()[0], nb.split()[0]
    if len(fa) >= 4 and len(fb) >= 4 and (fa.startswith(fb[:4]) or fb.startswith(fa[:4])):
        return True
    return False


def _load_xg(div: str, seasons: list[str]) -> tuple[dict, int]:
    """{date_iso: [ {home, away, hg, ag, xg_h, xg_a}, ... ]}. xG validé fini et ≥ 0 ; sinon ligne
    ignorée (absence conservée, jamais fabriquée). Renvoie (index, n_rejetés_num)."""
    by_date: dict = {}
    rejected = 0
    for s in seasons:
        p = FS_DIR / f"{div}_{s}.csv"
        if not p.exists():
            continue
        for r in csv.DictReader(p.open(encoding="utf-8")):
            try:
                xg_h = float(r["xg_h"]); xg_a = float(r["xg_a"])
                hg = int(r["hg"]); ag = int(r["ag"])
            except (TypeError, ValueError, KeyError):
                rejected += 1
                continue
            if not (math.isfinite(xg_h) and math.isfinite(xg_a)) or xg_h < 0 or xg_a < 0:
                rejected += 1
                continue
            if xg_h == 0 and xg_a == 0:        # ligne sans xG réel → absente, pas fabriquée
                continue
            by_date.setdefault(r["date_utc"], []).append(
                {"home": r["home"], "away": r["away"], "hg": hg, "ag": ag, "xg_h": xg_h, "xg_a": xg_a})
    return by_date, rejected


def attach_xg(matches: list[dict], by_date: dict) -> dict:
    """Attache xg_h/xg_a par (date, score final) ET identité des DEUX équipes. Refuse l'ambiguïté
    (≥2 candidats valides) et la réutilisation (une ligne xG sert au plus une fois). Renvoie un
    compte {apparié, ambigu, sans_candidat, total}. Les non‑appariés gardent xg=None (repli buts)."""
    consumed: set = set()
    stat = {"apparie": 0, "ambigu": 0, "sans_candidat": 0, "total": len(matches)}
    for m in matches:
        m["xg_h"] = m["xg_a"] = None
        cands = by_date.get(m["date"].isoformat(), [])
        hits = [(i, c) for i, c in enumerate(cands)
                if id(c) not in consumed and c["hg"] == m["hg"] and c["ag"] == m["ag"]
                and _same_team(m["home"], c["home"]) and _same_team(m["away"], c["away"])]
        if len(hits) == 1:
            _, c = hits[0]
            m["xg_h"], m["xg_a"] = c["xg_h"], c["xg_a"]
            consumed.add(id(c))
            stat["apparie"] += 1
        elif len(hits) >= 2:
            stat["ambigu"] += 1                # ambigu → refusé, jamais deviné
        else:
            stat["sans_candidat"] += 1
    return stat


# ───────────────────────────── fit des notes sur un mélange buts↔xG ─────────────────────────────

def fit_ratings_blend(hist, ref, xi, K, w_xg, iters=40):
    """Copie fidèle de apex_bsm.fit_ratings, cible = (1−w)·buts + w·xG. w=0 reproduit le moteur gelé ;
    un match sans xG retombe sur ses buts (pas d'invention)."""
    hist = [m for m in hist if 0 < (ref - m["date"]).days <= B.LOOKBACK_DAYS]
    if len(hist) < 30:
        return None
    teams = sorted({m["home"] for m in hist} | {m["away"] for m in hist})
    ix = {t: i for i, t in enumerate(teams)}
    h = np.array([ix[m["home"]] for m in hist]); a = np.array([ix[m["away"]] for m in hist])

    def blend(gk, xk):
        return np.array([((1 - w_xg) * float(m[gk]) + w_xg * float(m[xk]))
                         if (w_xg > 0 and m.get(xk) is not None) else float(m[gk]) for m in hist], float)

    hg = blend("hg", "xg_h"); ag = blend("ag", "xg_a")
    w = np.exp(-xi * np.array([(ref - m["date"]).days for m in hist], float))
    n = len(teams)
    att = np.zeros(n); dfn = np.zeros(n)
    c = math.log(max((w * ag).sum() / w.sum(), 0.2))
    H = math.log(max((w * hg).sum(), 1e-6) / max((w * ag).sum(), 1e-6))
    for _ in range(iters):
        eh = w * np.exp(c + H - dfn[a]); ea = w * np.exp(c - dfn[h])
        G = np.bincount(h, w * hg, n) + np.bincount(a, w * ag, n)
        E = np.bincount(h, eh, n) + np.bincount(a, ea, n)
        att = np.log((G + K) / (E + K))
        fh = w * np.exp(c + H + att[h]); fa = w * np.exp(c + att[a])
        C = np.bincount(a, w * hg, n) + np.bincount(h, w * ag, n)
        F = np.bincount(a, fh, n) + np.bincount(h, fa, n)
        dfn = -np.log((C + K) / (F + K))
        att -= att.mean(); dfn -= dfn.mean()
        c = math.log((w * ag).sum() / (w * np.exp(att[a] - dfn[h])).sum())
        H = math.log((w * hg).sum() / (w * np.exp(att[h] - dfn[a])).sum()) - c
    return {"teams": {t: (float(att[i]), float(dfn[i])) for t, i in ix.items()}, "c": c, "H": H}


# ───────────────────────────── évaluation walk-forward ─────────────────────────────

def _market_probs(m, benchmark: str):
    """Probas marché dé‑marginées du benchmark CHOISI, SANS repli. None si absent."""
    o = m.get("close1x2") if benchmark == "closing" else m.get("o1x2")
    return B.demargin(o) if o else None


def ll_records(all_m, seasons_eval, benchmark, w, xi, K, rho, sigma):
    """Enregistrements LL par match (walk‑forward hebdo, notes antérieures au lundi). Le set de matchs
    est identique pour tout w (le fit None ne dépend que de l'historique), donc appariable par id."""
    recs = []
    tgt = [m for m in all_m if m["season"] in seasons_eval]
    for wk in sorted({B.week_key(m["date"]) for m in tgt}):
        wm = [m for m in tgt if B.week_key(m["date"]) == wk]
        ref = min(m["date"] for m in wm)
        ref = ref - dt.timedelta(days=ref.weekday())
        R = fit_ratings_blend([m for m in all_m if m["date"] < ref], ref, xi, K, w)
        if R is None:
            continue
        for m in wm:
            mk = _market_probs(m, benchmark)
            if mk is None:
                continue
            lh, la = B.lambdas(R, m["home"], m["away"])
            P = B.probs_from_matrix(B.score_matrix(lh, la, rho, sigma))["1X2"]
            k = B.outcome_idx(m)
            recs.append({"id": m["id"], "week": wk,
                         "ll_model": -math.log(max(P[k], 1e-12)),
                         "ll_market": -math.log(max(mk[k], 1e-12))})
    return recs


def _cluster_ci(items, n=2000, seed=7):
    """Bootstrap GROUPÉ par semaine : (moyenne, lo, hi) sur items=[(week, value)]. Préserve la
    dépendance intra‑semaine. (None,None,None) si vide."""
    from collections import defaultdict
    byw = defaultdict(list)
    for wk, v in items:
        byw[wk].append(v)
    weeks = list(byw)
    if not weeks:
        return (None, None, None)
    allv = [v for _, v in items]
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(n):
        idx = rng.integers(0, len(weeks), size=len(weeks))
        vals = [v for i in idx for v in byw[weeks[i]]]
        means.append(float(np.mean(vals)))
    lo, hi = np.percentile(means, [2.5, 97.5])
    return (round(float(np.mean(allv)), 4), round(float(lo), 4), round(float(hi), 4))


def _verdict(ci, n_weeks):
    if n_weeks < MIN_WEEKS or ci[0] is None:
        return "DONNEES_INSUFFISANTES"
    _, lo, hi = ci
    if hi < 0:
        return "MEILLEUR"
    if lo > 0:
        return "PIRE"
    return "NON_CONCLUANT"


def _delta_items(recs, key_a, key_b):
    return [(r["week"], r[key_a] - r[key_b]) for r in recs]


def run_eval(div, seasons, weights, val_seasons, test_seasons, benchmark):
    # Paramètres gelés OBLIGATOIRES — échec explicite, pas de défaut silencieux.
    p = ROOT / "backtests" / "latest_params.json"
    if not p.exists():
        sys.exit("ÉCHEC : backtests/latest_params.json absent — paramètres gelés requis.")
    params = json.loads(p.read_text(encoding="utf-8"))
    try:
        xi = float(params["xi"]); K = float(params["K"]); rho = float(params["rho"]); sigma = float(params["sigma"])
    except (KeyError, TypeError, ValueError) as e:
        sys.exit(f"ÉCHEC : paramètres gelés invalides ({e}).")
    if benchmark not in ("prematch", "closing"):
        sys.exit("ÉCHEC : --benchmark doit être 'prematch' ou 'closing'.")
    if set(val_seasons) & set(test_seasons):
        sys.exit("ÉCHEC : validation et test doivent être disjoints.")
    weights = [w for w in weights if 0.0 <= w <= 1.0]
    if 0.0 not in weights:
        weights = [0.0] + weights                   # témoin w=0 obligatoire

    all_m = []
    for s in seasons:
        all_m += B.fetch_matches(div, s, refresh=False)
    by_date, n_rej = _load_xg(div, seasons)
    cov = attach_xg(all_m, by_date)
    if cov["apparie"] == 0:
        sys.exit(f"ÉCHEC : aucun xG apparié pour {div} (xG absent ou non apparié) — hors portée, écrit absent.")

    # Choix du poids sur la VALIDATION (ΔLL vs marché le plus bas), témoin w=0 inclus.
    val_by_w = {}
    for w in weights:
        recs = ll_records(all_m, val_seasons, benchmark, w, xi, K, rho, sigma)
        if recs:
            val_by_w[w] = float(np.mean([r["ll_model"] - r["ll_market"] for r in recs]))
    if not val_by_w:
        sys.exit("ÉCHEC : aucune donnée de validation exploitable.")
    w_star = min(val_by_w, key=val_by_w.get)

    # TEST hors échantillon : w=0 (buts) et w* (xG), appariés par match.
    recs0 = ll_records(all_m, test_seasons, benchmark, 0.0, xi, K, rho, sigma)
    recsW = ll_records(all_m, test_seasons, benchmark, w_star, xi, K, rho, sigma)
    by_id0 = {r["id"]: r for r in recs0}
    paired = [(recsW_i["week"], recsW_i["ll_model"] - by_id0[recsW_i["id"]]["ll_model"])
              for recsW_i in recsW if recsW_i["id"] in by_id0]  # LL_xG − LL_buts (apport incrémental)
    n_weeks = len({r["week"] for r in recs0})

    ci_buts = _cluster_ci(_delta_items(recs0, "ll_model", "ll_market"))
    ci_xg = _cluster_ci(_delta_items(recsW, "ll_model", "ll_market"))
    ci_gain = _cluster_ci(paired)

    report = {
        "outil": "apex_bsm_xg", "div": div, "benchmark": benchmark, "promotion_authorized": False,
        "seasons": seasons, "val_seasons": val_seasons, "test_seasons": test_seasons,
        "params_geles": {"xi": xi, "K": K, "rho": rho, "sigma": sigma, "run": params.get("run_id")},
        "xG_couverture": {**cov, "rejets_numeriques": n_rej,
                          "taux_apparie": round(cov["apparie"] / max(cov["total"], 1), 4)},
        "poids_teste": weights, "w_star_validation": w_star, "val_delta_par_poids": val_by_w,
        "test": {
            "n_semaines": n_weeks, "n_matchs": len(recs0),
            "delta_buts_vs_marche": {"moyenne": ci_buts[0], "IC95": [ci_buts[1], ci_buts[2]],
                                     "verdict": _verdict(ci_buts, n_weeks)},
            "delta_xg_vs_marche": {"w": w_star, "moyenne": ci_xg[0], "IC95": [ci_xg[1], ci_xg[2]],
                                   "verdict": _verdict(ci_xg, n_weeks)},
            "apport_xg_vs_buts": {"w": w_star, "moyenne": ci_gain[0], "IC95": [ci_gain[1], ci_gain[2]],
                                  "verdict_gain": _verdict(ci_gain, n_weeks),
                                  "lecture": "négatif = le xG réduit la log-loss vs buts"},
        },
        "source_benchmark": "football-data (Pinnacle exclu ≥ 23/07/2025 ; moyenne/Bet365 sinon)",
        "note": "DIAGNOSTIC — aucune mise, aucun statut VALIDÉ, latest_params non modifié ; "
                "les saisons ≤ 2526 sont déjà observées et ne peuvent pas promouvoir le moteur.",
        "genere_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    outdir = ROOT / "backtests" / f"xg-{div}-{benchmark}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _print(report, outdir)
    return 0


def _print(r, outdir):
    t = r["test"]
    print(f"APEX-BSM-xG [DIAGNOSTIC] · {r['div']} · benchmark={r['benchmark']} · "
          f"params {r['params_geles']['run']} (ξ={r['params_geles']['xi']} K={r['params_geles']['K']} "
          f"ρ={r['params_geles']['rho']} σ={r['params_geles']['sigma']})")
    c = r["xG_couverture"]
    print(f"xG apparié {c['apparie']}/{c['total']} ({c['taux_apparie']:.1%}) · ambigus {c['ambigu']} · "
          f"sans candidat {c['sans_candidat']} · rejets num. {c['rejets_numeriques']}")
    print(f"Validation : {','.join(r['val_seasons'])} → w*={r['w_star_validation']} · "
          f"Test : {','.join(r['test_seasons'])} ({t['n_matchs']} matchs / {t['n_semaines']} semaines)")
    print("(ΔLL < 0 = modèle MEILLEUR ; IC95 bootstrap GROUPÉ PAR SEMAINE ; IC contenant 0 = NON_CONCLUANT)")

    def line(lbl, d):
        v = d.get("verdict") or d.get("verdict_gain")
        print(f"  {lbl:<22} moyenne={d['moyenne']:+.4f}  IC95=[{d['IC95'][0]:+.4f},{d['IC95'][1]:+.4f}]  → {v}")
    line("buts − marché", t["delta_buts_vs_marche"])
    line(f"xG(w={t['delta_xg_vs_marche']['w']}) − marché", t["delta_xg_vs_marche"])
    line(f"xG(w={t['apport_xg_vs_buts']['w']}) − buts", t["apport_xg_vs_buts"])
    print(f"→ rapport : {outdir.relative_to(ROOT)}/report.json · promotion_authorized=false")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    e = sp.add_parser("eval", help="DIAGNOSTIC : buts vs xG vs marché, benchmark explicite")
    e.add_argument("--div", required=True)
    e.add_argument("--seasons", default="2122,2223,2324,2425,2526")
    e.add_argument("--weights", default=",".join(str(w) for w in WEIGHTS_DEFAULT))
    e.add_argument("--val", default="2324,2425")
    e.add_argument("--test", default="2526")
    e.add_argument("--benchmark", required=True, choices=["prematch", "closing"])
    a = p.parse_args()
    seasons = [s.strip() for s in a.seasons.split(",") if s.strip()]
    weights = [float(x) for x in a.weights.split(",") if x.strip()]
    val = [s.strip() for s in a.val.split(",") if s.strip()]
    test = [s.strip() for s in a.test.split(",") if s.strip()]
    return run_eval(a.div, seasons, weights, val, test, a.benchmark)


if __name__ == "__main__":
    raise SystemExit(main())
