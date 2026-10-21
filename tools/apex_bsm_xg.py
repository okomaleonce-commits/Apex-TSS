#!/usr/bin/env python3
"""APEX-BSM-xG — banc d'essai : noter les équipes sur le xG (réalisé passé) améliore-t-il le modèle ?

Hypothèse testée, honnête et mesurable : les buts = xG + bruit de finition/gardien. Noter l'attaque
et la défense d'une équipe sur le xG de ses matchs PASSÉS (strictement antérieurs, walk-forward) est un
estimateur de force moins bruité que les buts → de meilleurs λ → des probabilités mieux calibrées.

Ce module NE TOUCHE PAS au cœur gelé apex_bsm. Il réutilise sa mécanique (fit Poisson, Dixon-Coles,
dérivation des marchés, démarginage du marché, log-loss) et ne change qu'UNE chose : la cible sur
laquelle les notes attaque/défense sont ajustées — un mélange buts↔xG réglé par un seul poids `w_xg`.

  signal_marqués = (1 − w_xg)·buts + w_xg·xG      (w_xg=0 → modèle actuel ; w_xg=1 → tout xG)

Données :
  • matchs + cotes + score final + cote de clôture  : football-data.co.uk (via apex_bsm.fetch_matches) ;
  • xG réalisé par match                            : FootyStats (data/footystats/<div>_<saison>.csv,
                                                      produit par apex_footystats.py history).
Jointure robuste aux noms (« Man United » ↔ « Manchester United ») par (date, score final), départage
par similarité de nom. Un match sans xG apparié retombe sur les buts (jamais inventé).

ANTI-INVENTION : on n'utilise que le xG réalisé de matchs ANTÉRIEURS au match prédit — jamais le xG du
match lui-même. Mesure walk-forward, aucun pari réel, le gel reste entier. EC (National League) n'a pas
de xG chez FootyStats : ce module le refuse explicitement (couverture = 0).

Usage :
  python3 tools/apex_bsm_xg.py eval [--div E0] [--seasons 2122,2223,2324,2425,2526]
                                     [--weights 0,0.25,0.5,0.75,1.0] [--val 2324,2425] [--test 2526]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import difflib
import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import apex_bsm as B  # noqa: E402  (cœur gelé réutilisé tel quel)

FS_DIR = ROOT / "data" / "footystats"


# ───────────────────────────── jointure football-data ↔ xG FootyStats ─────────────────────────────

def _norm(s: str) -> str:
    s = (s or "").lower()
    for a, b in (("&", " and "), ("'", ""), ("-", " "), (".", " ")):
        s = s.replace(a, b)
    return " ".join(s.split())


def _load_xg(div: str, seasons: list[str]) -> dict:
    """{date_iso: [ {home, away, hg, ag, xg_h, xg_a}, ... ]} depuis les CSV FootyStats, xG réalisé seul."""
    by_date: dict = {}
    for s in seasons:
        p = FS_DIR / f"{div}_{s}.csv"
        if not p.exists():
            continue
        for r in csv.DictReader(p.open(encoding="utf-8")):
            try:
                xg_h = float(r["xg_h"]); xg_a = float(r["xg_a"])
            except (TypeError, ValueError, KeyError):
                continue
            if xg_h <= 0 and xg_a <= 0:          # ligne sans xG réel → ignorée (jamais fabriquée)
                continue
            by_date.setdefault(r["date_utc"], []).append({
                "home": r["home"], "away": r["away"],
                "hg": int(r["hg"]), "ag": int(r["ag"]), "xg_h": xg_h, "xg_a": xg_a})
    return by_date


def attach_xg(matches: list[dict], by_date: dict) -> tuple[int, int]:
    """Attache xg_h/xg_a à chaque match football-data par (date, score final), départage par nom.
    Retourne (n_appariés, n_total). Les non-appariés gardent xg_h/xg_a = None (repli sur buts)."""
    n_ok = 0
    for m in matches:
        m["xg_h"] = m["xg_a"] = None
        cands = by_date.get(m["date"].isoformat(), [])
        if not cands:
            continue
        same = [c for c in cands if c["hg"] == m["hg"] and c["ag"] == m["ag"]]
        pool = same or cands
        best, best_sim = None, -1.0
        for c in pool:
            sim = (difflib.SequenceMatcher(None, _norm(m["home"]), _norm(c["home"])).ratio()
                   + difflib.SequenceMatcher(None, _norm(m["away"]), _norm(c["away"])).ratio())
            if sim > best_sim:
                best, best_sim = c, sim
        # score identique → appariement sûr ; sinon exiger une forte similarité de noms
        if best is not None and (same or best_sim >= 1.2):
            m["xg_h"], m["xg_a"] = best["xg_h"], best["xg_a"]
            n_ok += 1
    return n_ok, len(matches)


# ───────────────────────────── fit des notes sur un mélange buts↔xG ─────────────────────────────

def fit_ratings_blend(hist: list[dict], ref: dt.date, xi: float, K: float, w_xg: float, iters: int = 40):
    """Copie fidèle de apex_bsm.fit_ratings, mais la cible marquée/encaissée est un mélange buts↔xG.
    w_xg=0 reproduit exactement le modèle gelé. Un match sans xG retombe sur ses buts (pas d'invention)."""
    hist = [m for m in hist if 0 < (ref - m["date"]).days <= B.LOOKBACK_DAYS]
    if len(hist) < 30:
        return None
    teams = sorted({m["home"] for m in hist} | {m["away"] for m in hist})
    ix = {t: i for i, t in enumerate(teams)}
    h = np.array([ix[m["home"]] for m in hist]); a = np.array([ix[m["away"]] for m in hist])

    def blend(goal_key, xg_key):
        out = []
        for m in hist:
            g = float(m[goal_key])
            x = m.get(xg_key)
            out.append((1 - w_xg) * g + w_xg * float(x) if (w_xg > 0 and x is not None) else g)
        return np.array(out, float)

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


# ───────────────────────────── walk-forward + écart au marché ─────────────────────────────

def _market_probs(m):
    """Proba marché démarginée : cote de clôture si dispo (plus sharp), sinon cote d'avant-match."""
    o = m.get("close1x2") or m.get("o1x2")
    return B.demargin(o) if o else None


def eval_weight(all_m, seasons_eval, xi, K, rho, sigma, w_xg):
    """Walk-forward hebdomadaire (comme apex_bsm) pour un poids xG donné. Renvoie la liste des
    (ll_modèle − ll_marché) par match, sur l'échantillon commun où le marché est connu."""
    diffs = []
    tgt = [m for m in all_m if m["season"] in seasons_eval]
    weeks = sorted({B.week_key(m["date"]) for m in tgt})
    for wk in weeks:
        wm = [m for m in tgt if B.week_key(m["date"]) == wk]
        ref = min(m["date"] for m in wm)
        ref = ref - dt.timedelta(days=ref.weekday())
        R = fit_ratings_blend([m for m in all_m if m["date"] < ref], ref, xi, K, w_xg)
        if R is None:
            continue
        for m in wm:
            mk = _market_probs(m)
            if mk is None:
                continue
            lh, la = B.lambdas(R, m["home"], m["away"])
            P = B.probs_from_matrix(B.score_matrix(lh, la, rho, sigma))["1X2"]
            k = B.outcome_idx(m)
            diffs.append(-math.log(max(P[k], 1e-12)) + math.log(max(mk[k], 1e-12)))
    return diffs


def run_eval(div, seasons, weights, val_seasons, test_seasons):
    if div == "EC":
        sys.exit("EC (National League) : aucun xG chez FootyStats (couverture = 0). "
                 "Le levier xG ne s'applique pas à EC — écrit absent, jamais fabriqué.")
    # paramètres figés au meilleur run officiel pour isoler l'effet du xG (pas de re-grille → pas de surajustement)
    try:
        import json
        p = json.loads((ROOT / "backtests" / "latest_params.json").read_text())
        xi, K, rho, sigma = p.get("xi", 0.003), p.get("K", 6.0), p.get("rho", -0.05), p.get("sigma", 0.0)
    except Exception:
        xi, K, rho, sigma = 0.003, 6.0, -0.05, 0.0

    all_m = []
    for s in seasons:
        all_m += B.fetch_matches(div, s, refresh=False)
    by_date = _load_xg(div, seasons)
    n_ok, n_tot = attach_xg(all_m, by_date)
    print(f"APEX-BSM-xG · {div} · {len(seasons)} saisons · xi={xi} K={K} rho={rho} sigma={sigma}")
    print(f"xG apparié : {n_ok}/{n_tot} matchs ({100*n_ok/max(n_tot,1):.1f} %) "
          f"— non appariés : repli sur les buts.")
    print(f"Validation : {','.join(val_seasons)} · Test final : {','.join(test_seasons)}")
    print("(delta = ll_modèle − ll_marché ; NÉGATIF = modèle MEILLEUR que le marché ; "
          "IC95 entièrement < 0 = supérieur prouvé)\n")

    def line(tag, seasons_eval, w):
        d = eval_weight(all_m, seasons_eval, xi, K, rho, sigma, w)
        if not d:
            return None
        mean = float(np.mean(d)); ci = B.boot_ci(np.array(d))
        verdict = "MEILLEUR" if ci[1] < 0 else ("pire" if ci[0] > 0 else "à égalité")
        print(f"  w_xg={w:<4} {tag:<12} n={len(d):4}  delta={mean:+.4f}  "
              f"IC95=[{ci[0]:+.4f},{ci[1]:+.4f}]  → {verdict}")
        return mean, ci

    print("— Validation (choix du poids) —")
    best_w, best_mean = None, None
    for w in weights:
        r = line("val", val_seasons, w)
        if r and (best_mean is None or r[0] < best_mean):
            best_mean, best_w = r[0], w
    print(f"\n  → poids retenu sur validation : w_xg={best_w} (delta val le plus bas)\n")

    print("— Test final (hors échantillon) —")
    base = line("test w=0", test_seasons, 0.0)        # modèle actuel (buts)
    chosen = line(f"test w={best_w}", test_seasons, best_w) if best_w not in (0.0, None) else None
    print()
    if chosen and base:
        gain = base[0] - chosen[0]
        print(f"Effet xG sur le test : delta passe de {base[0]:+.4f} (buts) à {chosen[0]:+.4f} "
              f"(xG w={best_w}) — {'rapproche du marché' if gain>0 else 'éloigne du marché'} "
              f"de {abs(gain):.4f} en log-loss.")
        if chosen[1][1] < 0:
            print("→ IC95 test entièrement < 0 : le modèle xG BAT le marché sur E0 hors échantillon.")
        elif chosen[0] < 0:
            print("→ delta test négatif mais IC95 touche 0 : amélioration réelle, pas encore significative.")
        else:
            print("→ delta test encore positif : le xG rapproche mais ne suffit pas à battre le marché.")
    elif best_w in (0.0, None):
        print("La validation n'a retenu aucun poids xG > 0 : le xG n'améliore pas sur ces saisons.")
    print("\nMesure walk-forward, aucun pari réel — le gel reste entier. Le juge en avant reste le CLV réel.")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    e = sp.add_parser("eval", help="compare buts vs xG en walk-forward, écart au marché")
    e.add_argument("--div", default="E0")
    e.add_argument("--seasons", default="2122,2223,2324,2425,2526")
    e.add_argument("--weights", default="0,0.25,0.5,0.75,1.0")
    e.add_argument("--val", default="2324,2425")
    e.add_argument("--test", default="2526")
    a = p.parse_args()
    seasons = [s.strip() for s in a.seasons.split(",") if s.strip()]
    weights = [float(x) for x in a.weights.split(",") if x.strip()]
    val = [s.strip() for s in a.val.split(",") if s.strip()]
    test = [s.strip() for s in a.test.split(",") if s.strip()]
    return run_eval(a.div, seasons, weights, val, test)


if __name__ == "__main__":
    raise SystemExit(main())
