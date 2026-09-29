#!/usr/bin/env python3
"""APEX-INTL — backtest de la conversion Elo → buts attendus pour les matchs de sélections.

Commandes
---------
  backtest   Elo reconstruit match par match (aucune information future), puis comparaison
             chronologique des conversions Elo→λ : heuristique actuelle (H0), régression de Poisson
             estimée (H1, variantes), référence simple. Apprentissage / validation / test final isolé.
  lambdas    λ d'un match à venir avec la conversion gelée et l'Elo le plus récent.

Données : github.com/martj42/international_results (résultats officiels depuis 1872), complétées au besoin par
API-Football (--supplement, matchs récents absents de la source). Aucune cote historique n'est disponible pour
les sélections : le modèle ne peut PAS être comparé au marché par ce backtest.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
import sys
import urllib.request
from pathlib import Path

import numpy as np
from scipy.optimize import minimize

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_bsm as B  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SRC = "https://raw.githubusercontent.com/martj42/international_results/master/results.csv"
CACHE = ROOT / "data" / "history" / "intl_results.csv"
SUPP = ROOT / "data" / "history" / "intl_supplement.csv"
BT = ROOT / "backtests"
VERSION = "apex-intl-1.1.0"

# Correspondance noms API-Football / eloratings → noms de la source de résultats
ALIASES = {"Cape Verde Islands": "Cape Verde", "FYR Macedonia": "North Macedonia", "Czechia": "Czech Republic",
           "Rep. Of Ireland": "Republic of Ireland", "Ireland": "Republic of Ireland", "Türkiye": "Turkey",
           "US Virgin Islands": "United States Virgin Islands", "French Guyana": "French Guiana",
           "Congo DR": "DR Congo", "Korea Republic": "South Korea", "USA": "United States", "Bosnia & Herzegovina":
           "Bosnia and Herzegovina", "Chinese Taipei": "Taiwan", "Curacao": "Curaçao", "Sao Tome and Principe":
           "São Tomé and Príncipe", "Swaziland": "Eswatini", "Gambia": "Gambia", "Congo": "Congo"}

FINALS = ("FIFA World Cup",)
CONT_FINALS = ("UEFA Euro", "Copa América", "African Cup of Nations", "AFC Asian Cup", "Gold Cup",
               "CONCACAF Championship", "Confederations Cup", "Oceania Nations Cup")


def canon(n: str) -> str:
    return ALIASES.get(n, n)


def k_factor(t: str) -> float:
    """Pondération de type World Football Elo."""
    if t in FINALS:
        return 60
    if t in CONT_FINALS:
        return 50
    if "qualification" in t or "Nations League" in t:
        return 40
    if t == "Friendly":
        return 20
    return 30


def kind(t: str) -> str:
    if t == "Friendly":
        return "amical"
    if "qualification" in t:
        return "qualification"
    if "Nations League" in t:
        return "ligue des nations"
    if t in FINALS or t in CONT_FINALS:
        return "phase finale"
    return "autre tournoi"


GROUPS = ["africa_q", "uefa_nl", "uefa_q", "concacaf", "amical", "phase_finale"]   # référence = autres


def group(t: str) -> int:
    if t == "Friendly":
        return GROUPS.index("amical") + 1
    if t in FINALS or t in CONT_FINALS:
        return GROUPS.index("phase_finale") + 1
    if t in ("African Cup of Nations qualification", "African Nations Championship qualification"):
        return GROUPS.index("africa_q") + 1
    if t == "UEFA Nations League":
        return GROUPS.index("uefa_nl") + 1
    if t in ("UEFA Euro qualification",) or (t == "FIFA World Cup qualification" and False):
        return GROUPS.index("uefa_q") + 1
    if "CONCACAF" in t or t == "Gold Cup qualification":
        return GROUPS.index("concacaf") + 1
    return 0


def load(refresh=False):
    if refresh or not CACHE.exists():
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_bytes(urllib.request.urlopen(urllib.request.Request(SRC, headers={"User-Agent": "apex"}), timeout=60).read())
    rows = []
    for path in (CACHE, SUPP):
        if not path.exists():
            continue
        for r in csv.DictReader(io.StringIO(path.read_text(encoding="utf-8"))):
            try:
                hs, as_ = int(r["home_score"]), int(r["away_score"])
            except (ValueError, TypeError):
                continue
            rows.append({"date": dt.date.fromisoformat(r["date"]), "home": canon(r["home_team"]), "away": canon(r["away_team"]),
                         "hs": hs, "as": as_, "tournament": r["tournament"], "neutral": r["neutral"].upper() == "TRUE",
                         "src": path.name})
    rows.sort(key=lambda r: r["date"])
    seen, out = set(), []
    for r in rows:                     # dédoublonnage source / complément
        k = (r["date"], r["home"], r["away"])
        if k not in seen:
            seen.add(k); out.append(r)
    return out


def run_elo(rows):
    """Elo avant-match (point-in-time) pour chaque ligne ; renvoie aussi les notes finales."""
    R = {}
    for r in rows:
        eh, ea = R.get(r["home"], 1500.0), R.get(r["away"], 1500.0)
        r["elo_h"], r["elo_a"] = eh, ea
        r["n_h"], r["n_a"] = r.get("n_h", 0), r.get("n_a", 0)
        dr = eh - ea + (0 if r["neutral"] else 100)
        we = 1 / (10 ** (-dr / 400) + 1)
        w = 1.0 if r["hs"] > r["as"] else 0.5 if r["hs"] == r["as"] else 0.0
        n = abs(r["hs"] - r["as"])
        g = 1 if n <= 1 else 1.5 if n == 2 else (11 + n) / 8
        d = k_factor(r["tournament"]) * g * (w - we)
        R[r["home"]] = eh + d; R[r["away"]] = ea - d
    return R


# ───────── conversions Elo → λ ─────────

def h0_lambdas(eh, ea, tournament):
    """Heuristique utilisée jusqu'ici (analyses/2026-09-29-bsm/run.py) : +100 toujours, total fixé par type."""
    base_T = 2.45 if tournament == "UEFA Nations League" else 2.2
    we = 1 / (10 ** (-(eh - ea + 100) / 400) + 1)
    T = base_T + 4.2 * (we - 0.5) ** 2
    lo, hi = -6.0, 6.0
    for _ in range(40):                # recherche de la supériorité reproduisant p1 + ½pX = We
        sup = (lo + hi) / 2
        lh, la = max((T + sup) / 2, 0.02), max((T - sup) / 2, 0.02)
        p = B.probs_from_matrix(B.dc_matrix(lh, la, -0.05))["1X2"]
        if p[0] + 0.5 * p[1] < we:
            lo = sup
        else:
            hi = sup
    return lh, la


def design(rows, quad, friendly):
    x = np.array([(r["elo_h"] - r["elo_a"]) / 400 for r in rows])
    grp = np.array([group(r["tournament"]) for r in rows])
    home = np.array([0.0 if r["neutral"] else 1.0 for r in rows])
    fr = np.array([1.0 if r["tournament"] == "Friendly" else 0.0 for r in rows])
    hs = np.array([r["hs"] for r in rows], float); as_ = np.array([r["as"] for r in rows], float)
    return x, home, fr, hs, as_, grp


def h1_rates(th, x, home, fr, quad, friendly, grp=None):
    a, b, c, f, h = th[:5]
    z = x + h * home / 4                # h en centaines de points Elo
    base = a + (c * z ** 2 if quad else 0) + (f * fr if friendly else 0)
    if len(th) > 5 and grp is not None:  # niveau de buts propre à la compétition
        base = base + np.concatenate([[0.0], np.asarray(th[5:])])[grp]
    return np.exp(base + b * z), np.exp(base - b * z)


def fit_h1(rows, quad, friendly, groups=False):
    x, home, fr, hs, as_, grp = design(rows, quad, friendly)

    def nll(th):
        lh, la = h1_rates(th, x, home, fr, quad, friendly, grp)
        return float((lh - hs * np.log(lh)).sum() + (la - as_ * np.log(la)).sum())
    th0 = [0.2, 0.9, 0.0, 0.0, 1.0] + ([0.0] * len(GROUPS) if groups else [])
    res = minimize(nll, np.array(th0), method="L-BFGS-B")
    return res.x.tolist()


def naive_rates(train):
    """Référence simple : moyennes de buts domicile/extérieur, séparées terrain neutre / non neutre."""
    out = {}
    for neu in (False, True):
        s = [r for r in train if r["neutral"] == neu]
        out[neu] = (float(np.mean([r["hs"] for r in s])), float(np.mean([r["as"] for r in s])))
    return out


def score(rows, lam, rho):
    recs = []
    for r in rows:
        lh, la = lam(r)
        M = B.dc_matrix(max(lh, 0.02), max(la, 0.02), rho)
        P = B.probs_from_matrix(M)
        k = 0 if r["hs"] > r["as"] else 1 if r["hs"] == r["as"] else 2
        recs.append({"r": r, "p": P["1X2"], "o": P["O25"], "b": P["BTTS"], "k": k, "lh": lh, "la": la,
                     "ps": float(M[min(r["hs"], 10), min(r["as"], 10)])})
    return recs


def summarize(recs):
    ll = [-math.log(max(x["p"][x["k"]], 1e-12)) for x in recs]
    over = [x["r"]["hs"] + x["r"]["as"] > 2 for x in recs]
    btts = [x["r"]["hs"] > 0 and x["r"]["as"] > 0 for x in recs]
    return {"n": len(recs), "logloss_1x2": round(float(np.mean(ll)), 4),
            "brier_1x2": round(float(np.mean([B.m_brier3(x["p"], x["k"]) for x in recs])), 4),
            "rps_1x2": round(float(np.mean([B.m_rps(x["p"], x["k"]) for x in recs])), 4),
            "logloss_ou25": round(float(np.mean([B.m_ll(x["o"], y) for x, y in zip(recs, over)])), 4),
            "logloss_btts": round(float(np.mean([B.m_ll(x["b"], y) for x, y in zip(recs, btts)])), 4),
            "logloss_score": round(float(np.mean([-math.log(max(x["ps"], 1e-12)) for x in recs])), 4),
            "buts_predits": round(float(np.mean([x["lh"] + x["la"] for x in recs])), 3),
            "buts_observes": round(float(np.mean([x["r"]["hs"] + x["r"]["as"] for x in recs])), 3)}


def cmd_backtest(a):
    rows = load(a.refresh)
    final = run_elo(rows)
    # seules les équipes ayant déjà ≥ 10 matchs ont un Elo informatif ; on évalue à partir de 2008
    cnt = {}
    for r in rows:
        r["exp_h"], r["exp_a"] = cnt.get(r["home"], 0), cnt.get(r["away"], 0)
        cnt[r["home"]] = cnt.get(r["home"], 0) + 1; cnt[r["away"]] = cnt.get(r["away"], 0) + 1
    ev = [r for r in rows if r["exp_h"] >= 10 and r["exp_a"] >= 10]
    tr = [r for r in ev if dt.date(2008, 1, 1) <= r["date"] <= dt.date(2018, 12, 31)]
    va = [r for r in ev if dt.date(2019, 1, 1) <= r["date"] <= dt.date(2022, 12, 31)]
    te = [r for r in ev if r["date"] >= dt.date(2023, 1, 1)]

    variants = []
    for quad in (False, True):
        for fr in (False, True):
            for gr in (False, True):
                if fr and gr:          # le groupe « amical » couvre déjà l'effet amical
                    continue
                th = fit_h1(tr, quad, fr, gr)
                for rho in (-0.10, -0.05, 0.0):
                    def lam(r, th=th, quad=quad, fr=fr):
                        x, home, f, _, _, g = design([r], quad, fr)
                        lh, la = h1_rates(th, x, home, f, quad, fr, g)
                        return float(lh[0]), float(la[0])
                    s = summarize(score(va, lam, rho))
                    variants.append({"quad": quad, "friendly": fr, "groupes": gr, "rho": rho, "theta": [round(v, 4) for v in th],
                                     **s, "objectif": s["logloss_1x2"] + s["logloss_ou25"]})
    variants.sort(key=lambda v: v["objectif"])
    # parcimonie : variante plus simple si le gain de validation est < 0,0005
    cx = lambda v: v["quad"] + v["friendly"] + 3 * v["groupes"]
    best = variants[0]
    for v in variants:
        if cx(v) < cx(best) and v["objectif"] - best["objectif"] < 0.0005:
            best = v
    Q, F, G, RHO = best["quad"], best["friendly"], best["groupes"], best["rho"]
    # ré-estimation sur apprentissage + validation (forme gelée), puis test final
    TH = fit_h1(tr + va, Q, F, G)

    def lam1(r):
        x, home, f, _, _, g = design([r], Q, F)
        lh, la = h1_rates(TH, x, home, f, Q, F, g)
        return float(lh[0]), float(la[0])
    nv = naive_rates(tr + va)
    recs1 = score(te, lam1, RHO)
    recs0 = score(te, lambda r: h0_lambdas(r["elo_h"], r["elo_a"], r["tournament"]), -0.05)
    recsn = score(te, lambda r: nv[r["neutral"]], 0.0)
    comp = {"H1 régression gelée": summarize(recs1), "H0 heuristique actuelle": summarize(recs0),
            "Référence simple (moyennes)": summarize(recsn)}

    def dll(A, Bb):
        return [-math.log(max(x["p"][x["k"]], 1e-12)) + math.log(max(y["p"][y["k"]], 1e-12)) for x, y in zip(A, Bb)]
    deltas = {"H1 − H0": [round(float(np.mean(dll(recs1, recs0))), 4), B.boot_ci(dll(recs1, recs0))],
              "H1 − simple": [round(float(np.mean(dll(recs1, recsn))), 4), B.boot_ci(dll(recs1, recsn))],
              "H0 − simple": [round(float(np.mean(dll(recs0, recsn))), 4), B.boot_ci(dll(recs0, recsn))]}
    by_kind = []
    for kd in ("amical", "qualification", "ligue des nations", "phase finale", "autre tournoi"):
        i = [j for j, x in enumerate(recs1) if kind(x["r"]["tournament"]) == kd]
        if i:
            d = dll([recs1[j] for j in i], [recs0[j] for j in i])
            by_kind.append({"type": kd, "n": len(i), "ll_H1": summarize([recs1[j] for j in i])["logloss_1x2"],
                            "ll_H0": summarize([recs0[j] for j in i])["logloss_1x2"], "delta": round(float(np.mean(d)), 4),
                            "IC95": B.boot_ci(d)})
    par_comp = []
    for name in ("African Cup of Nations qualification", "UEFA Nations League", "CONCACAF Nations League", "Friendly",
                 "FIFA World Cup qualification"):
        sel = [x for x in recs1 if x["r"]["tournament"] == name]
        if sel:
            par_comp.append({"comp": name, "n": len(sel), "buts_pred": round(float(np.mean([x["lh"] + x["la"] for x in sel])), 2),
                             "buts_obs": round(float(np.mean([x["r"]["hs"] + x["r"]["as"] for x in sel])), 2),
                             "o25_pred": round(float(np.mean([x["o"] for x in sel])), 3),
                             "o25_obs": round(float(np.mean([x["r"]["hs"] + x["r"]["as"] > 2 for x in sel])), 3)})
    calib = {"1X2 H1": B.calibration([(float(x["p"][j]), int(x["k"] == j)) for x in recs1 for j in range(3)]),
             "Over 2.5 H1": B.calibration([(x["o"], int(x["r"]["hs"] + x["r"]["as"] > 2)) for x in recs1]),
             "Over 2.5 H0": B.calibration([(x["o"], int(x["r"]["hs"] + x["r"]["as"] > 2)) for x in recs0])}
    lo, hi = deltas["H1 − H0"][1]
    status = ("H1 VALIDÉE contre H0 et la référence simple ; NON COMPARÉE AU MARCHÉ (pas de cotes historiques)"
              if hi < 0 and deltas["H1 − simple"][1][1] < 0 else "H1 NON SUPÉRIEURE à H0 — garder la prudence")
    run_id = f"intl-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    last = rows[-1]["date"].isoformat()
    rep = {"run_id": run_id, "version": VERSION, "source": SRC, "complement": SUPP.name if SUPP.exists() else None,
           "dernier_match": last, "n_matchs_total": len(rows),
           "periodes": {"apprentissage": "2008-2018", "validation": "2019-2022", "test_final": f"2023 → {last}"},
           "effectifs": {"apprentissage": len(tr), "validation": len(va), "test": len(te)},
           "variantes": variants, "retenue": {"quad": Q, "friendly": F, "groupes": G, "rho": RHO, "theta": TH, "groupes_ordre": GROUPS},
           "test_final": comp, "deltas_logloss_1x2": deltas, "par_type": by_kind, "par_competition": par_comp, "calibration": calib, "statut": status}
    out = BT / run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(rep, ensure_ascii=False, indent=1, default=str))
    (out / "REPORT.md").write_text(render(rep), encoding="utf-8")
    (BT / "intl_latest_params.json").write_text(json.dumps(
        {"run_id": run_id, "version": VERSION, "quad": Q, "friendly": F, "groupes": G, "rho": RHO, "theta": TH, "statut": status,
         "elo_au": last, "elo": {k: round(v, 1) for k, v in sorted(final.items())}}, ensure_ascii=False, indent=0))
    print(render(rep))


def render(R):
    L = [f"# Backtest international APEX-INTL — {R['run_id']}", "",
         f"- Source : {R['source']}" + (f" + complément `{R['complement']}` (API-Football)" if R["complement"] else ""),
         f"- {R['n_matchs_total']} matchs officiels jusqu'au {R['dernier_match']} ; Elo reconstruit match par match (avant-match uniquement)",
         f"- Apprentissage {R['periodes']['apprentissage']} ({R['effectifs']['apprentissage']}) · validation "
         f"{R['periodes']['validation']} ({R['effectifs']['validation']}) · **test final {R['periodes']['test_final']} "
         f"({R['effectifs']['test']})** ; équipes avec ≥ 10 matchs d'historique",
         f"- Variante retenue (validation) : {R['retenue']}",
         f"- **Statut : {R['statut']}**", "", "## Test final", "",
         "| Modèle | n | LL 1X2 | Brier | RPS | LL O/U 2,5 | LL BTTS | LL score | buts prédits | observés |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for k, s in R["test_final"].items():
        L.append(f"| {k} | {s['n']} | {s['logloss_1x2']} | {s['brier_1x2']} | {s['rps_1x2']} | {s['logloss_ou25']} | "
                 f"{s['logloss_btts']} | {s['logloss_score']} | {s['buts_predits']} | {s['buts_observes']} |")
    L += ["", "Δ log-loss 1X2 (négatif = premier meilleur), IC95 bootstrap par match :", ""]
    L += [f"- {k} : {v[0]:+.4f}  IC95 {v[1]}" for k, v in R["deltas_logloss_1x2"].items()]
    L += ["", "### Par type de match (H1 vs H0)", "", "| Type | n | LL H1 | LL H0 | Δ | IC95 |", "|---|---|---|---|---|---|"]
    L += [f"| {b['type']} | {b['n']} | {b['ll_H1']} | {b['ll_H0']} | {b['delta']:+.4f} | {b['IC95']} |" for b in R["par_type"]]
    for name, rows in R["calibration"].items():
        L += ["", f"### Calibration {name}", "", "| Tranche | n | Proba | Fréquence |", "|---|---|---|---|"]
        L += [f"| {r['tranche']} | {r['n']} | {r['p_moy']} | {r['freq_obs']} |" for r in rows]
    if R.get("par_competition"):
        L += ["", "### Buts par compétition (test final, H1 gelée)", "", "| Compétition | n | buts prédits | observés | O2,5 prédit | observé |",
              "|---|---|---|---|---|---|"]
        L += [f"| {c['comp']} | {c['n']} | {c['buts_pred']} | {c['buts_obs']} | {c['o25_pred']} | {c['o25_obs']} |" for c in R["par_competition"]]
    L += ["", "## Variantes essayées (validation)", "", "| quad | amical | groupes | rho | LL 1X2 | LL O/U | objectif |", "|---|---|---|---|---|---|---|"]
    L += [f"| {v['quad']} | {v['friendly']} | {v.get('groupes')} | {v['rho']} | {v['logloss_1x2']} | {v['logloss_ou25']} | {v['objectif']:.4f} |"
          for v in R["variantes"]]
    L += ["", "**Limite majeure** : aucune cote historique de sélections n'est disponible ; ce backtest ne dit rien de la "
          "précision face au marché. Toute sélection sur un match de sélections reste donc INDICATIVE."]
    return "\n".join(L) + "\n"


def lambdas_for(home, away, neutral=False, friendly=False, tournament=None):
    P = json.loads((BT / "intl_latest_params.json").read_text())
    h, a_ = canon(home), canon(away)
    missing = [t for t in (h, a_) if t not in P["elo"]]
    if missing:
        raise KeyError(f"Elo inconnu : {missing}")
    r = {"elo_h": P["elo"][h], "elo_a": P["elo"][a_], "neutral": neutral,
         "tournament": "Friendly" if friendly else (tournament or "x"), "hs": 0, "as": 0}
    x, hm, f, _, _, g = design([r], P["quad"], P["friendly"])
    lh, la = h1_rates(P["theta"], x, hm, f, P["quad"], P["friendly"], g)
    return float(lh[0]), float(la[0]), P


def cmd_lambdas(a):
    lh, la, P = lambdas_for(a.home, a.away, a.neutral, a.friendly, a.tournament)
    print(json.dumps({"lh": round(lh, 3), "la": round(la, 3), "rho": P["rho"], "elo_au": P["elo_au"], "statut": P["statut"]}))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("backtest"); b.add_argument("--refresh", action="store_true")
    l = sp.add_parser("lambdas"); l.add_argument("--home", required=True); l.add_argument("--away", required=True)
    l.add_argument("--neutral", action="store_true"); l.add_argument("--friendly", action="store_true")
    l.add_argument("--tournament", help="nom de compétition de la source (ex. UEFA Nations League)")
    a = p.parse_args()
    {"backtest": cmd_backtest, "lambdas": cmd_lambdas}[a.cmd](a)


if __name__ == "__main__":
    main()
