#!/usr/bin/env python3
"""APEX-BSM — module obligatoire de Backtesting, calibration et Simulation des Matchs.

Commandes
---------
  backtest   Backtest chronologique (walk-forward) sur l'historique football-data.co.uk :
             apprentissage / validation (réglage) / test final isolé, métriques, calibration,
             comparaison aux références, évaluation de paris à règles pré-enregistrées.
  simulate   Simulation Monte-Carlo d'un match (scénario partagé par les deux équipes),
             marchés dérivés des mêmes simulations, EV, sélection, enregistrement au journal.
  settle     Enregistre le résultat d'une prévision (journal append-only, rien n'est réécrit).
  audit      Confronte prévisions et résultats, abstentions comprises.

Principes (voir .claude/skills/apex-backtest-simulation/SKILL.md)
  * Aucune information postérieure à l'heure de prévision : les notes sont réestimées à chaque
    fenêtre avec les seuls matchs déjà joués ; les cotes utilisées sont celles relevées avant
    le match (colonnes PSH/PSD/PSA, P>2.5 de football-data : relevé du vendredi pour le week-end,
    du mardi pour le milieu de semaine), jamais les cotes de clôture.
  * Le test final n'est jamais utilisé pour régler un paramètre.
  * Aucune statistique n'est inventée : un match sans cote reste sans cote.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import functools
import hashlib
import io
import json
import math
import sys
import urllib.request
from pathlib import Path

import numpy as np
from scipy.stats import gamma as gamma_dist

MODEL_VERSION = "apex-bsm-1.0.0"
ROOT = Path(__file__).resolve().parent.parent
HIST_DIR = ROOT / "data" / "history"
BT_DIR = ROOT / "backtests"
LEDGER_DIR = ROOT / "ledger"
FD_URL = "https://www.football-data.co.uk/mmz4281/{season}/{div}.csv"
N_GOALS = 11

# Règles de pari fixées AVANT le backtest (ne pas modifier après avoir vu le test final).
BET_RULES = {
    "markets": ["1X2", "OU2.5"],
    "ev_min": 0.03,
    "one_market_per_match": True,
    "stake_units": 1.0,
    "price": "cote relevée avant match (Pinnacle PSH/PSD/PSA, P>2.5/P<2.5 ; à défaut moyenne marché)",
}

# Grille des variantes essayées sur la VALIDATION uniquement (toutes publiées dans le rapport).
# v1.0.0 : grille élargie après un essai E0 (validation 2425) dont l'optimum tombait en bordure.
GRID = {
    "xi": [0.001, 0.0015, 0.003, 0.005],  # décroissance temporelle par jour
    "K": [0.5, 1.0, 3.0, 6.0],            # rétrécissement vers la moyenne (unités de buts attendus)
    "rho": [-0.15, -0.10, -0.05, 0.0],    # correction Dixon-Coles des petits scores
    "sigma": [0.0, 0.10, 0.20, 0.30],     # écart-type du rythme partagé (0 = Poisson/DC pur)
}
LOOKBACK_DAYS = 730
MIN_GAIN = 0.0005   # gain minimal d'objectif (validation) pour adopter une variante plus complexe


# ───────────────────────────── données ─────────────────────────────

def _date(s: str) -> dt.date | None:
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return dt.datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


def _odds(r: dict, *cols: str):
    vals = []
    for c in cols:
        try:
            v = float(r.get(c, "") or "nan")
        except ValueError:
            return None
        if not v > 1.0:
            return None
        vals.append(v)
    return vals


def fetch_matches(div: str, season: str, refresh: bool = False) -> list[dict]:
    path = HIST_DIR / f"{div}_{season}.csv"
    if refresh or not path.exists():
        req = urllib.request.Request(FD_URL.format(season=season, div=div),
                                     headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=60).read()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    text = path.read_bytes().decode("utf-8-sig", errors="replace")
    out = []
    for r in csv.DictReader(io.StringIO(text)):
        d = _date(r.get("Date", "") or "")
        if not d or not r.get("HomeTeam") or (r.get("FTHG") or "") == "":
            continue
        try:
            hg, ag = int(float(r["FTHG"])), int(float(r["FTAG"]))
        except ValueError:
            continue
        o1x2, src = None, None
        for cols, name in ((("PSH", "PSD", "PSA"), "pinnacle"), (("AvgH", "AvgD", "AvgA"), "moyenne"),
                           (("B365H", "B365D", "B365A"), "bet365")):
            o1x2 = _odds(r, *cols)
            if o1x2:
                src = name
                break
        ou = _odds(r, "P>2.5", "P<2.5") or _odds(r, "Avg>2.5", "Avg<2.5") or _odds(r, "B365>2.5", "B365<2.5")
        out.append({
            "id": f"{div}|{d.isoformat()}|{r['HomeTeam']}|{r['AwayTeam']}",
            "div": div, "season": season, "date": d, "home": r["HomeTeam"].strip(),
            "away": r["AwayTeam"].strip(), "hg": hg, "ag": ag,
            "o1x2": o1x2, "o1x2_src": src, "ou25": ou,
            "close1x2": _odds(r, "PSCH", "PSCD", "PSCA") or _odds(r, "AvgCH", "AvgCD", "AvgCA"),
        })
    return out


# ───────────────────────────── modèles ─────────────────────────────

def fit_ratings(hist: list[dict], ref: dt.date, xi: float, K: float, iters: int = 40):
    """Poisson log-linéaire pondéré dans le temps, rétréci vers la moyenne (attaque/défense/terrain)."""
    hist = [m for m in hist if 0 < (ref - m["date"]).days <= LOOKBACK_DAYS]
    if len(hist) < 30:
        return None
    teams = sorted({m["home"] for m in hist} | {m["away"] for m in hist})
    ix = {t: i for i, t in enumerate(teams)}
    h = np.array([ix[m["home"]] for m in hist]); a = np.array([ix[m["away"]] for m in hist])
    hg = np.array([m["hg"] for m in hist], float); ag = np.array([m["ag"] for m in hist], float)
    w = np.exp(-xi * np.array([(ref - m["date"]).days for m in hist], float))
    n = len(teams)
    att = np.zeros(n); dfn = np.zeros(n)
    c = math.log(max((w * ag).sum() / w.sum(), 0.2))
    H = math.log(max((w * hg).sum(), 1e-6) / max((w * ag).sum(), 1e-6))
    for _ in range(iters):
        # attaque : buts marqués pondérés / exposition attendue (sans le terme d'attaque)
        eh = w * np.exp(c + H - dfn[a]); ea = w * np.exp(c - dfn[h])
        G = np.bincount(h, w * hg, n) + np.bincount(a, w * ag, n)
        E = np.bincount(h, eh, n) + np.bincount(a, ea, n)
        att = np.log((G + K) / (E + K))
        # défense : buts encaissés pondérés / exposition attendue (sans le terme de défense)
        fh = w * np.exp(c + H + att[h]); fa = w * np.exp(c + att[a])
        C = np.bincount(a, w * hg, n) + np.bincount(h, w * ag, n)
        F = np.bincount(a, fh, n) + np.bincount(h, fa, n)
        dfn = -np.log((C + K) / (F + K))
        att -= att.mean(); dfn -= dfn.mean()
        c = math.log((w * ag).sum() / (w * np.exp(att[a] - dfn[h])).sum())
        H = math.log((w * hg).sum() / (w * np.exp(att[h] - dfn[a])).sum()) - c
    return {"teams": {t: (float(att[i]), float(dfn[i])) for t, i in ix.items()},
            "c": c, "H": H, "n": len(hist)}


def lambdas(R: dict, home: str, away: str):
    ah, dh = R["teams"].get(home, (0.0, 0.0))
    aa, da = R["teams"].get(away, (0.0, 0.0))
    return math.exp(R["c"] + R["H"] + ah - da), math.exp(R["c"] + aa - dh)


_KS = np.arange(N_GOALS)
_FACT = np.array([math.factorial(k) for k in range(N_GOALS)], float)


def _pois(lam: float) -> np.ndarray:
    return np.exp(-lam) * lam ** _KS / _FACT


def dc_matrix(lh: float, la: float, rho: float) -> np.ndarray:
    M = np.outer(_pois(lh), _pois(la))
    M[0, 0] *= max(1 - lh * la * rho, 0); M[0, 1] *= max(1 + lh * rho, 0)
    M[1, 0] *= max(1 + la * rho, 0); M[1, 1] *= max(1 - rho, 0)
    return M / M.sum()


@functools.lru_cache(maxsize=None)
def pace_nodes(sigma: float, q: int = 15):
    """Rythme partagé g ~ Gamma(moyenne 1, écart-type sigma), discrétisé en q quantiles équiprobables."""
    if sigma <= 0:
        return np.array([1.0]), np.array([1.0])
    k = 1 / sigma ** 2
    g = gamma_dist.ppf((np.arange(q) + 0.5) / q, k, scale=1 / k)
    return g / g.mean(), np.full(q, 1 / q)


def score_matrix(lh: float, la: float, rho: float, sigma: float) -> np.ndarray:
    g, wt = pace_nodes(sigma)
    return sum(wi * dc_matrix(lh * gi, la * gi, rho) for gi, wi in zip(g, wt))


def probs_from_matrix(M: np.ndarray) -> dict:
    i, j = np.indices(M.shape)
    return {"1X2": np.array([M[i > j].sum(), M[i == j].sum(), M[i < j].sum()]),
            "O25": float(M[i + j > 2].sum()), "BTTS": float(M[(i > 0) & (j > 0)].sum())}


def legacy_probs(season_hist: list[dict], home: str, away: str) -> dict | None:
    """Réplique du skill actuel (analyses/tools/final.py) : saison en cours seule, K=4,5, ρ=-0,08."""
    G = season_hist
    if len(G) < 10:
        return None
    n = len(G); hgb = sum(m["hg"] for m in G); agb = sum(m["ag"] for m in G)
    T = n / (n + 35) * (hgb + agb) / n + (1 - n / (n + 35)) * 2.85
    share = n / (n + 70) * (hgb / max(hgb + agb, 1)) + (1 - n / (n + 70)) * 0.545
    HG, AG, L = T * share, T * (1 - share), T / 2

    def rat(t):
        gp = gf = ga = hgp = hgf = hga = agp = agf = aga = 0
        for m in G:
            if m["home"] == t:
                gp += 1; gf += m["hg"]; ga += m["ag"]; hgp += 1; hgf += m["hg"]; hga += m["ag"]
            elif m["away"] == t:
                gp += 1; gf += m["ag"]; ga += m["hg"]; agp += 1; agf += m["ag"]; aga += m["hg"]
        att = (gf + 4.5 * L) / ((gp + 4.5) * L); dfn = (ga + 4.5 * L) / ((gp + 4.5) * L)
        ath = (hgf + 4 * HG) / ((hgp + 4) * HG); dfh = (hga + 4 * AG) / ((hgp + 4) * AG)
        ata = (agf + 4 * AG) / ((agp + 4) * AG); dfa = (aga + 4 * HG) / ((agp + 4) * HG)
        return (att ** .75 * ath ** .25, dfn ** .75 * dfh ** .25, att ** .75 * ata ** .25, dfn ** .75 * dfa ** .25)
    rh, ra = rat(home), rat(away)
    return probs_from_matrix(dc_matrix(HG * rh[0] * ra[3], AG * ra[2] * rh[1], -0.08))


def naive_probs(hist: list[dict], ref: dt.date) -> dict | None:
    """Référence simple : buts moyens domicile/extérieur de la ligue sur 365 jours, Poisson indépendant."""
    H = [m for m in hist if 0 < (ref - m["date"]).days <= 365]
    if len(H) < 30:
        return None
    return probs_from_matrix(dc_matrix(np.mean([m["hg"] for m in H]), np.mean([m["ag"] for m in H]), 0.0))


def demargin(odds) -> np.ndarray:
    p = 1 / np.array(odds, float)
    return p / p.sum()


# ───────────────────────────── métriques ─────────────────────────────

def outcome_idx(m):
    return 0 if m["hg"] > m["ag"] else 1 if m["hg"] == m["ag"] else 2


def m_brier3(p, k):
    o = np.zeros(3); o[k] = 1
    return float(((p - o) ** 2).sum())


def m_rps(p, k):
    o = np.zeros(3); o[k] = 1
    return float(((np.cumsum(p)[:2] - np.cumsum(o)[:2]) ** 2).sum() / 2)


def m_ll(p, y):
    return -math.log(max(p, 1e-12)) if y else -math.log(max(1 - p, 1e-12))


def calibration(pairs, bins=10):
    """pairs = [(proba annoncée, 0/1)] → tableau fréquence observée par tranche."""
    rows = []
    for b in range(bins):
        lo, hi = b / bins, (b + 1) / bins
        sel = [(p, y) for p, y in pairs if lo <= p < hi or (b == bins - 1 and p == 1)]
        if sel:
            rows.append({"tranche": f"{lo:.1f}-{hi:.1f}", "n": len(sel),
                         "p_moy": round(float(np.mean([p for p, _ in sel])), 3),
                         "freq_obs": round(float(np.mean([y for _, y in sel])), 3)})
    return rows


def boot_ci(x, n=2000, seed=7):
    x = np.asarray(x, float)
    if len(x) < 5:
        return [float("nan")] * 2
    rng = np.random.default_rng(seed)
    means = x[rng.integers(0, len(x), (n, len(x)))].mean(1)
    return [round(float(np.quantile(means, .025)), 4), round(float(np.quantile(means, .975)), 4)]


# ───────────────────────────── backtest ─────────────────────────────

def week_key(d: dt.date):
    iso = d.isocalendar()
    return (iso[0], iso[1])


def walk_forward(all_m: list[dict], seasons_eval: list[str], xi: float, K: float, rhos, sigmas):
    """Fenêtres hebdomadaires : notes réestimées avec les seuls matchs antérieurs au lundi de la semaine.
    Retourne {(rho, sigma): [enregistrements]}; tous les marchés d'un match restent dans le même enregistrement."""
    out = {(r, s): [] for r in rhos for s in sigmas}
    tgt = [m for m in all_m if m["season"] in seasons_eval]
    weeks = sorted({week_key(m["date"]) for m in tgt})
    for wk in weeks:
        wm = [m for m in tgt if week_key(m["date"]) == wk]
        ref = min(m["date"] for m in wm)
        ref = ref - dt.timedelta(days=ref.weekday())          # lundi : aucune info de la semaine
        R = fit_ratings([m for m in all_m if m["date"] < ref], ref, xi, K)
        if R is None:
            continue
        for m in wm:
            lh, la = lambdas(R, m["home"], m["away"])
            for r in rhos:
                for s in sigmas:
                    M = score_matrix(lh, la, r, s)
                    P = probs_from_matrix(M)
                    out[(r, s)].append({"id": m["id"], "m": m, "lh": lh, "la": la, "P": P,
                                        "p_score": float(M[min(m["hg"], N_GOALS - 1), min(m["ag"], N_GOALS - 1)]),
                                        "known": m["home"] in R["teams"] and m["away"] in R["teams"]})
    return out


def score_records(recs: list[dict]) -> dict:
    if not recs:
        return {}
    ll1 = [-math.log(max(r["P"]["1X2"][outcome_idx(r["m"])], 1e-12)) for r in recs]
    llo = [m_ll(r["P"]["O25"], r["m"]["hg"] + r["m"]["ag"] > 2) for r in recs]
    return {"n": len(recs), "ll_1x2": float(np.mean(ll1)), "ll_ou25": float(np.mean(llo)),
            "ll_score": float(np.mean([-math.log(max(r["p_score"], 1e-12)) for r in recs])),
            "objectif": float(np.mean(ll1) + np.mean(llo))}


def cmd_backtest(a):
    divs = a.divs.split(","); seasons = a.seasons.split(",")
    train, val, test = seasons[:a.n_train], seasons[a.n_train:a.n_train + a.n_val], seasons[a.n_train + a.n_val:]
    if not test:
        sys.exit("Il faut au moins une saison de test final après apprentissage + validation.")
    data, coverage = {}, []
    for d in divs:
        ms = []
        for s in seasons:
            try:
                rows = fetch_matches(d, s, a.refresh)
            except Exception as e:  # noqa: BLE001 — on documente la donnée manquante, on n'invente rien
                coverage.append({"div": d, "season": s, "matchs": 0, "erreur": str(e)[:80]})
                continue
            coverage.append({"div": d, "season": s, "matchs": len(rows),
                             "cotes_1x2_avant_match": sum(1 for r in rows if r["o1x2"]),
                             "source_1x2": sorted({r["o1x2_src"] for r in rows if r["o1x2_src"]}),
                             "cotes_ou25": sum(1 for r in rows if r["ou25"])})
            ms += rows
        data[d] = sorted(ms, key=lambda m: m["date"])

    # 1) réglage sur la VALIDATION uniquement
    variants = []
    for xi in GRID["xi"]:
        for K in GRID["K"]:
            pooled = {(r, s): [] for r in GRID["rho"] for s in GRID["sigma"]}
            for d in divs:
                res = walk_forward(data[d], val, xi, K, GRID["rho"], GRID["sigma"])
                for k, v in res.items():
                    pooled[k] += v
            for (r, s), recs in pooled.items():
                variants.append({"xi": xi, "K": K, "rho": r, "sigma": s, **score_records(recs)})
    variants.sort(key=lambda v: v["objectif"])
    # Le rythme partagé (plus complexe) n'est retenu que s'il améliore l'objectif d'au moins MIN_GAIN.
    best = variants[0]
    simple = min((v for v in variants if v["sigma"] == 0), key=lambda v: v["objectif"])
    if best["sigma"] > 0 and simple["objectif"] - best["objectif"] < MIN_GAIN:
        best = simple
    base_sigma0 = min((v for v in variants if v["sigma"] == 0), key=lambda v: v["objectif"])

    # 2) TEST FINAL : paramètres gelés, jamais re-réglés
    P = {"xi": best["xi"], "K": best["K"], "rho": best["rho"], "sigma": best["sigma"]}
    recs = []
    for d in divs:
        recs += walk_forward(data[d], test, P["xi"], P["K"], [P["rho"]], [P["sigma"]])[(P["rho"], P["sigma"])]
    rows = []
    for r in recs:
        m = r["m"]; k = outcome_idx(m); tot = m["hg"] + m["ag"]
        season_hist = [x for x in data[m["div"]] if x["season"] == m["season"] and x["date"] < m["date"] - dt.timedelta(days=m["date"].weekday())]
        prior = [x for x in data[m["div"]] if x["date"] < m["date"] - dt.timedelta(days=m["date"].weekday())]
        leg = legacy_probs(season_hist, m["home"], m["away"])
        nai = naive_probs(prior, m["date"] - dt.timedelta(days=m["date"].weekday()))
        mk = demargin(m["o1x2"]) if m["o1x2"] else None
        mo = demargin(m["ou25"])[0] if m["ou25"] else None
        rows.append({"r": r, "k": k, "over": tot > 2, "btts": m["hg"] > 0 and m["ag"] > 0,
                     "leg": leg, "nai": nai, "mk": mk, "mo": mo})

    def block(sel, name, g1, go):
        s = [x for x in sel if g1(x) is not None]
        if not s:
            return None
        p1 = [g1(x) for x in s]
        d = {"modele": name, "n": len(s),
             "brier_1x2": round(float(np.mean([m_brier3(p, x["k"]) for p, x in zip(p1, s)])), 4),
             "logloss_1x2": round(float(np.mean([-math.log(max(p[x["k"]], 1e-12)) for p, x in zip(p1, s)])), 4),
             "rps_1x2": round(float(np.mean([m_rps(p, x["k"]) for p, x in zip(p1, s)])), 4)}
        so = [x for x in s if go(x) is not None]
        if so:
            d["n_ou"] = len(so)
            d["brier_ou25"] = round(float(np.mean([(go(x) - x["over"]) ** 2 for x in so])), 4)
            d["logloss_ou25"] = round(float(np.mean([m_ll(go(x), x["over"]) for x in so])), 4)
        return d

    # Comparaisons sur échantillon COMMUN (tous les modèles disponibles au même instant)
    common = [x for x in rows if x["mk"] is not None and x["leg"] is not None and x["nai"] is not None]
    comp = [b for b in (
        block(common, "APEX-BSM (gelé)", lambda x: x["r"]["P"]["1X2"], lambda x: x["r"]["P"]["O25"] if x["mo"] is not None else None),
        block(common, "Skill actuel (legacy)", lambda x: x["leg"]["1X2"], lambda x: x["leg"]["O25"] if x["mo"] is not None else None),
        block(common, "Modèle simple de buts", lambda x: x["nai"]["1X2"], lambda x: x["nai"]["O25"] if x["mo"] is not None else None),
        block(common, "Marché démarginé (avant match)", lambda x: x["mk"], lambda x: x["mo"]),
    ) if b]

    def diff_ll(g):
        return [-math.log(max(x["r"]["P"]["1X2"][x["k"]], 1e-12)) + math.log(max(g(x)[x["k"]], 1e-12)) for x in common]
    deltas = {
        "vs_marche": boot_ci(diff_ll(lambda x: x["mk"])), "vs_legacy": boot_ci(diff_ll(lambda x: x["leg"]["1X2"])),
        "vs_simple": boot_ci(diff_ll(lambda x: x["nai"]["1X2"])),
    }
    mean_delta = {k: round(float(np.mean(diff_ll(g))), 4) for k, g in
                  (("vs_marche", lambda x: x["mk"]), ("vs_legacy", lambda x: x["leg"]["1X2"]), ("vs_simple", lambda x: x["nai"]["1X2"]))}

    calib = {"1X2 (3 issues regroupées)": calibration([(float(x["r"]["P"]["1X2"][j]), int(x["k"] == j)) for x in rows for j in range(3)]),
             "Over 2.5": calibration([(x["r"]["P"]["O25"], int(x["over"])) for x in rows]),
             "BTTS": calibration([(x["r"]["P"]["BTTS"], int(x["btts"])) for x in rows])}
    btts = {"n": len(rows), "brier": round(float(np.mean([(x["r"]["P"]["BTTS"] - x["btts"]) ** 2 for x in rows])), 4),
            "logloss": round(float(np.mean([m_ll(x["r"]["P"]["BTTS"], x["btts"]) for x in rows])), 4),
            "note": "aucune cote BTTS historique dans la source : pas de comparaison marché"}
    goals = {"buts_moy_predits": round(float(np.mean([x["r"]["lh"] + x["r"]["la"] for x in rows])), 3),
             "buts_moy_observes": round(float(np.mean([x["r"]["m"]["hg"] + x["r"]["m"]["ag"] for x in rows])), 3),
             "logloss_score_exact": round(float(np.mean([-math.log(max(x["r"]["p_score"], 1e-12)) for x in rows])), 4)}
    stab = []
    for d in divs:
        sub = [x for x in common if x["r"]["m"]["div"] == d]
        if sub:
            dl = [-math.log(max(x["r"]["P"]["1X2"][x["k"]], 1e-12)) + math.log(max(x["mk"][x["k"]], 1e-12)) for x in sub]
            stab.append({"div": d, "n": len(sub), "delta_ll_vs_marche": round(float(np.mean(dl)), 4), "IC95": boot_ci(dl)})

    # 3) paris : règles fixées à l'avance, cotes avant match, 1 marché max par match
    bets = []
    for x in sorted(rows, key=lambda x: x["r"]["m"]["date"]):
        m = x["r"]["m"]; cands = []
        if m["o1x2"]:
            for j, lab in enumerate(("1", "X", "2")):
                cands.append(("1X2", lab, float(x["r"]["P"]["1X2"][j]), m["o1x2"][j], x["k"] == j))
        if m["ou25"]:
            cands.append(("OU2.5", "Over", x["r"]["P"]["O25"], m["ou25"][0], x["over"]))
            cands.append(("OU2.5", "Under", 1 - x["r"]["P"]["O25"], m["ou25"][1], not x["over"]))
        cands = [c for c in cands if c[2] * c[3] - 1 >= BET_RULES["ev_min"]]
        if cands:
            mkt, sel, p, o, won = max(cands, key=lambda c: c[2] * c[3] - 1)
            clv = None
            if mkt == "1X2" and m["close1x2"]:
                clv = o * demargin(m["close1x2"])[("1", "X", "2").index(sel)] - 1
            bets.append({"id": m["id"], "div": m["div"], "date": m["date"].isoformat(), "marche": mkt, "sel": sel,
                         "p": round(p, 4), "cote": o, "ev": round(p * o - 1, 4), "gagne": bool(won),
                         "pnl": round(o - 1 if won else -1.0, 4), "clv": clv})
    pnl = np.array([b["pnl"] for b in bets]) if bets else np.array([])
    cum = np.cumsum(pnl) if len(pnl) else np.array([0.0])
    betting = {"regles": BET_RULES, "n_paris": len(bets),
               "taux_reussite": round(float(np.mean([b["gagne"] for b in bets])), 4) if bets else None,
               "rendement_net_par_unite": round(float(pnl.mean()), 4) if bets else None,
               "IC95_rendement": boot_ci(pnl) if bets else None,
               "pnl_total_unites": round(float(pnl.sum()), 2) if bets else 0,
               "perte_max_cumulee_unites": round(float((cum - np.maximum.accumulate(np.concatenate([[0], cum]))[1:]).min()), 2),
               "clv_moyen_1x2": round(float(np.mean([b["clv"] for b in bets if b["clv"] is not None])), 4)
               if any(b["clv"] is not None for b in bets) else None,
               "par_marche": {}}
    for mk_ in ("1X2", "OU2.5"):
        sb = [b for b in bets if b["marche"] == mk_]
        if sb:
            betting["par_marche"][mk_] = {"n": len(sb), "rendement": round(float(np.mean([b["pnl"] for b in sb])), 4),
                                         "reussite": round(float(np.mean([b["gagne"] for b in sb])), 4)}

    beats_market = deltas["vs_marche"][1] < 0
    worse_market = deltas["vs_marche"][0] > 0
    status = ("VALIDÉ (meilleur que le marché hors échantillon)" if beats_market else
              "NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire" if worse_market else
              "NON CONCLUANT — pas de différence significative avec le marché")
    run_id = f"bsm-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    report = {"run_id": run_id, "model_version": MODEL_VERSION, "heure_prevision": "lundi précédant la semaine du match (00:00), cotes relevées avant match",
              "ligues": divs, "saisons": {"apprentissage": train, "validation": val, "test_final": test},
              "couverture": coverage, "variantes_essayees": variants, "parametres_geles": P,
              "apport_rythme_partage": {"meilleur_sigma0_objectif": round(base_sigma0["objectif"], 4),
                                        "retenu_objectif": round(best["objectif"], 4)},
              "test_final": {"comparaison_echantillon_commun": comp, "delta_logloss_1x2_moyen": mean_delta,
                             "IC95_delta_logloss_1x2": deltas, "stabilite_par_ligue": stab,
                             "btts": btts, "buts": goals, "calibration": calib, "paris": betting},
              "statut_validation": status}
    out = BT_DIR / run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str))
    (out / "bets.json").write_text(json.dumps(bets, ensure_ascii=False, indent=0))
    (out / "REPORT.md").write_text(render_report(report), encoding="utf-8")
    params = {"run_id": run_id, "model_version": MODEL_VERSION, **P, "divs": divs,
              "statut_validation": status, "stabilite_par_ligue": stab}
    (BT_DIR / "latest_params.json").write_text(json.dumps(params, ensure_ascii=False, indent=1))
    print(render_report(report))


def render_report(R: dict) -> str:
    T = R["test_final"]; L = []
    L += [f"# Backtest APEX-BSM — {R['run_id']}", "",
          f"- Version du modèle : `{R['model_version']}`",
          f"- Heure de prévision simulée : {R['heure_prevision']}",
          f"- Ligues : {', '.join(R['ligues'])}",
          f"- Apprentissage (historique seul) : {', '.join(R['saisons']['apprentissage'])} · "
          f"Validation (réglage) : {', '.join(R['saisons']['validation'])} · "
          f"**Test final isolé : {', '.join(R['saisons']['test_final'])}**",
          f"- Paramètres gelés après validation : {R['parametres_geles']}",
          f"- **Statut : {R['statut_validation']}**", "", "## Couverture des données", "",
          "| Ligue | Saison | Matchs | Cotes 1X2 avant match | Source | Cotes O/U 2,5 |", "|---|---|---|---|---|---|"]
    for c in R["couverture"]:
        L.append(f"| {c['div']} | {c['season']} | {c['matchs']} | {c.get('cotes_1x2_avant_match', '—')} | "
                 f"{', '.join(c.get('source_1x2', [])) or c.get('erreur', '—')} | {c.get('cotes_ou25', '—')} |")
    L += ["", "## Test final — comparaison aux références (même échantillon, même instant)", "",
          "| Modèle | n | Brier 1X2 | Log-loss 1X2 | RPS | n O/U | Brier O/U 2,5 | Log-loss O/U 2,5 |", "|---|---|---|---|---|---|---|---|"]
    for b in T["comparaison_echantillon_commun"]:
        L.append(f"| {b['modele']} | {b['n']} | {b['brier_1x2']} | {b['logloss_1x2']} | {b['rps_1x2']} | "
                 f"{b.get('n_ou', '—')} | {b.get('brier_ou25', '—')} | {b.get('logloss_ou25', '—')} |")
    L += ["", "Écart de log-loss 1X2 (APEX-BSM − référence ; négatif = APEX-BSM meilleur), IC 95 % bootstrap par match :", ""]
    for k, v in T["delta_logloss_1x2_moyen"].items():
        L.append(f"- {k} : {v:+.4f}  IC95 {T['IC95_delta_logloss_1x2'][k]}")
    L += ["", "### Stabilité par ligue (log-loss 1X2 vs marché)", "", "| Ligue | n | Δ | IC95 |", "|---|---|---|---|"]
    L += [f"| {s['div']} | {s['n']} | {s['delta_ll_vs_marche']:+.4f} | {s['IC95']} |" for s in T["stabilite_par_ligue"]]
    L += ["", f"### BTTS : Brier {T['btts']['brier']}, log-loss {T['btts']['logloss']} (n={T['btts']['n']}) — {T['btts']['note']}",
          "", f"### Buts : prédits {T['buts']['buts_moy_predits']} / observés {T['buts']['buts_moy_observes']} par match ; "
          f"log-loss du score exact {T['buts']['logloss_score_exact']}", "", "### Calibration (test final)", ""]
    for name, rows in T["calibration"].items():
        L += [f"**{name}**", "", "| Tranche | n | Proba moyenne | Fréquence observée |", "|---|---|---|---|"]
        L += [f"| {r['tranche']} | {r['n']} | {r['p_moy']} | {r['freq_obs']} |" for r in rows] + [""]
    P = T["paris"]
    L += ["### Paris simulés (règles fixées avant le test)", "",
          f"Règles : {P['regles']}", "",
          f"- Paris : {P['n_paris']} · taux de réussite : {P['taux_reussite']} · rendement net/unité : {P['rendement_net_par_unite']} "
          f"(IC95 {P['IC95_rendement']}) · P&L : {P['pnl_total_unites']} u · perte max cumulée : {P['perte_max_cumulee_unites']} u · "
          f"CLV moyen 1X2 : {P['clv_moyen_1x2']}",
          f"- Par marché : {P['par_marche']}", "",
          "*Qualité prédictive (log-loss), taux de réussite et rentabilité sont trois choses distinctes. "
          "Ces paris sont simulés : aucune mise n'a été exécutée.*", "",
          "## Variantes essayées (validation uniquement)", "",
          f"{len(R['variantes_essayees'])} variantes. Les 10 meilleures :", "",
          "| xi | K | rho | sigma | n | LL 1X2 | LL O/U | LL score | objectif |", "|---|---|---|---|---|---|---|---|---|"]
    for v in R["variantes_essayees"][:10]:
        L.append(f"| {v['xi']} | {v['K']} | {v['rho']} | {v['sigma']} | {v['n']} | {v['ll_1x2']:.4f} | {v['ll_ou25']:.4f} | {v['ll_score']:.4f} | {v['objectif']:.4f} |")
    a = R["apport_rythme_partage"]
    L += ["", f"Apport du rythme partagé (validation) : meilleur objectif sans rythme {a['meilleur_sigma0_objectif']} · retenu {a['retenu_objectif']}.",
          f"Le rythme partagé n'est adopté que s'il améliore l'objectif de validation d'au moins {MIN_GAIN} ; sinon le modèle simple (σ=0) est gelé."]
    return "\n".join(L) + "\n"


# ───────────────────────────── simulation d'un match ─────────────────────────────

def simulate_scores(lh, la, rho, sigma, scenarios, n, rng):
    """Chaque simulation tire UN scénario commun (composition, rythme) appliqué aux deux équipes,
    puis un score dans la loi Dixon-Coles correspondante."""
    g, gw = pace_nodes(sigma)
    cells, w = [], []
    for sp, mh, ma in scenarios:
        for gi, wi in zip(g, gw):
            cells.append((lh * mh * gi, la * ma * gi)); w.append(sp * wi)
    w = np.array(w) / sum(w)
    counts = rng.multinomial(n, w)
    hs, as_ = [], []
    for (l1, l2), c in zip(cells, counts):
        if c == 0:
            continue
        M = dc_matrix(l1, l2, rho).ravel()
        k = rng.choice(M.size, size=c, p=M / M.sum())
        hs.append(k // N_GOALS); as_.append(k % N_GOALS)
    return np.concatenate(hs), np.concatenate(as_)


def settle_states(margin: np.ndarray, line: float) -> dict:
    """États de règlement d'un pari « margin + line > 0 » (handicap asiatique / total asiatique).
    Les lignes au quart sont traitées comme deux demi-mises aux lignes voisines."""
    def one(l):
        v = margin + l
        return (v > 0).astype(float), (v == 0).astype(float), (v < 0).astype(float)
    frac = round(line * 4) % 2
    if frac == 1:  # ligne au quart
        w1, p1, l1 = one(line - 0.25); w2, p2, l2 = one(line + 0.25)
        W = w1 * w2; L = l1 * l2
        HW = (w1 * p2 + p1 * w2); HL = (l1 * p2 + p1 * l2)
        return {"gain": float(W.mean()), "demi_gain": float(HW.mean()), "rembourse": float((p1 * p2).mean()),
                "demi_perte": float(HL.mean()), "perte": float(L.mean())}
    w, p, l = one(line)
    return {"gain": float(w.mean()), "demi_gain": 0.0, "rembourse": float(p.mean()), "demi_perte": 0.0, "perte": float(l.mean())}


def ev_states(s: dict, odds: float) -> float:
    return s["gain"] * (odds - 1) + s["demi_gain"] * (odds - 1) / 2 - s["demi_perte"] / 2 - s["perte"]


def markets_from_sims(hg: np.ndarray, ag: np.ndarray) -> dict:
    n = len(hg); tot = hg + ag; mg = hg - ag
    M = {"1": float((mg > 0).mean()), "X": float((mg == 0).mean()), "2": float((mg < 0).mean())}
    M.update({"1X": M["1"] + M["X"], "X2": M["X"] + M["2"], "12": M["1"] + M["2"],
              "BTTS_oui": float(((hg > 0) & (ag > 0)).mean())})
    M["BTTS_non"] = 1 - M["BTTS_oui"]
    M["DNB_1"] = settle_states(mg.astype(float), 0.0)
    M["DNB_2"] = settle_states(-mg.astype(float), 0.0)
    for L in (0.5, 1.5, 2.5, 3.5, 4.5, 5.5):
        M[f"Over{L}"] = float((tot > L).mean()); M[f"Under{L}"] = 1 - M[f"Over{L}"]
    for L in (1.75, 2.0, 2.25, 2.75, 3.0, 3.25):
        M[f"Over{L}_asiat"] = settle_states(tot.astype(float), -L)
        M[f"Under{L}_asiat"] = settle_states(-tot.astype(float), L)
    for side, x in (("dom", hg), ("ext", ag)):
        for L in (0.5, 1.5, 2.5):
            M[f"{side}_Over{L}"] = float((x > L).mean())
    for q in range(-12, 13):
        L = q / 4
        M[f"AH_dom{L:+.2f}"] = settle_states(mg.astype(float), L)
        M[f"AH_ext{L:+.2f}"] = settle_states(-mg.astype(float), L)
    sc = {}
    for i, j in zip(hg, ag):
        sc[(int(i), int(j))] = sc.get((int(i), int(j)), 0) + 1
    M["scores"] = [[f"{i}-{j}", round(c / n, 4)] for (i, j), c in sorted(sc.items(), key=lambda kv: -kv[1])[:10]]
    # contrôles de cohérence
    assert abs(M["1"] + M["X"] + M["2"] - 1) < 1e-9
    for a_, b_ in ((0.5, 1.5), (1.5, 2.5), (2.5, 3.5), (3.5, 4.5)):
        assert M[f"Over{a_}"] >= M[f"Over{b_}"]
    return M


def parse_odds_arg(s: str | None):
    return [float(x) for x in s.split(",")] if s else None


def cmd_simulate(a):
    rng = np.random.default_rng(a.seed)
    params_path = BT_DIR / "latest_params.json"
    params = json.loads(params_path.read_text()) if params_path.exists() else None
    notes = []
    if a.lh and a.la:
        lh, la = a.lh, a.la
        rho = a.rho if a.rho is not None else (params["rho"] if params else -0.05)
        sigma = params["sigma"] if params else 0.0
        source = a.lambda_source or "λ fournis directement (hors périmètre du backtest : non validé)"
        status = a.status_note or "NON VALIDÉ — λ externes"
    else:
        if not params:
            sys.exit("Aucun backtest disponible : lancer d'abord `backtest`, ou fournir --lh/--la (statut NON VALIDÉ).")
        asof = dt.date.fromisoformat(a.asof) if a.asof else dt.date.today()
        hist = []
        for s in a.seasons.split(","):
            try:
                hist += fetch_matches(a.div, s, a.refresh)
            except Exception as e:  # noqa: BLE001
                notes.append(f"saison {s} indisponible : {e}")
        R = fit_ratings(sorted(hist, key=lambda m: m["date"]), asof, params["xi"], params["K"])
        if R is None:
            sys.exit("Historique insuffisant pour estimer les forces.")
        for t in (a.home, a.away):
            if t not in R["teams"]:
                sys.exit(f"Équipe inconnue dans l'historique : {t}. Noms disponibles : {sorted(R['teams'])}")
        lh, la = lambdas(R, a.home, a.away); rho, sigma = params["rho"], params["sigma"]
        stab = {s["div"]: s for s in params.get("stabilite_par_ligue", [])}.get(a.div)
        source = f"forces estimées au {asof} sur {R['n']} matchs antérieurs (backtest {params['run_id']})"
        status = params["statut_validation"] + (f" · {a.div} : Δ={stab['delta_ll_vs_marche']:+.4f} IC95 {stab['IC95']}" if stab else " · ligue hors backtest")
    scen = [(1.0, 1.0, 1.0)]
    if a.scenario:
        scen = [tuple(float(v) for v in s.split(":")) for s in a.scenario]
        rest = 1 - sum(s[0] for s in scen)
        if rest < -1e-9:
            sys.exit("La somme des probabilités de scénarios dépasse 1.")
        if rest > 1e-9:
            scen.append((rest, 1.0, 1.0))
        notes.append("Scénarios de composition = correction SUBJECTIVE, testée en sensibilité ci-dessous.")

    # précision numérique fixée à l'avance : demi-largeur IC95 MC ≤ tol sur tous les marchés binaires clés
    n = a.n0
    while True:
        hg, ag = simulate_scores(lh, la, rho, sigma, scen, n, rng)
        M = markets_from_sims(hg, ag)
        keys = ["1", "X", "2", "Over1.5", "Over2.5", "Over3.5", "BTTS_oui"]
        half = max(1.96 * math.sqrt(M[k] * (1 - M[k]) / n) for k in keys)
        if half <= a.tol or n >= a.nmax:
            break
        n *= 2
    exact = probs_from_matrix(score_matrix(lh, la, rho, sigma)) if len(scen) == 1 else None

    # sensibilité (≠ intervalle de confiance) : λ ±10 %, sans scénarios subjectifs
    sens = {}
    for lab, fh, fa in (("λ dom −10 %", .9, 1), ("λ dom +10 %", 1.1, 1), ("λ ext −10 %", 1, .9), ("λ ext +10 %", 1, 1.1),
                        ("sans scénarios subjectifs", None, None)):
        if fh is None:
            if len(scen) == 1:
                continue
            h2, a2 = simulate_scores(lh, la, rho, sigma, [(1, 1, 1)], n, rng)
        else:
            h2, a2 = simulate_scores(lh * fh, la * fa, rho, sigma, scen, n, rng)
        m2 = markets_from_sims(h2, a2)
        sens[lab] = {k: round(m2[k], 3) for k in ("1", "X", "2", "Over2.5", "BTTS_oui")}

    offers = {}
    for lab, vals in (("1X2", parse_odds_arg(a.odds_1x2)), ("OU2.5", parse_odds_arg(a.odds_ou25)),
                      ("BTTS", parse_odds_arg(a.odds_btts))):
        if vals:
            offers[lab] = vals
    evs = []
    if "1X2" in offers:
        for k, o in zip(("1", "X", "2"), offers["1X2"]):
            evs.append({"marche": f"1X2 {k}", "p": M[k], "cote": o, "ev": M[k] * o - 1, "sens": [s[k] * o - 1 for s in sens.values()]})
    if "OU2.5" in offers:
        evs.append({"marche": "Over 2.5", "p": M["Over2.5"], "cote": offers["OU2.5"][0], "ev": M["Over2.5"] * offers["OU2.5"][0] - 1,
                    "sens": [s["Over2.5"] * offers["OU2.5"][0] - 1 for s in sens.values()]})
        evs.append({"marche": "Under 2.5", "p": M["Under2.5"], "cote": offers["OU2.5"][1], "ev": M["Under2.5"] * offers["OU2.5"][1] - 1,
                    "sens": [(1 - s["Over2.5"]) * offers["OU2.5"][1] - 1 for s in sens.values()]})
    if "BTTS" in offers:
        evs.append({"marche": "BTTS oui", "p": M["BTTS_oui"], "cote": offers["BTTS"][0], "ev": M["BTTS_oui"] * offers["BTTS"][0] - 1,
                    "sens": [s["BTTS_oui"] * offers["BTTS"][0] - 1 for s in sens.values()]})
        evs.append({"marche": "BTTS non", "p": M["BTTS_non"], "cote": offers["BTTS"][1], "ev": M["BTTS_non"] * offers["BTTS"][1] - 1,
                    "sens": [(1 - s["BTTS_oui"]) * offers["BTTS"][1] - 1 for s in sens.values()]})
    for spec in a.odds_ah or []:
        side, line, o = spec.split(":"); line = float(line) + 0.0; o = float(o)
        st = M[f"AH_{side}{line:+.2f}"]
        evs.append({"marche": f"AH {side} {line:+.2f}", "p": st["gain"] + st["demi_gain"], "cote": o, "ev": ev_states(st, o),
                    "etats": {k: round(v, 4) for k, v in st.items()}, "sens": []})
    for spec in a.odds_dnb or []:
        side, o = spec.split(":"); o = float(o); st = M[f"DNB_{side}"]
        evs.append({"marche": f"DNB {side}", "p": st["gain"], "cote": o, "ev": st["gain"] * (o - 1) - st["perte"],
                    "etats": {k: round(v, 4) for k, v in st.items()}, "sens": []})

    # sélection : au plus UN marché officiel ; veto si modèle non supérieur au marché ou EV instable
    veto = []
    if "NON SUPÉRIEUR" in status:
        veto.append("backtest : modèle significativement moins précis que le marché → EV présumée illusoire")
    if a.missing_lineup:
        veto.append("composition déterminante manquante")
    # écart suspect : > 10 points de probabilité implicite avec le marché démarginé → erreur de modèle probable.
    # Marchés à règlement partiel (AH) comparés via leurs cotes justes (EV = 0).
    def fair_prob(e):
        st = e.get("etats")
        if not st:
            return e["p"]
        den = st["gain"] + st["demi_gain"] / 2
        return den / (den + st["demi_perte"] / 2 + st["perte"]) if den > 0 else 0.0
    mkt = {}
    if "1X2" in offers:
        mkt.update(zip(("1X2 1", "1X2 X", "1X2 2"), demargin(offers["1X2"])))
    if "OU2.5" in offers:
        mkt.update(zip(("Over 2.5", "Under 2.5"), demargin(offers["OU2.5"])))
    if "BTTS" in offers:
        mkt.update(zip(("BTTS oui", "BTTS non"), demargin(offers["BTTS"])))
    ah = {e["marche"]: e["cote"] for e in evs if e["marche"].startswith("AH ")}
    for name, o in ah.items():
        _, side, line = name.split()
        opp = f"AH {'ext' if side == 'dom' else 'dom'} {-float(line) + 0.0:+.2f}"
        if opp in ah:
            mkt[name] = demargin([o, ah[opp]])[0]
    if not status.startswith("VALIDÉ"):
        for e in evs:
            if e["marche"] in mkt:
                e["ecart_marche"] = fair_prob(e) - mkt[e["marche"]]
                if abs(e["ecart_marche"]) > 0.10:
                    e["suspect"] = True
    ok = [e for e in evs if e["ev"] >= 0.03 and (not e["sens"] or min(e["sens"]) >= 0) and not e.get("suspect")]
    if any(e.get("suspect") and e["ev"] >= 0.03 for e in evs):
        notes.append("EV ≥ 3 % écartée : écart > 10 pts avec le marché sur un modèle non validé (erreur de modèle probable).")
    fragile = [e for e in evs if e["ev"] >= 0.03 and e["sens"] and min(e["sens"]) < 0]
    if not offers and not a.odds_ah and not a.odds_dnb:
        decision = "SANS COTE VÉRIFIÉE — aucune sélection possible"
        official = None
    elif veto:
        official = None; decision = "SURVEILLANCE / ABSTENTION — " + " ; ".join(veto)
    elif ok:
        official = max(ok, key=lambda e: e["ev"])
        tag = "SÉLECTION" if status.startswith("VALIDÉ") or status.startswith("NON CONCLUANT") else "SÉLECTION INDICATIVE (modèle non validé, mise ≤ ½)"
        decision = f"{tag} : {official['marche']} @ {official['cote']}"
    else:
        official = None
        decision = "ABSTENTION — aucune EV ≥ 3 % stable" + (" (EV qui disparaît en sensibilité : surveillance)" if fragile else "")

    fid = hashlib.sha1(f"{a.home}|{a.away}|{a.kickoff}|{dt.datetime.now(dt.timezone.utc).isoformat()}".encode()).hexdigest()[:12]
    rec = {"forecast_id": fid, "cree_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
           "coup_envoi": a.kickoff, "div": a.div, "home": a.home, "away": a.away, "model_version": MODEL_VERSION,
           "source_lambdas": source, "statut_modele": status, "lh": round(lh, 3), "la": round(la, 3), "rho": rho, "sigma": sigma,
           "scenarios": scen, "n_simulations": n, "demi_largeur_IC95_MC": round(half, 4),
           "marches": {k: (round(v, 4) if isinstance(v, float) else v) for k, v in M.items()
                       if k in ("1", "X", "2", "1X", "X2", "12", "BTTS_oui", "Over1.5", "Over2.5", "Over3.5", "scores")},
           "cotes": offers, "cotes_source": a.odds_source, "cotes_relevees_utc": a.odds_time,
           "ev": [{k: (round(v, 4) if isinstance(v, float) else v) for k, v in e.items() if k != "sens"} for e in evs],
           "sensibilite": sens, "decision": decision, "statut_mise": "PROPOSÉE" if official else "AUCUNE", "notes": notes}
    if a.record:
        LEDGER_DIR.mkdir(exist_ok=True)
        with open(LEDGER_DIR / "forecasts.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    # affichage
    print(f"# {a.home} – {a.away}  ({a.div}, {a.kickoff or 'coup d envoi non renseigné'})")
    print(f"Modèle {MODEL_VERSION} · {source}\nStatut : {status}")
    print(f"Buts attendus : {a.home} {lh:.2f} · {a.away} {la:.2f}  (moyenne, ≠ score le plus probable)")
    print(f"Simulations exécutées : {n:,} (demi-largeur IC95 Monte-Carlo max {half:.4f}) · ρ={rho} · rythme σ={sigma}")
    print(f"1X2 : {M['1']:.1%} / {M['X']:.1%} / {M['2']:.1%}" + (f"   (exact : {exact['1X2'][0]:.1%} / {exact['1X2'][1]:.1%} / {exact['1X2'][2]:.1%})" if exact else ""))
    print(f"DC 1X {M['1X']:.1%} · X2 {M['X2']:.1%} · 12 {M['12']:.1%} · DNB dom gain {M['DNB_1']['gain']:.1%} remb. {M['DNB_1']['rembourse']:.1%}")
    print(f"Over 1.5 {M['Over1.5']:.1%} · Over 2.5 {M['Over2.5']:.1%} · Over 3.5 {M['Over3.5']:.1%} · BTTS {M['BTTS_oui']:.1%}")
    print(f"Buts équipe : dom O0.5 {M['dom_Over0.5']:.1%} O1.5 {M['dom_Over1.5']:.1%} · ext O0.5 {M['ext_Over0.5']:.1%} O1.5 {M['ext_Over1.5']:.1%}")
    print("Scores les plus probables : " + ", ".join(f"{s} ({p:.1%})" for s, p in M["scores"][:3]))
    print("Sensibilité (scénarios, pas un intervalle de confiance) :")
    for k, v in sens.items():
        print(f"  {k:28s} {v}")
    for e in evs:
        print(f"  EV {e['marche']:14s} p={e['p']:.3f} cote={e['cote']} → EV {e['ev']:+.1%}" +
              (f"  (min sensibilité {min(e['sens']):+.1%})" if e["sens"] else "") + (f"  états {e.get('etats')}" if e.get("etats") else "") +
              (f"  ⚠ écart marché {e['ecart_marche']:+.1%}" if e.get("suspect") else ""))
    for n_ in notes:
        print("Note :", n_)
    print(f"DÉCISION : {decision}")
    print(f"forecast_id : {fid}" + (" (enregistré dans ledger/forecasts.jsonl)" if a.record else " (non enregistré : ajouter --record)"))


# ───────────────────────────── journal / audit ─────────────────────────────

def cmd_settle(a):
    LEDGER_DIR.mkdir(exist_ok=True)
    known = {json.loads(l)["forecast_id"] for l in open(LEDGER_DIR / "forecasts.jsonl", encoding="utf-8")} \
        if (LEDGER_DIR / "forecasts.jsonl").exists() else set()
    if a.forecast_id not in known:
        sys.exit("forecast_id inconnu dans ledger/forecasts.jsonl")
    rec = {"forecast_id": a.forecast_id, "enregistre_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
           "statut": a.status, "score": a.score, "mise_executee": a.executed_stake, "cote_executee": a.executed_odds,
           "observations": a.note}
    with open(LEDGER_DIR / "settlements.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print("Résultat ajouté (append-only) :", rec)


def cmd_audit(a):
    F = [json.loads(l) for l in open(LEDGER_DIR / "forecasts.jsonl", encoding="utf-8")] if (LEDGER_DIR / "forecasts.jsonl").exists() else []
    S = {}
    if (LEDGER_DIR / "settlements.jsonl").exists():
        for l in open(LEDGER_DIR / "settlements.jsonl", encoding="utf-8"):
            s = json.loads(l); S[s["forecast_id"]] = s          # dernière saisie = correction tracée, l'historique reste
    ll, br = [], []
    print("| id | match | décision | 1/X/2 prévus | statut | score |\n|---|---|---|---|---|---|")
    for f in F:
        s = S.get(f["forecast_id"], {}); sc = s.get("score")
        m = f["marches"]
        print(f"| {f['forecast_id']} | {f['home']} – {f['away']} | {f['decision']} | {m['1']:.2f}/{m['X']:.2f}/{m['2']:.2f} | {s.get('statut', 'EN ATTENTE')} | {sc or '—'} |")
        if sc and s.get("statut") == "JOUE":
            h, g = map(int, sc.split("-")); k = 0 if h > g else 1 if h == g else 2
            p = np.array([m["1"], m["X"], m["2"]]); ll.append(-math.log(max(p[k], 1e-12))); br.append(m_brier3(p, k))
    if ll:
        print(f"\nMatchs réglés : {len(ll)} · log-loss 1X2 {np.mean(ll):.4f} · Brier {np.mean(br):.4f}"
              " — un petit échantillon ne valide ni n'invalide le modèle.")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("backtest")
    b.add_argument("--divs", default="E0,E1,E2,E3,SP1,I1,D1,F1")
    b.add_argument("--seasons", default="2122,2223,2324,2425,2526", help="ordre chronologique")
    b.add_argument("--n-train", type=int, default=2); b.add_argument("--n-val", type=int, default=2)
    b.add_argument("--refresh", action="store_true")
    s = sp.add_parser("simulate")
    s.add_argument("--div", default=""); s.add_argument("--home", required=True); s.add_argument("--away", required=True)
    s.add_argument("--seasons", default="2425,2526,2627"); s.add_argument("--asof"); s.add_argument("--kickoff", default="")
    s.add_argument("--lh", type=float); s.add_argument("--la", type=float)
    s.add_argument("--rho", type=float, help="ρ Dixon-Coles propre à la source des λ externes")
    s.add_argument("--lambda-source", help="provenance des λ externes (ex. APEX-INTL H1)")
    s.add_argument("--status-note", help="statut de validation des λ externes ; ne doit pas commencer par VALIDÉ sans comparaison marché")
    s.add_argument("--scenario", action="append", help="proba:mult_dom:mult_ext (correction subjective)")
    s.add_argument("--odds-1x2"); s.add_argument("--odds-ou25"); s.add_argument("--odds-btts")
    s.add_argument("--odds-ah", action="append", help="dom|ext:ligne:cote, ex. dom:-0.25:1.95")
    s.add_argument("--odds-dnb", action="append", help="1|2:cote")
    s.add_argument("--odds-source", default=""); s.add_argument("--odds-time", default="")
    s.add_argument("--missing-lineup", action="store_true")
    s.add_argument("--n0", type=int, default=10_000); s.add_argument("--nmax", type=int, default=640_000)
    s.add_argument("--tol", type=float, default=0.005); s.add_argument("--seed", type=int, default=2026)
    s.add_argument("--record", action="store_true"); s.add_argument("--refresh", action="store_true")
    st = sp.add_parser("settle")
    st.add_argument("--forecast-id", required=True)
    st.add_argument("--status", default="JOUE", choices=["JOUE", "REPORTE", "ANNULE", "EXCLU"])
    st.add_argument("--score"); st.add_argument("--executed-stake", type=float); st.add_argument("--executed-odds", type=float)
    st.add_argument("--note", default="")
    sp.add_parser("audit")
    a = p.parse_args()
    {"backtest": cmd_backtest, "simulate": cmd_simulate, "settle": cmd_settle, "audit": cmd_audit}[a.cmd](a)


if __name__ == "__main__":
    main()
