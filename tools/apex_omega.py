#!/usr/bin/env python3
"""APEX-OMEGA — couche de TRIAGE, TRANSPARENCE et CLASSEMENT au-dessus de FUSION.

Problème traité : la chaîne APEX produit beaucoup de NO BET *secs* — un mur « NO BET » sans dire
POURQUOI ni ce qui s'en rapproche. OMEGA ne supprime pas l'abstention, il la rend LISIBLE :

  1. EXPLIQUER chaque abstention — gate bloquante, donnée manquante, seuil raté, verdict des moteurs.
  2. CLASSER tous les marchés d'un match (« closest-to-actionable ») avec un EV HONNÊTE : un EV n'est
     calculé que s'il existe une COTE RÉELLE horodatée dans le snapshot. Pas de cote ⇒ pas d'EV inventé,
     le marché part en surveillance (WATCH), jamais en recommandation.
  3. WATCH list réelle — marchés à suivre pour un mouvement de ligne, aucune mise.
  4. LEAN / BET uniquement si l'EV mesuré est réellement positif (LEAN 1,5–3 %, BET ≥ 3 %).
  5. PASS justifié, jamais vide — si tous les marchés cotés sont à EV < 0, on affiche quand même les 3
     « moins mauvais » À TITRE INFORMATIF, EV négatif clairement marqué, aucune recommandation de mise.

Ce que OMEGA NE FAIT PAS (refusé sciemment, cf. articles 4.3 / 7 / 8 / 11.4 de la directive initiale) :
  • aucune recommandation de mise forcée sur un EV négatif ;
  • aucun « emprunt » de probabilité fabriqué quand le moteur de ligue est muet ;
  • aucune relance anti-NO BET qui forcerait une sortie ;
  • aucune abrogation des gates — les garde-fous restent entiers.

GARDE-FOUS CONSERVÉS (inchangés) : gel actif (aucune mise réelle émise), gates G0–G7 et matrice du gel,
plafonds Kelly fractionné, anti-corrélation, règle 1 pari/marché/match, journal append-only, ANTI-INVENTION,
respect des backtests négatifs. Sous gel, la mise réelle de tout palier — BET inclus — vaut 0.

Les probabilités consensus viennent de la STRUCTURE WORM (λ de classement, mécanique Monte-Carlo
Dixon-Coles). Ce n'est PAS le modèle BSM calibré (règle CLAUDE.md) : c'est une lecture descriptive,
non validée. Rien n'est émis comme pari réel.

Usage :
  python3 tools/apex_omega.py triage [--date J]      # blocs de triage par match (texte)
  python3 tools/apex_omega.py card   [--date J]      # carte HTML OMEGA seule
  python3 tools/apex_omega.py demo                   # démonstration mécanique autonome (sans réseau)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import apex_worm as W          # noqa: E402  (snapshot horodaté → records par fixture, cotes réelles)
import apex_bsm as B           # noqa: E402  (simulate_scores + markets_from_sims : dérivation des marchés)
import apex_apifootball as AF  # noqa: E402  (pick_book : extraction des cotes réelles du snapshot)
import gel_matrix as GM        # noqa: E402  (matrice du gel : cases locales + portes globales)
# PAS d'import apex_fusion ici : apex_fusion importe apex_omega en différé (anti-cycle).

N_SIMS = 20000
SEED = 20261020

# Seuils de palier, en EV % (EV = p·cote − 1). Un palier qui AUTORISE une mise (BET/LEAN) exige une
# COTE RÉELLE ; sans cote on ne prétend aucun EV, le marché reste WATCH (à tracker).
EV_BET = 3.0
EV_LEAN = 1.5
EV_WATCH_FLOOR = 0.0

# Kelly fractionné (indicatif seulement : la mise RÉELLE est 0 tant que le gel est actif).
KELLY_FRACTION = 0.25
KELLY_CAP = 0.02   # 2 % de bankroll, plafond nominal

PALIER_RANK = {"BET": 0, "LEAN": 1, "WATCH": 2, "PASS": 3}
FEU_RANK = {"VERT": 0, "ORANGE": 1, "ROUGE": 2}


# ───────────────────────────── simulation structurelle (λ WORM) ─────────────────────────────

def _bt_params():
    """(rho, sigma) du dernier backtest ; défauts prudents si absent (jamais inventé, juste par défaut)."""
    try:
        p = json.loads((ROOT / "backtests" / "latest_params.json").read_text(encoding="utf-8"))
        return p.get("rho", -0.05), p.get("sigma", 0.0)
    except (OSError, ValueError):
        return -0.05, 0.0


def _sim_markets(lh: float, la: float, rho: float, sigma: float, rng) -> dict:
    """Marchés dérivés de la structure WORM (Poisson-classement Dixon-Coles). NON BSM, non validé."""
    hg, ag = B.simulate_scores(lh, la, rho, sigma, [(1, 1, 1)], N_SIMS, rng)
    return B.markets_from_sims(hg, ag)


# ───────────────────────────── extraction des cotes réelles ─────────────────────────────

def _odds_of(rec: dict) -> dict:
    """Cotes réelles du snapshot WORM (meilleur book par marché via pick_book). Absent = None."""
    o = rec.get("odds") or {}
    _, o1x2 = AF.pick_book(o, "1X2")
    _, ou = AF.pick_book(o, "OU")
    _, btts = AF.pick_book(o, "BTTS")
    _, ah = AF.pick_book(o, "AH")
    _, dc = AF.pick_book(o, "DC")
    return {"1X2": o1x2, "OU": ou, "BTTS": btts, "AH": ah, "DC": dc, "update": o.get("_update")}


def _dc_odds(dc, key):
    """Cote Double Chance pour 1X / 12 / X2 si le book la publie sous un libellé reconnu. Sinon None
    (on n'invente pas de cote DC : le marché reste affiché en proba, EV non calculable)."""
    if not isinstance(dc, dict):
        return None
    aliases = {
        "1X": ["1X", "Home/Draw", "Home or Draw", "1X (Home/Draw)"],
        "12": ["12", "Home/Away", "Home or Away", "12 (Home/Away)"],
        "X2": ["X2", "Draw/Away", "Draw or Away", "X2 (Draw/Away)"],
    }
    for a in aliases.get(key, []):
        v = dc.get(a)
        if v:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


# ───────────────────────────── énumération de l'univers de marchés ─────────────────────────────

def _enumerate_markets(M: dict, odds: dict) -> list[dict]:
    """Univers de marchés (résultat, buts, handicap). Chaque ligne : {famille, marche, p, cote, _settle?}.
    `p` est la proba consensus (structure WORM) ; `cote` la cote réelle ou None (jamais fabriquée)."""
    rows: list[dict] = []
    o1x2 = odds.get("1X2")
    ou = odds.get("OU") if isinstance(odds.get("OU"), dict) else {}
    btts = odds.get("BTTS")
    ah = odds.get("AH") if isinstance(odds.get("AH"), dict) else {}
    dc = odds.get("DC")

    # Résultat 1X2
    for key, lab, idx in (("1", "1 (domicile)", 0), ("X", "X (nul)", 1), ("2", "2 (extérieur)", 2)):
        rows.append({"famille": "Résultat", "marche": lab, "p": M[key],
                     "cote": (o1x2[idx] if o1x2 else None)})
    # Double chance
    for key, lab in (("1X", "Double chance 1X"), ("X2", "Double chance X2"), ("12", "Double chance 12")):
        rows.append({"famille": "Résultat", "marche": lab, "p": M[key], "cote": _dc_odds(dc, key)})

    # Buts : Over/Under
    for L in (0.5, 1.5, 2.5, 3.5, 4.5):
        pair = ou.get(str(L)) if isinstance(ou, dict) else None
        over_c = pair[0] if (pair and len(pair) == 2) else None
        under_c = pair[1] if (pair and len(pair) == 2) else None
        rows.append({"famille": "Buts", "marche": f"Over {L}", "p": M[f"Over{L}"], "cote": over_c})
        rows.append({"famille": "Buts", "marche": f"Under {L}", "p": M[f"Under{L}"], "cote": under_c})
    # BTTS
    rows.append({"famille": "Buts", "marche": "BTTS Oui", "p": M["BTTS_oui"],
                 "cote": (btts[0] if btts else None)})
    rows.append({"famille": "Buts", "marche": "BTTS Non", "p": M["BTTS_non"],
                 "cote": (btts[1] if btts else None)})

    # Handicap asiatique (lignes principales) — EV via ev_states (demi-lignes gérées en amont).
    for L in (-1.5, -1.0, -0.5, 0.5, 1.0, 1.5):
        for side in ("dom", "ext"):
            s = M.get(f"AH_{side}{L:+.2f}")
            if s is None:
                continue
            cote = (ah.get(side) or {}).get(f"{L:+.2f}") if isinstance(ah, dict) else None
            rows.append({"famille": "Handicap", "marche": f"AH {side} {L:+.2f}",
                         "p": s.get("gain"), "cote": cote, "_settle": s})
    return rows


def _ev_pct(row: dict):
    """EV en %, uniquement si cote réelle. AH : via ev_states (gère gains/demis/rembours)."""
    cote = row.get("cote")
    if cote is None:
        return None
    try:
        cote = float(cote)
    except (TypeError, ValueError):
        return None
    if "_settle" in row:
        return round(B.ev_states(row["_settle"], cote) * 100.0, 3)
    p = row.get("p")
    if p is None:
        return None
    return round((p * cote - 1.0) * 100.0, 3)


def _palier_marche(ev_pct, cote, modele_sup: bool) -> str:
    """Palier d'un marché.

    GARDE-FOU CENTRAL — tant que le backtest n'a PAS prouvé le modèle supérieur au marché
    (`modele_sup=False`, porte globale rouge), aucun marché ne peut être BET/LEAN : l'EV calculé
    est une DIVERGENCE structure↔marché « présumée illusoire » (statut backtest), pas un bord
    exploitable. Palier plafonné à WATCH. C'est la traduction honnête du fait que l'EV positif
    d'un modèle non supérieur ne gagne pas d'argent — on surveille, on ne mise pas.

    Sans cote réelle : WATCH (trackable) — on ne prétend pas un EV qu'on ne peut pas calculer.
    Avec cote ET modèle prouvé supérieur : barème EV honnête (BET/LEAN/WATCH/PASS)."""
    if not modele_sup:
        return "WATCH"
    if cote is None or ev_pct is None:
        return "WATCH"
    if ev_pct >= EV_BET:
        return "BET"
    if ev_pct >= EV_LEAN:
        return "LEAN"
    if ev_pct >= EV_WATCH_FLOOR:
        return "WATCH"
    return "PASS"


def _kelly_indicatif(ev_pct, cote):
    """Fraction de Kelly fractionné, INDICATIVE (la mise réelle reste 0 sous gel). None si non pertinent."""
    if cote is None or ev_pct is None or ev_pct <= 0:
        return None
    try:
        cote = float(cote)
    except (TypeError, ValueError):
        return None
    if cote <= 1.0:
        return None
    f_full = (ev_pct / 100.0) / (cote - 1.0)       # Kelly plein = edge / (cote−1)
    return round(min(KELLY_CAP, KELLY_FRACTION * f_full), 4)


# ───────────────────────────── feu dérivé & explications ─────────────────────────────

def _feu(v: dict) -> str:
    """Feu de risque DÉRIVÉ de worm_meta + matrice du gel (pas un run apex_sync complet).
    ROUGE : intégrité suspecte ou aucune cote. ORANGE : signal gelé. VERT : verrou PRÊT."""
    wm = v.get("worm_meta") or {}
    if wm.get("integrity_suspect") or not wm.get("odds_present"):
        return "ROUGE"
    if (v.get("gel") or {}).get("verrou") == "PRÊT":
        return "VERT"
    return "ORANGE"


def _explain(v: dict, actionable: list[dict]) -> dict:
    """Pourquoi ce match ne porte pas de mise : gate bloquante, données manquantes, seuil raté,
    verdict des moteurs. Tout vient de l'évaluation réelle (matrice du gel + ORION), rien n'est inventé."""
    gm = v.get("gel") or {}
    cells = gm.get("cells") or {}
    couches = v.get("couches") or {}

    # Gate bloquante : cases locales ❌ (libellé long) + portes globales via manquants.
    cell_labels = {k: lng for k, _, lng in GM.CELLS}
    gates_locales = [cell_labels[k] for k, _, _ in GM.CELLS if cells.get(k) != GM.OK]
    manquants = gm.get("manquants") or []
    portes_globales = [m for m in manquants if "porte globale" in m]

    # Données manquantes : couches d'information absentes + cote.
    donnees = []
    if not couches.get("S"):
        donnees.append("simulation BSM calibrée absente (ligue non backtestée ou sim non versée)")
    if not couches.get("K"):
        donnees.append("voix marché sharp absente (ligue non couverte par apex_sources)")
    if not couches.get("C"):
        donnees.append("indice comportemental apex-bi-* absent")
    if not (v.get("worm_meta") or {}).get("odds_present"):
        donnees.append("aucune cote horodatée dans le snapshot")

    # Seuil raté : meilleur EV coté vs seuils ; voix indépendantes ; désaccord.
    seuils = []
    best_ev = max((r["ev_pct"] for r in actionable if r.get("ev_pct") is not None), default=None)
    if best_ev is None:
        seuils.append("EV non calculable (aucun marché coté) → aucun palier BET/LEAN possible")
    elif best_ev < EV_BET:
        seuils.append(f"meilleur EV coté = {best_ev:+.2f} % (< seuil BET {EV_BET:.0f} %)")
    n_ind = v.get("n_independantes")
    if isinstance(n_ind, int) and n_ind < 3:
        seuils.append(f"{n_ind} voix indépendantes (< 3 → ORION COLLECTER)")
    des = v.get("desaccord")
    if des is not None and des >= 0.14:
        seuils.append(f"désaccord inter-moteurs élevé (σ={des})")

    return {
        "gate_bloquante": gates_locales + portes_globales,
        "donnees_manquantes": donnees,
        "seuil_rate": seuils,
        "verdict_moteurs": {
            "ORION": f"{v.get('decision')} — {v.get('raison', '')}".strip(" —"),
            "WORM_tier": v.get("tier_worm") or "—",
            "confiance": v.get("confiance"),
            "desaccord": des,
        },
    }


# ───────────────────────────── triage par match ─────────────────────────────

def _triage_one(v: dict, rec: dict, rho: float, sigma: float, rng,
                modele_sup: bool, statut_backtest: str | None) -> dict:
    """Bloc de triage d'un match. Jamais de sortie vide : au pire, abstention EXPLIQUÉE + top marchés.

    `modele_sup` : le backtest a-t-il prouvé le modèle supérieur au marché ? Si False (cas réel
    actuel), tous les paliers sont plafonnés à WATCH et l'EV est présenté comme une DIVERGENCE,
    pas un bord (voir _palier_marche)."""
    lambdas = v.get("lambdas")
    feu = _feu(v)
    block = {
        "fixture_id": v.get("fixture_id"),
        "match": v.get("match"),
        "league": v.get("league"),
        "country": v.get("country"),
        "kickoff": v.get("kickoff"),
        "orion": v.get("decision"),
        "orion_desaccord": v.get("desaccord"),
        "feu": feu,
        "feu_note": "dérivé worm+gel (pas un run apex_sync complet)",
        "couches": "".join(k for k, on in (v.get("couches") or {}).items() if on) or "—",
        "verrou_gel": (v.get("gel") or {}).get("verrou"),
        "gel_actif": True,                         # garde-fou : aucune mise réelle émise
        "modele_sup_marche": modele_sup,
        "statut_backtest": statut_backtest,
        "label": "RECOMMANDATION — gel actif",
        "marquages": [],
        "structure_source": "structure WORM (λ classement, Monte-Carlo Dixon-Coles) — NON BSM, non validé",
        # Sens de l'EV affiché selon le régime : vrai bord seulement si le modèle est prouvé supérieur.
        "ev_sens": ("bord estimé (modèle prouvé supérieur au marché)" if modele_sup
                    else "DIVERGENCE structure↔marché — PAS un bord (modèle non supérieur au marché)"),
    }

    if not (isinstance(lambdas, (list, tuple)) and len(lambdas) == 2):
        # Pas de λ → pas de triage de marché possible. On n'invente rien : abstention expliquée.
        block["classement"] = []
        block["watch_cote_absente"] = []
        block["scores_probables"] = []
        block["palier_match"] = "WATCH"
        block["marquages"].append("STRUCTURE_INDISPONIBLE")
        block["explication"] = _explain(v, [])
        block["explication"]["seuil_rate"].insert(0, "λ de classement WORM absents → triage de marché impossible")
        return block

    lh, la = float(lambdas[0]), float(lambdas[1])
    M = _sim_markets(lh, la, rho, sigma, rng)
    odds = _odds_of(rec)
    rows = _enumerate_markets(M, odds)

    # EV + palier par marché. Kelly indicatif UNIQUEMENT si le modèle est prouvé supérieur
    # (sinon l'EV n'est pas un bord → une mise Kelly n'aurait aucun sens, même indicative).
    for r in rows:
        r["ev_pct"] = _ev_pct(r)
        r["palier"] = _palier_marche(r.get("ev_pct"), r.get("cote"), modele_sup)
        r["kelly_indic"] = _kelly_indicatif(r.get("ev_pct"), r.get("cote")) if modele_sup else None
        # Divergence extrême = presque toujours une FAIBLESSE du modèle (compétition non calibrée,
        # cote isolée), pas une opportunité. On le signale pour éviter toute mésinterprétation.
        ev = r.get("ev_pct")
        r["divergence_suspecte"] = bool(ev is not None and ev > 50.0)

    actionable = [r for r in rows if r.get("cote") is not None and r.get("ev_pct") is not None]
    watch_no_odds = [r for r in rows if r.get("cote") is None]

    # Classement « closest-to-actionable » : palier, puis EV/divergence décroissante, puis proba.
    actionable.sort(key=lambda r: (PALIER_RANK.get(r["palier"], 9), -(r["ev_pct"]), -(r.get("p") or 0)))

    # Palier du match.
    if not modele_sup:
        # Régime réel : modèle non prouvé supérieur ⇒ aucun bord ⇒ WATCH plafonné (jamais BET/LEAN/PASS).
        palier_match = "WATCH"
        block["marquages"].append("MODELE_NON_SUPERIEUR")
        if not actionable:
            block["marquages"].append("COTES_ABSENTES")
    elif actionable:
        palier_match = actionable[0]["palier"]
        if all(r["ev_pct"] < EV_WATCH_FLOOR for r in actionable):   # PASS = tous les cotés à EV < 0
            palier_match = "PASS"
    else:
        palier_match = "WATCH"
        block["marquages"].append("COTES_ABSENTES")

    block["palier_match"] = palier_match

    # Top 3 du classement. En régime « non supérieur » : les 3 plus fortes divergences, à SURVEILLER.
    # En PASS : les 3 « moins mauvais », EV négatif marqué, informatif.
    if palier_match == "PASS":
        palier_affiche = lambda r: "PASS (informatif)"
    elif not modele_sup:
        palier_affiche = lambda r: "WATCH (divergence)"
    else:
        palier_affiche = lambda r: r["palier"]
    top3 = actionable[:3]
    block["classement"] = [{
        "famille": r["famille"], "marche": r["marche"],
        "palier": palier_affiche(r),
        "p": round(r["p"], 4) if r.get("p") is not None else None,
        "cote": r.get("cote"),
        "ev_pct": r.get("ev_pct"),
        "ev_sens": block["ev_sens"],
        "divergence_suspecte": r.get("divergence_suspecte", False),
        "kelly_indic": r.get("kelly_indic"),
        "mise_reelle": 0.0,                         # gel actif
        "ev_negatif": (r["ev_pct"] < 0),
    } for r in top3]

    # WATCH list réelle : marchés sans cote (à suivre quand une cote apparaît), triés par proba.
    watch_no_odds.sort(key=lambda r: -(r.get("p") or 0))
    block["watch_cote_absente"] = [{
        "famille": r["famille"], "marche": r["marche"],
        "p": round(r["p"], 4) if r.get("p") is not None else None,
        "note": "cote absente du snapshot — à tracker",
    } for r in watch_no_odds[:6]]

    # Scores exacts probables (proba seule, aucune cote → informatif).
    block["scores_probables"] = [{"score": s, "p": p} for s, p in (M.get("scores") or [])[:6]]

    # Flux non présents dans le snapshot : écrits absents (anti-invention), jamais de ligne fabriquée.
    block["flux_absents"] = "Corners / Cartons / Buteurs : flux absent du snapshot — non évalué (anti-invention)."

    if palier_match == "PASS":
        block["marquages"].append("PASS_JUSTIFIE")
    if block["verrou_gel"] == "GELÉ":
        block["marquages"].append("GELÉ")

    block["explication"] = _explain(v, actionable)
    # En régime « non supérieur », la raison DOMINANTE d'absence de mise est la porte globale :
    # on la met en tête pour que ce soit la première chose lue.
    if not modele_sup:
        block["explication"]["seuil_rate"].insert(
            0, f"porte globale ROUGE — modèle non supérieur au marché (backtest : "
               f"{statut_backtest or 'statut inconnu'}) ⇒ EV = divergence, pas un bord ⇒ palier plafonné à WATCH")
    return block


def triage_from_verdicts(day: str, verdicts: list, rows: dict | None = None) -> list[dict]:
    """Produit un bloc de triage par match à partir des verdicts ORION déjà calculés par FUSION.
    N'importe PAS apex_fusion (anti-cycle) : le caller fournit `verdicts`."""
    if rows is None:
        d = dt.date.fromisoformat(day)
        rows = {r.get("fixture_id"): r for r in W.latest_by_fixture(d)}
    rho, sigma = _bt_params()
    rng = np.random.default_rng(SEED)
    # Porte globale « modèle supérieur au marché » (dure, commune à tous les matchs). Tant qu'elle
    # est rouge, aucun EV n'est un bord exploitable — OMEGA plafonne tous les paliers à WATCH.
    g = GM.evaluate_global(day)
    modele_sup = (g.get("modele_sup_marche") == GM.OK)
    statut_backtest = g.get("statut_backtest")
    out = []
    for v in verdicts:
        rec = rows.get(v.get("fixture_id"), {}) if isinstance(rows, dict) else {}
        out.append(_triage_one(v, rec, rho, sigma, rng, modele_sup, statut_backtest))
    # Tri de sortie : palier du match, puis feu, puis meilleur EV.
    def best_ev(b):
        evs = [c["ev_pct"] for c in b.get("classement", []) if c.get("ev_pct") is not None]
        return max(evs) if evs else -999
    out.sort(key=lambda b: (PALIER_RANK.get(b["palier_match"], 9),
                            FEU_RANK.get(b["feu"], 9), -best_ev(b)))
    return out


def triage_day(day: str) -> list[dict]:
    """Chemin autonome : calcule les verdicts via FUSION (import différé) puis le triage OMEGA."""
    import apex_fusion as F       # différé : casse le cycle d'import
    verdicts = F.arbitrate_day(day)
    return triage_from_verdicts(day, verdicts)


# ───────────────────────────── rendus (texte + HTML) ─────────────────────────────

def _fmt_pct(x):
    return "—" if x is None else f"{x:.0%}"


def _fmt_ev(x):
    return "n/c" if x is None else f"{x:+.2f}%"


def render_text(day: str, blocks: list[dict]) -> str:
    L = [f"APEX-OMEGA {day} — triage & classement (couche au-dessus de FUSION)",
         "RECOMMANDATION — gel actif : aucune mise réelle émise (mise = 0 sur tout palier).", ""]
    modele_sup = bool(blocks and blocks[0].get("modele_sup_marche"))
    if blocks and not modele_sup:
        L.append("⚠ MODÈLE NON SUPÉRIEUR AU MARCHÉ (porte globale rouge, backtest : "
                 f"{blocks[0].get('statut_backtest') or 'statut inconnu'}).")
        L.append("  ⇒ Les « EV » affichés sont des DIVERGENCES structure↔marché, PAS des bords exploitables.")
        L.append("  ⇒ Palier plafonné à WATCH : aucun BET/LEAN possible même hors gel. On surveille, on ne mise pas.")
        L.append("")
    from collections import Counter
    c = Counter(b["palier_match"] for b in blocks)
    L.append("Paliers de match : " + (" · ".join(f"{n} {k}" for k, n in c.most_common()) or "—"))
    L.append("=" * 78)
    for b in blocks:
        L.append("")
        L.append(f"MATCH : {b['match']} — {(b.get('country') or '')} {(b.get('league') or '')} — {(b.get('kickoff') or '')[:16]}")
        L.append(f"  MOTEURS : ORION={b['orion']} · WORM={b['explication']['verdict_moteurs']['WORM_tier']} "
                 f"· couches={b['couches']} · feu={b['feu']} ({b['feu_note']}) · verrou={b['verrou_gel']}")
        L.append(f"  PALIER MATCH : {b['palier_match']}   {b['label']}   marquages={','.join(b['marquages']) or '—'}")
        L.append(f"  structure : {b['structure_source']}")
        ev_lbl = "div" if not b.get("modele_sup_marche") else "EV"
        if b["classement"]:
            if b["palier_match"] == "PASS":
                titre = "  CLASSEMENT OMEGA (top 3 — moins mauvais, informatif)"
            elif not b.get("modele_sup_marche"):
                titre = "  CLASSEMENT OMEGA (top 3 — plus fortes divergences structure↔marché, à SURVEILLER)"
            else:
                titre = "  CLASSEMENT OMEGA (top 3)"
            L.append(titre + " :")
            for i, r in enumerate(b["classement"], 1):
                k = f" · Kelly indic {r['kelly_indic']}" if r.get("kelly_indic") else ""
                neg = "  ⚠EV<0" if r.get("ev_negatif") else ""
                susp = "  ⚠divergence extrême (faiblesse modèle probable)" if r.get("divergence_suspecte") else ""
                L.append(f"    {i}. [{r['palier']:>16}] {r['famille']}/{r['marche']} — "
                         f"p={_fmt_pct(r['p'])} cote={r['cote']} {ev_lbl}={_fmt_ev(r['ev_pct'])}"
                         f" mise_réelle={r['mise_reelle']}{k}{neg}{susp}")
        else:
            L.append("  CLASSEMENT OMEGA : aucun marché coté exploitable (voir WATCH / explication).")
        if b["watch_cote_absente"]:
            L.append("  WATCH (cote absente — à tracker) : "
                     + " | ".join(f"{r['marche']} p={_fmt_pct(r['p'])}" for r in b["watch_cote_absente"][:5]))
        if b.get("scores_probables"):
            L.append("  Scores probables : "
                     + " ".join(f"{s['score']}({s['p']:.0%})" for s in b["scores_probables"][:5]))
        ex = b["explication"]
        if b["palier_match"] in ("PASS", "WATCH"):
            L.append("  POURQUOI PAS DE MISE :")
            if ex["gate_bloquante"]:
                L.append("    · gate bloquante : " + " ; ".join(ex["gate_bloquante"]))
            if ex["donnees_manquantes"]:
                L.append("    · données manquantes : " + " ; ".join(ex["donnees_manquantes"]))
            if ex["seuil_rate"]:
                L.append("    · seuil raté : " + " ; ".join(ex["seuil_rate"]))
        L.append(f"  {b['flux_absents']}")
    return "\n".join(L)


def _esc(x):
    return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_card_html(day: str, blocks: list[dict]) -> str:
    """Carte HTML OMEGA à greffer en TÊTE du digest FUSION (avant la carte ORION)."""
    from collections import Counter
    c = Counter(b["palier_match"] for b in blocks)
    repartition = " · ".join(f"{n} {k}" for k, n in c.most_common()) or "—"
    modele_sup = bool(blocks and blocks[0].get("modele_sup_marche"))
    statut = (blocks[0].get("statut_backtest") if blocks else None) or "statut inconnu"
    badge = {"BET": "background:#e7f6ec;color:#137333", "LEAN": "background:#eaf2fb;color:#1a56c4",
             "WATCH": "background:#fef7e0;color:#8a6d00", "PASS": "background:#eef0f4;color:#555"}
    feucol = {"VERT": "#137333", "ORANGE": "#8a6d00", "ROUGE": "#b02a2a"}
    ev_col = "EV" if modele_sup else "Div."

    warn = ""
    if blocks and not modele_sup:
        warn = ("<div style='margin:8px 0;padding:8px 10px;background:#fde8e8;color:#b02a2a;"
                "border-radius:8px'><b>Modèle NON supérieur au marché</b> (porte globale rouge · "
                f"backtest : {_esc(statut)}). Les « {ev_col} » ci-dessous sont des <b>divergences "
                "structure↔marché</b>, <b>pas des bords exploitables</b> : palier plafonné à WATCH, "
                "aucun BET/LEAN même hors gel. On surveille, on ne mise pas.</div>")

    H = ["<div class='card' style='border:2px solid #7c3aed'>",
         f"<h1>APEX-OMEGA — triage &amp; classement · {_esc(day)}</h1>",
         "<div class='muted'>Couche de transparence au-dessus de FUSION : chaque match est classé par "
         "marché (« closest-to-actionable ») avec un EV <b>honnête</b> (calculé seulement s'il existe une "
         "cote réelle, et qui ne vaut comme bord que si le modèle est prouvé supérieur au marché). Aucune "
         f"reco forcée, aucun EV inventé. <b>GEL actif</b> : mise réelle = 0 sur tout palier. Paliers : "
         f"{_esc(repartition)}.</div>",
         warn,
         "<table><tr><th>Match</th><th>KO</th><th>Palier</th><th>Meilleur marché coté</th>"
         f"<th class='r'>p</th><th class='r'>cote</th><th class='r'>{ev_col}</th><th>Feu</th><th>ORION</th></tr>"]
    for b in blocks[:20]:
        top = (b.get("classement") or [{}])[0]
        pal = b["palier_match"]
        st = badge.get(pal, "")
        fc = feucol.get(b["feu"], "#555")
        ko = (b.get("kickoff") or "")[11:16]
        marche = f"{top.get('famille', '—')}/{top.get('marche', '—')}" if top.get("marche") else "— (cote absente)"
        H.append(
            f"<tr><td><b>{_esc(b['match'])}</b></td><td>{_esc(ko)}</td>"
            f"<td><span style='font-weight:700;border-radius:6px;padding:1px 7px;{st}'>{_esc(pal)}</span></td>"
            f"<td>{_esc(marche)}</td>"
            f"<td class='r'>{_fmt_pct(top.get('p'))}</td>"
            f"<td class='r'>{top.get('cote') if top.get('cote') is not None else '—'}</td>"
            f"<td class='r'>{_fmt_ev(top.get('ev_pct'))}</td>"
            f"<td style='font-weight:700;color:{fc}'>{_esc(b['feu'])}</td>"
            f"<td>{_esc(b['orion'])}</td></tr>")
    legende_paliers = (
        "Palier : <b>WATCH</b> uniquement tant que le modèle n'est pas prouvé supérieur au marché "
        "(divergence à surveiller, aucune mise) — BET/LEAN/PASS ne s'ouvrent que si la porte globale passe au vert."
        if not modele_sup else
        "Palier : <b>BET</b> EV≥3 % · <b>LEAN</b> EV 1,5–3 % · <b>WATCH</b> EV 0–1,5 % ou cote absente "
        "(à tracker) · <b>PASS</b> tous marchés cotés à EV&lt;0 (3 moins mauvais affichés, informatif).")
    H += ["</table>",
          f"<div class='muted'>{legende_paliers} Probabilités = structure WORM (non BSM, non validé). "
          "Détail et « pourquoi pas de mise » par match dans le corps du digest / "
          "<code>apex_omega.py triage</code>.</div>",
          "</div>"]
    return "\n".join(H)


# ───────────────────────────── CLI ─────────────────────────────

def cmd_triage(a):
    day = a.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    blocks = triage_day(day)
    print(render_text(day, blocks))
    outdir = ROOT / "reports" / "omega"
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / f"{day}.triage.txt").write_text(render_text(day, blocks), encoding="utf-8")
    (outdir / f"{day}.triage.json").write_text(json.dumps(blocks, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


def cmd_card(a):
    day = a.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    blocks = triage_day(day)
    print(build_card_html(day, blocks))
    return 0


def cmd_demo(a):
    """Démonstration mécanique AUTONOME (sans réseau ni snapshot) : un verdict ORION factice + un
    record WORM factice avec cotes réalistes, pour prouver la mécanique de triage de bout en bout."""
    rho, sigma = _bt_params()
    rng = np.random.default_rng(SEED)
    # Verdict ORION factice (comme en produit apex_fusion.arbitrate_day), λ structurels plausibles.
    v = {
        "fixture_id": 999001, "match": "DÉMO FC – DEMO United", "league": "Démonstration",
        "country": "XX", "kickoff": "2026-10-20T19:00:00+00:00",
        "decision": "COLLECTER", "raison": "seulement 1 voix indépendante (< 3)",
        "confiance": None, "desaccord": None, "tier_worm": "SURVEILLER",
        "n_independantes": 1, "couches": {"W": True, "M": False, "C": False, "S": False, "K": False},
        "bsm_in_scope": False, "lambdas": [1.62, 1.08],
        "worm_meta": {"odds_present": True, "ev_best": None, "signal_stable": True,
                      "data_quality": 72, "integrity_suspect": False},
    }
    v["gel"] = GM.evaluate_match(v)
    # Record WORM factice : cotes réelles PLAUSIBLES (démo uniquement, marquées comme telles).
    rec = {"fixture_id": 999001, "odds": {"Pinnacle": {
        "1X2": [1.95, 3.60, 3.90],
        "OU": {"2.5": [1.90, 1.95], "1.5": [1.28, 3.70], "3.5": [3.40, 1.32]},
        "BTTS": [1.78, 2.00],
        "AH": {"dom": {"-0.50": 1.98, "-1.00": 2.70}, "ext": {"+0.50": 1.88, "+1.00": 1.46}},
    }}}
    g = GM.evaluate_global()
    modele_sup = (g.get("modele_sup_marche") == GM.OK)
    block = _triage_one(v, rec, rho, sigma, rng, modele_sup, g.get("statut_backtest"))
    print(render_text("DÉMO", [block]))
    print(f"\n[Rappel] DÉMO : cotes factices plausibles pour exercer la mécanique — ce n'est PAS un match réel.")
    print(f"[Rappel] modèle supérieur au marché = {modele_sup} (statut backtest : {g.get('statut_backtest')}).")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    t = sp.add_parser("triage", help="blocs de triage par match (texte + fichiers reports/omega/)")
    t.add_argument("--date")
    t.set_defaults(func=cmd_triage)
    cc = sp.add_parser("card", help="carte HTML OMEGA seule")
    cc.add_argument("--date")
    cc.set_defaults(func=cmd_card)
    d = sp.add_parser("demo", help="démonstration mécanique autonome (sans réseau)")
    d.set_defaults(func=cmd_demo)
    a = p.parse_args()
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
