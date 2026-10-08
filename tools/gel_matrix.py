"""Matrice du gel des signaux APEX.

Transforme le « gel » (jusqu'ici un simple booléen `PROMOTION_FROZEN = True`) en une
TABLE DE CONDITIONS évaluée par match. Chaque match reçoit une ligne de cases
(✅ remplie / ❌ manquante / — inconnue) et un verrou final GELÉ / PRÊT.

Deux portes sont GLOBALES et DURES : tant qu'une seule est rouge, AUCUN match ne peut
passer à PRÊT, quelles que soient ses cases locales. Ce sont :
  • modèle supérieur au marché  (backtests/latest_params.json → statut_validation)
  • CLV cumulé non négatif        (tools/apex_clv.summary)

Règles :
  • Anti-invention : une condition non vérifiable est 'na' (inconnue), JAMAIS comptée ✅.
  • Le gel ne se lève (PRÊT) que si TOUTES les cases locales sont ✅ ET les deux portes
    globales sont vertes. Aujourd'hui la porte « modèle » est rouge → tout reste GELÉ.
  • Ce module ne price pas et n'émet aucun pari : il rend lisible POURQUOI chaque signal
    est gelé, et lève le gel automatiquement, par match, le jour où tout est réuni.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import orion_consensus as O  # noqa: E402  (seuil de désaccord partagé)

# Seuil de désaccord « faible » = celui d'ORION (une seule vérité).
LOW_DISAGREEMENT = O.HIGH_DISAGREEMENT
MIN_VOICES = O.MIN_INDEP_VOICES
EV_MIN = 0.03          # EV ≥ 3 % requis sur le marché recommandé (fraction)
DQ_FLOOR = 45          # data_quality plancher (seuil APEX-SYNC) sous lequel les données vétoisent

# États de case
OK, NO, NA = "ok", "no", "na"
_GLYPH = {OK: "✅", NO: "❌", NA: "—"}


def glyph(state: str) -> str:
    return _GLYPH.get(state, "—")


# ───────────────────────────── portes globales (dures) ─────────────────────────────

def evaluate_global(day: str | None = None) -> dict:
    """État des deux portes globales. Rouge = gel maintenu pour TOUS les matchs.
    Jamais inventé : lit les fichiers réels ; donnée absente ⇒ porte 'na' (= rouge prudent)."""
    g = {"modele_sup_marche": NA, "clv_cumule_ok": NA, "audit_council": NA,
         "statut_backtest": None, "clv_moyen": None, "pnl_units": None, "clv_n": 0}

    # Porte 1 — modèle supérieur au marché
    try:
        params = json.loads((ROOT / "backtests" / "latest_params.json").read_text())
        statut = (params.get("statut_validation") or "").strip()
        g["statut_backtest"] = statut or None
        if statut:
            up = statut.upper()
            # Vert UNIQUEMENT si le statut affirme explicitement la supériorité et ne contient
            # aucune négation. Tout statut « NON SUPÉRIEUR … » ⇒ rouge.
            if "NON SUP" in up or "NON VALID" in up or "ILLUSOIRE" in up:
                g["modele_sup_marche"] = NO
            elif "SUPÉRIEUR" in up or "SUPERIEUR" in up or "VALIDÉ" in up or "VALIDE" in up:
                g["modele_sup_marche"] = OK
            else:
                g["modele_sup_marche"] = NO  # statut non concluant → prudence = rouge
    except Exception:
        g["modele_sup_marche"] = NA

    # Porte 2 — CLV cumulé non négatif
    try:
        import apex_clv as C
        s = C.summary()
        g["clv_moyen"] = s.get("clv_moyen")
        g["pnl_units"] = s.get("pnl_units")
        g["clv_n"] = s.get("clv_n") or 0
        if g["clv_n"] and g["clv_moyen"] is not None:
            g["clv_cumule_ok"] = OK if g["clv_moyen"] >= 0 else NO
        else:
            g["clv_cumule_ok"] = NA  # pas encore d'échantillon réglé → inconnu, donc rouge prudent
    except Exception:
        g["clv_cumule_ok"] = NA

    # Porte 3 — audit council/preflight (S8). Agents non exécutés par le cron horaire : on ne peut
    # pas affirmer qu'ils sont passés ⇒ 'na' = rouge prudent, jamais coché ✅ sans exécution réelle.
    g["audit_council"] = NA

    g["portes_vertes"] = (g["modele_sup_marche"] == OK and g["clv_cumule_ok"] == OK
                          and g["audit_council"] == OK)
    return g


# ───────────────────────────── matrice par match ─────────────────────────────

# (clé, libellé court, libellé long) — l'ordre est celui des colonnes du digest.
CELLS = [
    ("backtest", "Ligue BT", "Ligue backtestée"),
    ("sim", "Sim BSM", "Simulation BSM calibrée présente"),
    ("voix", "≥3 voix", "≥ 3 voix indépendantes"),
    ("accord", "Accord", "Désaccord interne faible"),
    ("cote", "Cote/EV", "Cote horodatée + EV ≥ 3 % stable"),
    ("veto", "Intégrité", "Aucun veto d'intégrité (handicap AH non suspect, données fiables)"),
]


def evaluate_match(verdict: dict, g: dict | None = None, day: str | None = None) -> dict:
    """Évalue les cases locales d'un match + applique les portes globales.
    `verdict` = dict produit par orion_consensus.arbitrate enrichi par apex_fusion.
    Renvoie {cells, verrou, manquants, portes}."""
    if g is None:
        g = evaluate_global(day)

    couches = verdict.get("couches") or {}
    n_indep = verdict.get("n_independantes")
    des = verdict.get("desaccord")

    cells = {}
    # Ligue backtestée
    cells["backtest"] = OK if verdict.get("bsm_in_scope") else NO
    # Simulation BSM réellement présente (couche S active ⇒ sim calibrée versée ce jour)
    cells["sim"] = OK if couches.get("S") else NO
    # ≥ 3 voix indépendantes
    cells["voix"] = OK if (isinstance(n_indep, int) and n_indep >= MIN_VOICES) else NO
    # Désaccord faible
    if des is None:
        cells["accord"] = NA
    else:
        cells["accord"] = OK if des < LOW_DISAGREEMENT else NO
    wm = verdict.get("worm_meta") or {}
    # Cote horodatée + EV ≥ 3 % stable (depuis le snapshot WORM) :
    #   • pas de cote dans le snapshot ⇒ non vérifiable ⇒ ❌ (jamais ✅ sans cote réelle)
    #   • EV absente ⇒ inconnu
    #   • sinon ✅ seulement si EV ≥ EV_MIN ET signal stable
    ev = wm.get("ev_best")
    if not wm.get("odds_present"):
        cells["cote"] = NO
    elif ev is None:
        cells["cote"] = NA
    else:
        cells["cote"] = OK if (ev >= EV_MIN and wm.get("signal_stable")) else NO
    # Veto d'intégrité (calculable à chaque scan) : handicap asiatique suspect / reco neutralisée,
    # ou données trop faibles. L'audit council/preflight (S8) reste une PORTE GLOBALE séparée.
    dq = wm.get("data_quality")
    if wm.get("integrity_suspect"):
        cells["veto"] = NO
    elif dq is None:
        cells["veto"] = NA
    else:
        cells["veto"] = OK if dq >= DQ_FLOOR else NO

    local_ok = all(cells[k] == OK for k, _, _ in CELLS)
    verrou = "PRÊT" if (local_ok and g.get("portes_vertes")) else "GELÉ"
    manquants = [lng for k, _, lng in CELLS if cells[k] != OK]
    if not g.get("portes_vertes"):
        if g.get("modele_sup_marche") != OK:
            manquants.append("Modèle supérieur au marché (porte globale)")
        if g.get("clv_cumule_ok") != OK:
            manquants.append("CLV cumulé non négatif (porte globale)")
        if g.get("audit_council") != OK:
            manquants.append("Audit council/preflight passé (porte globale)")

    return {"cells": cells, "verrou": verrou, "manquants": manquants, "portes": g}


def is_frozen(verdict: dict, g: dict | None = None, day: str | None = None) -> bool:
    """True = signal gelé (aucune mise). Remplace le booléen unique : le gel est désormais
    CALCULÉ par la matrice, et ne se lève que si verrou == PRÊT."""
    return evaluate_match(verdict, g, day)["verrou"] != "PRÊT"


def _demo():
    g = evaluate_global()
    print("Portes globales:", json.dumps(g, ensure_ascii=False, indent=2))
    fake = {"couches": {"W": True, "M": True}, "n_independantes": 2, "desaccord": 0.01,
            "bsm_in_scope": False}
    print(json.dumps(evaluate_match(fake, g), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    _demo()
