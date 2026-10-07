#!/usr/bin/env python3
"""APEX-HOCKEY — agent A5 (PRICING).

Poisson ajusté + Monte-Carlo, calibré sur la saison en cours de la ligue.
Le modèle football (Dixon-Coles, taux de nuls, base de buts) n'est pas
transposable : au hockey il n'existe pas de nul final en saison régulière
NHL, la prolongation change de format entre saison et playoffs, et le but
en cage vide déforme spécifiquement les écarts de deux buts — donc la
puck line.

Chaîne de simulation, par match simulé :

    1. buts de temps réglementaire, hors cage vide, tirés en Poisson ;
    2. module de fin de match : à un but d'écart, l'équipe menée peut
       égaliser après avoir retiré son gardien, ou encaisser en cage vide ;
    3. prolongation selon la règle de la ligue (A0), puis tirs au but
       si la règle en prévoit.

Tout paramètre non sourcé est signalé `UNSOURCED_PARAM` : le protocole
interdit les constantes codées en dur sans source, et un paramètre par
défaut silencieux se propagerait jusqu'à la mise.
"""

from __future__ import annotations

import json
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from apex_common import now_utc  # noqa: E402

try:
    import numpy as np
except ImportError:  # repli stdlib : plus lent mais identique en loi
    np = None
    import random

DEFAULT_SIMS = 50_000

# Valeurs de repli, utilisées UNIQUEMENT si l'amont n'a rien fourni. Chaque
# usage lève un flag nominatif : elles ne doivent jamais atteindre une mise
# sans avoir été remplacées par une valeur sourcée.
FALLBACKS = {
    "home_ice_multiplier": 1.045,
    "regulation_goal_share": 0.96,
    "empty_net_goal_share": 0.035,
    "p_trailing_ties_with_extra_skater": 0.145,
    "p_empty_net_goal_against": 0.215,
    "p_ot_decided_before_shootout": 0.62,
    "xg_blend_weight": 0.60,
    "shootout_home_edge": 0.0,
}


class ParamSet:
    """Paramètres sourcés. `get` trace toute retombée sur un défaut."""

    def __init__(self, raw: dict | None):
        self.raw = raw or {}
        self.flags: list[str] = []
        self.used: dict[str, dict] = {}

    def get(self, key: str, default=None) -> float:
        entry = self.raw.get(key)
        if isinstance(entry, dict) and entry.get("value") is not None:
            if not entry.get("source"):
                self.flags.append(f"UNSOURCED_PARAM:{key}")
            self.used[key] = {"value": float(entry["value"]),
                              "source": entry.get("source")}
            return float(entry["value"])
        if isinstance(entry, (int, float)):
            # Valeur nue, sans source : acceptée mais signalée.
            self.flags.append(f"UNSOURCED_PARAM:{key}")
            self.used[key] = {"value": float(entry), "source": None}
            return float(entry)
        value = FALLBACKS.get(key) if default is None else default
        if value is None:
            raise KeyError(f"paramètre obligatoire absent : {key}")
        self.flags.append(f"FALLBACK_PARAM:{key}")
        self.used[key] = {"value": float(value), "source": "défaut APEX-HOCKEY (non sourcé)"}
        return float(value)


# --------------------------------------------------------------------------
# Intensités
# --------------------------------------------------------------------------

def team_rate(team: dict, params: ParamSet, tier: str, side: str) -> tuple[float, float]:
    """(attaque, défense) normalisées par la moyenne de la ligue.

    En TIER_A, les buts sont mélangés aux xG : sur dix matchs, les buts seuls
    sont trop bruités pour l'intensité, les xG seuls ignorent la finition.
    """
    league_avg = params.get("league_avg_goals_per_team")
    gf, ga = float(team["gf_per_game"]), float(team["ga_per_game"])
    if tier == "TIER_A" and team.get("xgf_per_game") and team.get("xga_per_game"):
        w = params.get("xg_blend_weight")
        gf = w * float(team["xgf_per_game"]) + (1 - w) * gf
        ga = w * float(team["xga_per_game"]) + (1 - w) * ga
    return gf / league_avg, ga / league_avg


def intensities(home: dict, away: dict, params: ParamSet, tier: str,
                adjustments: list[dict]) -> dict:
    """λ de temps réglementaire, hors buts en cage vide."""
    league_avg = params.get("league_avg_goals_per_team")
    home_ice = params.get("home_ice_multiplier")
    atk_h, def_h = team_rate(home, params, tier, "home")
    atk_a, def_a = team_rate(away, params, tier, "away")

    lam_home = league_avg * atk_h * def_a * home_ice
    lam_away = league_avg * atk_a * def_h / home_ice

    applied = []
    for adj in adjustments or []:
        factor = float(adj["factor"])
        target = adj.get("applies_to", "both")
        if target in ("home", "both"):
            lam_home *= factor
        if target in ("away", "both"):
            lam_away *= factor
        applied.append({"name": adj.get("name"), "factor": factor,
                        "applies_to": target,
                        "justification": adj.get("justification"),
                        "source_url": adj.get("source_url")})

    reg_share = params.get("regulation_goal_share")
    en_share = params.get("empty_net_goal_share")
    # Le Poisson porte sur les buts de temps réglementaire hors cage vide ;
    # les buts en cage vide sont réinjectés par le module de fin de match,
    # sans quoi ils seraient comptés deux fois.
    even_factor = reg_share * (1.0 - en_share)
    return {
        "lambda_home_full": lam_home, "lambda_away_full": lam_away,
        "lambda_home_regulation_even": lam_home * even_factor,
        "lambda_away_regulation_even": lam_away * even_factor,
        "adjustments_applied": applied,
        "regulation_goal_share": reg_share, "empty_net_goal_share": en_share,
    }


# --------------------------------------------------------------------------
# Monte-Carlo
# --------------------------------------------------------------------------

def _poisson(lam: float, size: int, rng):
    if np is not None:
        return rng.poisson(lam, size)
    return [_poisson_scalar(lam) for _ in range(size)]


def _poisson_scalar(lam: float) -> int:
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= random.random()
        if p <= limit:
            return k
        k += 1


def simulate(lam_home: float, lam_away: float, ot_rules: dict | None,
             game_type: str, params: ParamSet, sims: int, seed: int = 20261) -> dict:
    """Simule `sims` matchs complets et renvoie les probabilités de marché."""
    rng = np.random.default_rng(seed) if np is not None else random.Random(seed)
    if np is None:
        random.seed(seed)

    gh = _poisson(lam_home, sims, rng)
    ga = _poisson(lam_away, sims, rng)
    if np is not None:
        gh, ga = gh.astype(int), ga.astype(int)
    else:
        gh, ga = list(gh), list(ga)

    p_tie_en = params.get("p_trailing_ties_with_extra_skater")
    p_en_against = params.get("p_empty_net_goal_against")
    p_ot_decided = params.get("p_ot_decided_before_shootout")
    so_edge = params.get("shootout_home_edge")

    # Force relative pour départager prolongation et tirs au but.
    share_home = lam_home / (lam_home + lam_away)
    playoff = game_type == "playoff"
    shootout_allowed = bool((ot_rules or {}).get(
        "playoff" if playoff else "regular", {}).get("shootout", not playoff))

    def draw(n):
        return rng.random(n) if np is not None else [random.random() for _ in range(n)]

    if np is not None:
        gh, ga = np.array(gh), np.array(ga)
        diff = gh - ga
        one_goal = np.abs(diff) == 1
        u1, u2 = draw(len(gh)), draw(len(gh))

        # Fin de match à un but d'écart : l'équipe menée sort son gardien.
        ties = one_goal & (u1 < p_tie_en)
        gh = np.where(ties & (diff < 0), gh + 1, gh)
        ga = np.where(ties & (diff > 0), ga + 1, ga)
        # Sinon, risque de but dans la cage vide, qui crée l'écart de deux.
        en = one_goal & ~ties & (u2 < p_en_against)
        gh = np.where(en & (diff > 0), gh + 1, gh)
        ga = np.where(en & (diff < 0), ga + 1, ga)

        reg_h, reg_a = gh.copy(), ga.copy()
        tied = reg_h == reg_a
        n_tied = int(tied.sum())

        final_h, final_a = reg_h.copy(), reg_a.copy()
        went_ot = tied.copy()
        went_so = np.zeros(len(gh), dtype=bool)

        if n_tied:
            u_ot, u_win, u_so = draw(n_tied), draw(n_tied), draw(n_tied)
            decided_in_ot = np.ones(n_tied, dtype=bool) if playoff else (u_ot < p_ot_decided)
            home_wins_ot = u_win < share_home
            # Un but de prolongation ajoute exactement un but au vainqueur.
            add_h = np.where(decided_in_ot & home_wins_ot, 1, 0)
            add_a = np.where(decided_in_ot & ~home_wins_ot, 1, 0)
            if shootout_allowed:
                so = ~decided_in_ot
                home_wins_so = u_so < (0.5 + so_edge)
                # Le vainqueur aux tirs au but se voit créditer un but.
                add_h = np.where(so & home_wins_so, 1, add_h)
                add_a = np.where(so & ~home_wins_so, 1, add_a)
                went_so[tied] = so
            final_h[tied] = reg_h[tied] + add_h
            final_a[tied] = reg_a[tied] + add_a

        def frac(mask):
            return float(np.mean(mask))

        reg_total = reg_h + reg_a
        final_total = final_h + final_a
        margin = final_h - final_a
        out = {
            "p_reg_home": frac(reg_h > reg_a),
            "p_reg_draw": frac(reg_h == reg_a),
            "p_reg_away": frac(reg_h < reg_a),
            "p_ot": frac(went_ot),
            "p_shootout": frac(went_so),
            "p_ml_home": frac(final_h > final_a),
            "p_ml_away": frac(final_h < final_a),
            "p_home_-1.5": frac(margin >= 2),
            "p_away_+1.5": frac(margin <= 1),
            "p_away_-1.5": frac(margin <= -2),
            "p_home_+1.5": frac(margin >= -1),
        }
        for line in (5.5, 6.5):
            out[f"p_over_{line}_inc_ot"] = frac(final_total > line)
            out[f"p_under_{line}_inc_ot"] = frac(final_total < line)
            out[f"p_over_{line}_reg"] = frac(reg_total > line)
            out[f"p_under_{line}_reg"] = frac(reg_total < line)

        scores: dict[str, int] = {}
        for h, a in zip(final_h.tolist(), final_a.tolist()):
            key = f"{h}-{a}"
            scores[key] = scores.get(key, 0) + 1
        out["top_scores"] = [{"score": k, "probability": round(v / sims, 4)}
                             for k, v in sorted(scores.items(), key=lambda kv: -kv[1])[:5]]
        return out

    raise RuntimeError("numpy requis pour la simulation (déclaré dans requirements.txt)")


def fair_odds(probabilities: dict) -> dict:
    return {k: (round(1.0 / v, 3) if isinstance(v, float) and v > 0 else None)
            for k, v in probabilities.items() if k.startswith("p_")}


def validate(probs: dict, game_type: str, league: str, observed_ot_rate=None) -> list[str]:
    """Contrôles imposés par le protocole."""
    flags = []
    reg_sum = probs["p_reg_home"] + probs["p_reg_draw"] + probs["p_reg_away"]
    if abs(reg_sum - 1.0) > 0.005:
        flags.append(f"REG_SUM_ERROR:{reg_sum:.4f}")
    ml_sum = probs["p_ml_home"] + probs["p_ml_away"]
    if game_type == "regular" and league == "NHL" and abs(ml_sum - 1.0) > 0.005:
        # Pas de nul final en saison régulière NHL : la somme doit faire 1.
        flags.append(f"ML_SUM_ERROR:{ml_sum:.4f}")
    if observed_ot_rate is not None:
        gap = abs(probs["p_ot"] - float(observed_ot_rate))
        if gap > 0.05:
            flags.append(f"OT_CALIBRATION:{gap:.3f}")
    return flags


def price(payload: dict, sims: int = DEFAULT_SIMS) -> dict:
    """Point d'entrée. `payload` agrège les sorties de A0 à A4."""
    params = ParamSet(payload.get("params"))
    tier = payload.get("tier", "TIER_C")
    league = payload.get("league", "?")
    game_type = payload.get("game_type", "regular")

    lam = intensities(payload["home"], payload["away"], params, tier,
                      payload.get("adjustments"))
    probs = simulate(lam["lambda_home_regulation_even"],
                     lam["lambda_away_regulation_even"],
                     payload.get("ot_rules"), game_type, params, sims)
    flags = validate(probs, game_type, league, payload.get("observed_ot_rate"))
    flags += sorted(set(params.flags))
    if payload.get("ot_rules") is None:
        flags.append("OT_RULES_UNVERIFIED")

    return {
        "agent": "A5_PRICING",
        "status": "OK",
        "flags": flags,
        "payload": {
            "league": league, "tier": tier, "game_type": game_type,
            "simulations": sims,
            "intensities": lam,
            "probabilities": probs,
            "fair_odds": fair_odds(probs),
            "parameters_used": params.used,
        },
        "generated_at_utc": now_utc(),
    }


if __name__ == "__main__":
    import argparse
    from apex_common import read_json, write_json
    parser = argparse.ArgumentParser(description="APEX-HOCKEY A5 pricing")
    parser.add_argument("--input", required=True, help="JSON agrégé A0–A4")
    parser.add_argument("--out", help="Fichier de sortie 06_h5_pricing.json")
    parser.add_argument("--sims", type=int, default=DEFAULT_SIMS)
    args = parser.parse_args()
    result = price(read_json(args.input), args.sims)
    if args.out:
        write_json(args.out, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
