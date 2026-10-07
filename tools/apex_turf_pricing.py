#!/usr/bin/env python3
"""APEX-TURF — agent A5 (PRICING).

Une course est un **classement de N partants**, pas un score entre deux
équipes : ni Poisson ni Dixon-Coles ne s'appliquent.

Chaîne de calcul :

    1. score de force par partant, à partir de performances récentes
       normalisées, pondérées de façon décroissante avec l'ancienneté ;
    2. ajustements multiplicatifs documentés (terrain, distance, corde,
       rythme, jockey, équipement, fraîcheur, classe) ;
    3. conversion en probabilités de victoire par logit multinomial
       (softmax) à température calibrée sur la discipline ;
    4. Monte-Carlo de l'ordre d'arrivée par Plackett-Luce, échantillonné
       exactement via l'astuce de Gumbel, avec issues « non classé »
       (faute d'allure au trot, chute en obstacle) ;
    5. probabilités de place selon le **nombre de places payées par
       l'opérateur pour ce nombre de partants**, qui doit être sourcé.

Sur les places, Harville et Plackett-Luce non corrigé sont **le même
modèle** : tirer les places séquentiellement proportionnellement aux forces
restantes, c'est exactement la formule de Harville. Les comparer ne dit donc
rien. Le biais documenté est empirique : ce modèle surestime la probabilité
de place des favoris et sous-estime celle des outsiders, parce qu'un cheval
battu pour la victoire ne conserve pas le même avantage relatif sur la fin
de parcours.

La correction appliquée est celle de Lo et Bacon-Shone : au tirage de la
k-ième place, les forces sont élevées à une puissance λ_k inférieure à 1,
ce qui aplatit la hiérarchie pour les places sans toucher à la victoire.
λ_1 vaut 1 par construction. À λ_2 = λ_3 = 1 le modèle redevient exactement
Harville, ce qui sert de contrôle.
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
except ImportError:
    np = None

DEFAULT_SIMS = 50_000
MIN_PROB_FLAG = 0.005
MAX_PROB_FLAG = 0.80
PROB_SUM_TOLERANCE = 0.005
PLACE_SUM_TOLERANCE = 0.01


class ParamSet:
    """Paramètres sourcés. Toute retombée sur un défaut est tracée."""

    def __init__(self, raw: dict | None):
        self.raw = raw or {}
        self.flags: list[str] = []
        self.used: dict[str, dict] = {}

    def get(self, key: str, default=None, required: bool = False):
        entry = self.raw.get(key)
        if isinstance(entry, dict) and entry.get("value") is not None:
            if not entry.get("source"):
                self.flags.append(f"UNSOURCED_PARAM:{key}")
            self.used[key] = {"value": entry["value"], "source": entry.get("source")}
            return entry["value"]
        if isinstance(entry, (int, float)):
            self.flags.append(f"UNSOURCED_PARAM:{key}")
            self.used[key] = {"value": entry, "source": None}
            return entry
        if required:
            # Règle du protocole : les règles de l'opérateur ne se supposent pas.
            self.flags.append(f"MISSING_REQUIRED_PARAM:{key}")
            self.used[key] = {"value": None, "source": None}
            return None
        self.flags.append(f"FALLBACK_PARAM:{key}")
        self.used[key] = {"value": default, "source": "défaut APEX-TURF (non sourcé)"}
        return default


# --------------------------------------------------------------------------
# Forces
# --------------------------------------------------------------------------

def strength_of(runner: dict, params: ParamSet) -> tuple[float, dict]:
    """Score de force brut, avant ajustements.

    Si l'amont fournit `strength_score`, on le prend. Sinon on le construit
    à partir des performances récentes : chaque ligne apporte une valeur
    normalisée, pondérée par un facteur décroissant avec l'ancienneté.
    """
    if runner.get("strength_score") is not None:
        return float(runner["strength_score"]), {"origin": "fourni par l'amont"}

    perfs = runner.get("recent_performances") or []
    if not perfs:
        return 0.0, {"origin": "aucune performance exploitable", "missing": True}

    decay = float(params.get("recency_decay", 0.80))
    total_w, total_v = 0.0, 0.0
    detail = []
    for i, perf in enumerate(perfs):
        value = perf.get("normalized_value")
        if value is None:
            continue
        weight = decay ** i
        total_w += weight
        total_v += weight * float(value)
        detail.append({"index": i, "weight": round(weight, 4),
                       "normalized_value": value, "source_url": perf.get("source_url")})
    if total_w == 0:
        return 0.0, {"origin": "performances non chiffrées", "missing": True}
    return total_v / total_w, {"origin": "moyenne pondérée des performances",
                               "lines_used": detail}


def apply_adjustments(base: float, runner_number: int,
                      adjustments: list[dict]) -> tuple[float, list[dict]]:
    score, applied = base, []
    for adj in adjustments or []:
        targets = adj.get("runner_numbers")
        if targets is not None and runner_number not in targets:
            continue
        factor = float(adj["factor"])
        score *= factor
        applied.append({"name": adj.get("name"), "factor": factor,
                        "justification": adj.get("justification"),
                        "source_url": adj.get("source_url")})
    return score, applied


def softmax(scores: list[float], temperature: float) -> list[float]:
    """Logit multinomial. La température règle la concentration du marché."""
    if temperature <= 0:
        raise ValueError("température de softmax nulle ou négative")
    scaled = [s / temperature for s in scores]
    top = max(scaled)
    exps = [math.exp(v - top) for v in scaled]
    total = sum(exps)
    return [e / total for e in exps]


# --------------------------------------------------------------------------
# Simulation de l'ordre d'arrivée
# --------------------------------------------------------------------------

def simulate_order(weights, non_finish, sims: int, places_paid: int | None,
                   lambdas: dict | None = None, seed: int = 20263) -> dict:
    """Ordre d'arrivée par Plackett-Luce, avec correction de place et non-classés.

    Chaque position est tirée séquentiellement parmi les partants encore
    disponibles : argmax de `λ_k · log(w_i) + Gumbel_i` échantillonne
    exactement une loi proportionnelle à `w_i^λ_k`. Un partant non classé
    (faute d'allure, chute, arrêt) est retiré du tirage : il ne peut ni
    gagner ni être placé.
    """
    if np is None:
        raise RuntimeError("numpy requis (déclaré dans requirements.txt)")
    rng = np.random.default_rng(seed)
    w = np.asarray(weights, dtype=float)
    n = len(w)
    log_w = np.log(np.maximum(w, 1e-12))
    lambdas = lambdas or {}

    failed = rng.random((sims, n)) < np.asarray(non_finish, dtype=float)[None, :]
    available = ~failed

    k = min(int(places_paid), n) if places_paid else 1
    picks = np.full((sims, k), -1, dtype=int)
    rows = np.arange(sims)

    for pos in range(k):
        lam = float(lambdas.get(pos + 1, 1.0))
        keys = lam * log_w[None, :] + rng.gumbel(size=(sims, n))
        keys = np.where(available, keys, -np.inf)
        chosen = np.argmax(keys, axis=1)
        # Aucun partant disponible : la position reste vide (-1).
        none_left = ~available.any(axis=1)
        picks[:, pos] = np.where(none_left, -1, chosen)
        available[rows[~none_left], chosen[~none_left]] = False

    winner = picks[:, 0]
    has_winner = winner >= 0

    p_win = np.array([np.mean((winner == i) & has_winner) for i in range(n)])
    result = {"p_win": p_win.tolist(),
              "p_void_race": float(np.mean(~has_winner)),
              "p_non_finish": list(non_finish),
              "place_lambdas": {str(i + 1): float(lambdas.get(i + 1, 1.0))
                                for i in range(k)}}

    if places_paid:
        p_place = np.array([np.mean(np.any(picks == i, axis=1)) for i in range(n)])
        result["p_place"] = p_place.tolist()
        result["places_paid"] = k
        if k >= 2:
            pairs: dict[str, int] = {}
            for a, b in picks[:, :2]:
                if a < 0 or b < 0:
                    continue
                key = "-".join(sorted((str(a + 1), str(b + 1)), key=int))
                pairs[key] = pairs.get(key, 0) + 1
            result["top_couples"] = [
                {"combination": key, "probability": round(v / sims, 5)}
                for key, v in sorted(pairs.items(), key=lambda kv: -kv[1])[:5]]
    return result


def harville_place(p_win: list[float], places: int) -> list[float]:
    """Places selon Harville analytique (équivalent de λ_k = 1 partout)."""
    n = len(p_win)
    out = [0.0] * n
    for i in range(n):
        out[i] = p_win[i]
        if places >= 2:
            for j in range(n):
                if j == i or p_win[j] >= 1:
                    continue
                out[i] += p_win[j] * p_win[i] / (1 - p_win[j])
        if places >= 3:
            for j in range(n):
                if j == i or p_win[j] >= 1:
                    continue
                for m in range(n):
                    if m in (i, j):
                        continue
                    denom = (1 - p_win[j]) * (1 - p_win[j] - p_win[m])
                    if denom <= 0:
                        continue
                    out[i] += p_win[j] * p_win[m] * p_win[i] / denom
    return out


# --------------------------------------------------------------------------
# Point d'entrée
# --------------------------------------------------------------------------

def price(payload: dict, sims: int = DEFAULT_SIMS) -> dict:
    params = ParamSet(payload.get("params"))
    runners = [r for r in payload["runners"] if r.get("status", "PARTANT") == "PARTANT"]
    if len(runners) < 2:
        return {"agent": "A5_PRICING", "status": "ABORT",
                "flags": ["FIELD_TOO_SMALL"], "payload": {},
                "generated_at_utc": now_utc()}

    temperature = params.get("softmax_temperature", 1.0)
    scores, breakdown = [], []
    for r in runners:
        base, detail = strength_of(r, params)
        adjusted, applied = apply_adjustments(base, r["number"], payload.get("adjustments"))
        scores.append(adjusted)
        breakdown.append({"number": r["number"], "name": r.get("name"),
                          "base_strength": round(base, 5),
                          "adjusted_strength": round(adjusted, 5),
                          "strength_origin": detail, "adjustments": applied})

    # Un score nul ou négatif n'a pas de sens dans un logit multiplicatif :
    # on translate l'échelle plutôt que de produire des poids aberrants.
    floor = min(scores)
    if floor <= 0:
        shift = abs(floor) + 1.0
        scores = [s + shift for s in scores]
        params.flags.append(f"STRENGTH_SHIFTED:{round(shift, 4)}")

    probs = softmax(scores, temperature)
    non_finish = [float(r.get("non_finish_prob") or 0.0) for r in runners]

    places_paid = params.get("places_paid", required=True)
    # Correction Lo / Bacon-Shone. À 1.0 le modèle redevient Harville pur :
    # c'est un défaut explicite, signalé, pas un choix silencieux.
    lambdas = {2: float(params.get("place_lambda_2", 1.0)),
               3: float(params.get("place_lambda_3", 1.0))}
    sim = simulate_order(probs, non_finish, sims, places_paid, lambdas)

    p_win = sim["p_win"]
    fair_win = [round(1 / p, 3) if p > 0 else None for p in p_win]
    p_place = sim.get("p_place")
    fair_place = [round(1 / p, 3) if p > 0 else None for p in (p_place or [])]

    # --- contrôles du protocole -----------------------------------------
    flags = []
    total_win = sum(p_win)
    if abs(total_win + sim["p_void_race"] - 1.0) > PROB_SUM_TOLERANCE:
        flags.append(f"WIN_SUM_ERROR:{total_win + sim['p_void_race']:.4f}")
    if abs(total_win - 1.0) > PROB_SUM_TOLERANCE:
        # Écart normal dès que des partants peuvent ne pas être classés :
        # la masse manquante est la probabilité de course sans vainqueur.
        flags.append(f"NON_FINISH_MASS:{1 - total_win:.4f}")
    if p_place is not None and places_paid:
        total_place = sum(p_place)
        if abs(total_place - min(int(places_paid), len(runners))) > (
                PLACE_SUM_TOLERANCE + sum(non_finish)):
            flags.append(f"PLACE_SUM_ERROR:{total_place:.4f}")
    for i, p in enumerate(p_win):
        if p < MIN_PROB_FLAG or p > MAX_PROB_FLAG:
            flags.append(f"EXTREME_PROB:{runners[i]['number']}:{p:.4f}")
    if places_paid is None:
        flags.append("PLACES_PAID_UNSOURCED")

    harville = None
    if p_place is not None and places_paid:
        k = min(int(places_paid), len(runners))
        h = harville_place(p_win, k)
        idx = max(range(len(p_win)), key=lambda i: p_win[i])
        corrected = any(abs(v - 1.0) > 1e-9 for v in lambdas.values())
        harville = {
            "favourite_number": runners[idx]["number"],
            "model_p_place": round(p_place[idx], 4),
            "harville_uncorrected_p_place": round(h[idx], 4),
            "delta": round(h[idx] - p_place[idx], 4),
            "place_lambdas": {str(key): value for key, value in lambdas.items()},
            "correction_active": corrected,
            "note": ("Harville et Plackett-Luce non corrigé sont le même modèle : "
                     "sans correction l'écart n'est que du bruit Monte-Carlo. "
                     "Fournir place_lambda_2 / place_lambda_3 calibrés (< 1) pour "
                     "corriger la surestimation empirique des places du favori."
                     if not corrected else
                     "Écart entre le modèle corrigé et Harville non corrigé. "
                     "Contrôle seulement, jamais décisionnel."),
        }
    if not any(abs(float(v) - 1.0) > 1e-9 for v in lambdas.values()):
        flags.append("PLACE_CORRECTION_INACTIVE")

    return {
        "agent": "A5_PRICING",
        "status": "OK",
        "flags": flags + sorted(set(params.flags)),
        "payload": {
            "field_size_at_pricing": len(runners),
            "simulations": sims,
            "places_paid": sim.get("places_paid"),
            "p_void_race": round(sim["p_void_race"], 5),
            "runners": [
                {"number": runners[i]["number"], "name": runners[i].get("name"),
                 "p_win": round(p_win[i], 5), "fair_odds_win": fair_win[i],
                 "p_place": round(p_place[i], 5) if p_place else None,
                 "fair_odds_place": fair_place[i] if p_place else None,
                 "p_non_finish": round(non_finish[i], 5)}
                for i in range(len(runners))],
            "strength_breakdown": breakdown,
            "top_couples": sim.get("top_couples"),
            "harville_control": harville,
            "parameters_used": params.used,
        },
        "generated_at_utc": now_utc(),
    }


if __name__ == "__main__":
    import argparse
    from apex_common import read_json, write_json
    parser = argparse.ArgumentParser(description="APEX-TURF A5 pricing")
    parser.add_argument("--input", required=True)
    parser.add_argument("--out")
    parser.add_argument("--sims", type=int, default=DEFAULT_SIMS)
    args = parser.parse_args()
    result = price(read_json(args.input), args.sims)
    if args.out:
        write_json(args.out, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
