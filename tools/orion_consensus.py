#!/usr/bin/env python3
"""ORION SUPERBRAIN — cœur d'arbitrage (consensus, désaccord, META anti-corrélation).

Ce module est la partie MÉCANIQUE et testable du « cerveau collectif » : il transforme les avis de
plusieurs agents en UNE décision, en appliquant les principes qui séparent un cerveau d'un vote naïf :

  1. INDÉPENDANCE (META) : des agents qui s'appuient sur la MÊME source ne sont pas des voix
     indépendantes. On les regroupe par `source` et on les fond en une seule voix avant tout calcul —
     sinon 5 agents citant une même mauvaise donnée créent une illusion de consensus 5 contre 1.
  2. DÉSACCORD = INCERTITUDE : la confiance finale est le consensus PÉNALISÉ par la dispersion des
     voix indépendantes. Fort désaccord ⇒ confiance basse ⇒ tendance à l'abstention.
  3. AUTORITÉ DE NE RIEN FAIRE : la décision peut être ATTENDRE ou COLLECTER (pas assez de voix
     indépendantes), pas seulement ACCEPTER/REJETER.
  4. MÉMOIRE D'ÉCHEC : une correction de biais mesurée a posteriori (ex. « ce profil surestime de
     11 % ») s'applique AVANT l'arbitrage.

ORION n'émet JAMAIS seul un pari réel : tant que le gel de promotion est actif (comme tout APEX), une
conclusion « ACCEPTER » est rétrogradée en « ATTENDRE (gel) ». C'est un outil de recherche.
"""
from __future__ import annotations

import math
import statistics as st

# Seuils par défaut (fraction de probabilité). Volontairement prudents.
ACCEPT = 0.62          # confiance finale requise pour ACCEPTER
REJECT = 0.42          # sous ce seuil → REJETER
MIN_INDEP_VOICES = 3   # moins de 3 voix INDÉPENDANTES → COLLECTER (pas assez d'information)
HIGH_DISAGREEMENT = 0.14   # écart-type des voix indépendantes au-delà → incertitude forte

# Le gel de promotion d'APEX vaut aussi pour ORION : recherche uniquement, aucune mise réelle émise.
PROMOTION_FROZEN = True


def _finite(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def independent_voices(votes):
    """META : regroupe les votes par `source` et fond chaque groupe en UNE voix (moyenne pondérée).
    Renvoie (voices, groupes) où voices = [{p, weight, sources, agents, n_agents}]. Un vote sans
    source est traité comme sa propre source (indépendant)."""
    groups = {}
    for v in votes:
        p = _finite(v.get("p"))
        if p is None or not (0.0 <= p <= 1.0):
            continue
        w = _finite(v.get("weight"))
        w = 1.0 if (w is None or w <= 0) else w
        src = v.get("source") or f"__solo__:{v.get('agent')}"
        g = groups.setdefault(src, {"p_sum": 0.0, "w_sum": 0.0, "agents": [], "source": src})
        g["p_sum"] += p * w
        g["w_sum"] += w
        g["agents"].append(v.get("agent"))
    voices = []
    for src, g in groups.items():
        if g["w_sum"] <= 0:
            continue
        # Une SOURCE = une voix indépendante de poids 1, quel que soit le nombre d'agents qui la
        # citent (c'est tout l'intérêt de META : empêcher une source de peser plus par duplication).
        # Les poids internes ne servent qu'à moyenner le p à l'intérieur du groupe.
        voices.append({"p": g["p_sum"] / g["w_sum"], "weight": 1.0,
                       "source": src, "agents": g["agents"], "n_agents": len(g["agents"])})
    return voices, groups


def arbitrate(votes, *, accept=ACCEPT, reject=REJECT, min_voices=MIN_INDEP_VOICES,
              high_disagreement=HIGH_DISAGREEMENT, failure_bias=0.0, frozen=None):
    """Arbitre les votes en une décision ORION. `failure_bias` (mémoire d'échec) est AJOUTÉ au
    consensus brut (ex. -0.11 si le profil surestime historiquement). Renvoie un dict structuré."""
    frozen = PROMOTION_FROZEN if frozen is None else frozen
    voices, _ = independent_voices(votes)
    n_indep = len(voices)
    n_votes = sum(1 for v in votes if _finite(v.get("p")) is not None)
    if n_indep == 0:
        return {"decision": "COLLECTER", "raison": "aucune voix exploitable",
                "consensus": None, "confiance": None, "desaccord": None,
                "n_votes": n_votes, "n_independantes": 0}

    ps = [v["p"] for v in voices]
    ws = [v["weight"] for v in voices]
    consensus = sum(p * w for p, w in zip(ps, ws)) / sum(ws)
    consensus = max(0.0, min(1.0, consensus + failure_bias))     # mémoire d'échec appliquée
    desaccord = st.pstdev(ps) if n_indep > 1 else 0.0

    # Confiance = consensus pénalisé par la dispersion des voix INDÉPENDANTES. La pénalité pousse la
    # confiance vers 0.5 (l'ignorance) à mesure que le désaccord monte — jamais au-delà du consensus.
    penalite = min(1.0, desaccord / high_disagreement)           # 0 (accord) → 1 (désaccord fort)
    confiance = consensus - (consensus - 0.5) * 0.5 * penalite   # au pire : mi-chemin vers 0.5
    confiance = max(0.0, min(1.0, confiance))

    # Décision : l'autorité de NE RIEN FAIRE est première.
    if n_indep < min_voices:
        decision, raison = "COLLECTER", f"seulement {n_indep} voix indépendantes (< {min_voices})"
    elif desaccord >= high_disagreement:
        decision, raison = "ATTENDRE", f"désaccord interne élevé (σ={desaccord:.3f}) → incertitude forte"
    elif confiance >= accept:
        decision, raison = "ACCEPTER", f"confiance {confiance:.3f} ≥ {accept}"
    elif confiance <= reject:
        decision, raison = "REJETER", f"confiance {confiance:.3f} ≤ {reject}"
    else:
        decision, raison = "ATTENDRE", f"confiance {confiance:.3f} en zone grise"

    gele = False
    if decision == "ACCEPTER" and frozen:
        decision, raison, gele = "ATTENDRE", raison + " — mais GEL actif : aucune mise réelle émise", True

    return {"decision": decision, "raison": raison,
            "consensus": round(consensus, 4), "confiance": round(confiance, 4),
            "desaccord": round(desaccord, 4), "penalite_desaccord": round(penalite, 3),
            "n_votes": n_votes, "n_independantes": n_indep, "gel_actif": gele,
            "voix": [{"source": v["source"], "p": round(v["p"], 4), "poids": round(v["weight"], 2),
                      "agents": v["agents"]} for v in sorted(voices, key=lambda v: -v["weight"])]}


def _demo():
    votes = [
        {"agent": "FORECAST", "p": 0.82, "source": "stats"},
        {"agent": "PATTERN", "p": 0.79, "source": "stats"},      # même source que FORECAST → corrélé
        {"agent": "CONTEXT", "p": 0.55, "source": "news"},
        {"agent": "SKEPTIC", "p": 0.48, "source": "critique"},
        {"agent": "MARKET", "p": 0.61, "source": "marche"},
    ]
    import json
    print(json.dumps(arbitrate(votes), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    _demo()
