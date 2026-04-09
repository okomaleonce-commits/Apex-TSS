"""
TSS LAYER 1 — DÉMARGINALISATION ENGINE
=======================================
Conversion cotes brutes → probabilités nettes via méthode Shin.
Méthode Shin : plus précise que la proportionnelle sur marchés sportifs.
Préserve la structure relative des probabilités, corrige le biais outsider.
"""

import logging
import math
from typing import Optional

logger = logging.getLogger("TSS.Layer1")


class ShinDemarginaliser:
    """
    Méthode Shin (1993) de démarginalisation.

    Formule :
        p_i = 1 / cote_i
        overround = Σ p_i
        z = (overround - 1) / (overround * N - 1)
        P_i = (p_i - z) / (1 - z * N)

    z représente la vig implicite redistribuée équitablement.
    """

    def demarginalise(self, odds: dict[str, float]) -> dict[str, float]:
        """
        Prend un dict {outcome: cote} et retourne {outcome: probabilité nette}.
        Lève ValueError si les cotes sont invalides.
        """
        if not odds:
            raise ValueError("Aucune cote fournie.")

        n = len(odds)
        p_raw = {}
        for key, cote in odds.items():
            if cote <= 1.0:
                raise ValueError(f"Cote invalide pour '{key}': {cote} (doit être > 1.0)")
            p_raw[key] = 1.0 / cote

        overround = sum(p_raw.values())

        if overround <= 1.0:
            # Marché sous-rondi (rare) — retourner les p_raw normalisées
            logger.warning(f"Overround ≤ 1.0 ({overround:.4f}), normalisation simple appliquée.")
            total = overround
            return {k: v / total for k, v in p_raw.items()}

        # Calcul Shin z
        z = (overround - 1.0) / (overround * n - 1.0)

        result = {}
        for key, pi in p_raw.items():
            p_net = (pi - z) / (1.0 - z * n)
            if p_net < 0:
                p_net = 0.0
                logger.warning(f"Probabilité négative pour '{key}', fixée à 0.")
            result[key] = p_net

        # Normalisation finale (correction arrondi flottant)
        total = sum(result.values())
        if total > 0:
            result = {k: v / total for k, v in result.items()}

        logger.debug(f"Démarginalisé (Shin) | overround={overround:.4f} z={z:.4f} | {result}")
        return result

    def demarginalise_market(self, market_odds: "MarketOdds") -> dict[str, float]:  # type: ignore
        return self.demarginalise(market_odds.outcomes)

    def overround(self, odds: dict[str, float]) -> float:
        return sum(1.0 / c for c in odds.values() if c > 1.0)

    def vig_pct(self, odds: dict[str, float]) -> float:
        """Vig en pourcentage (overround - 1)."""
        return (self.overround(odds) - 1.0) * 100.0


class ProportionalDemarginaliser:
    """
    Méthode proportionnelle classique — fournie pour comparaison.
    Moins précise que Shin sur les outsiders.
    """

    def demarginalise(self, odds: dict[str, float]) -> dict[str, float]:
        p_raw = {k: 1.0 / v for k, v in odds.items() if v > 1.0}
        total = sum(p_raw.values())
        return {k: v / total for k, v in p_raw.items()}


class DemarginalisationEngine:
    """
    Point d'entrée principal.
    Utilise Shin par défaut, bascule sur proportionnel si Shin échoue.
    """

    def __init__(self, method: str = "shin"):
        self.method = method
        self._shin = ShinDemarginaliser()
        self._prop = ProportionalDemarginaliser()

    def run(self, odds: dict[str, float]) -> dict[str, float]:
        try:
            if self.method == "shin":
                return self._shin.demarginalise(odds)
            else:
                return self._prop.demarginalise(odds)
        except Exception as e:
            logger.error(f"Démarginalisation Shin échouée ({e}), fallback proportionnel.")
            return self._prop.demarginalise(odds)

    def run_snapshot_market(self, snapshot: "MatchSnapshot", market_key: str) -> Optional[dict[str, float]]:  # type: ignore
        """Démarginalisie un marché spécifique d'un snapshot."""
        if market_key not in snapshot.markets:
            logger.warning(f"Marché '{market_key}' absent du snapshot {snapshot.match_id}")
            return None
        return self.run(snapshot.markets[market_key].outcomes)

    # Marchés dont la structure n'est pas un simple dict {outcome: cote}
    NON_ODDS_MARKETS = {"ah"}

    def run_all_markets(self, snapshot: "MatchSnapshot") -> dict[str, dict[str, float]]:  # type: ignore
        """Démarginalisie tous les marchés d'un snapshot en une passe."""
        result = {}
        for mkt_key, mkt_odds in snapshot.markets.items():
            if mkt_key in self.NON_ODDS_MARKETS:
                # Conserver brut — structure spéciale (ex: home_line, home_odds, away_odds)
                result[mkt_key] = mkt_odds.outcomes
                continue
            try:
                result[mkt_key] = self.run(mkt_odds.outcomes)
            except Exception as e:
                logger.error(f"Erreur démarginalisation marché '{mkt_key}': {e}")
        return result


# ─────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────

def implied_probability(cote: float) -> float:
    """Probabilité brute depuis une cote décimale."""
    return 1.0 / cote if cote > 1.0 else 0.0


def fair_cote(probability: float) -> float:
    """Cote juste (sans marge) depuis une probabilité."""
    return 1.0 / probability if probability > 0 else float("inf")


def extract_ah_implied_line(ah_market: dict) -> tuple[float, float, float]:
    """
    Extrait ligne AH, P(home couvre), P(away couvre) depuis le marché AH.
    Format attendu : {"home_line": -0.5, "home_odds": 2.08, "away_odds": 1.85}
    """
    line = ah_market.get("home_line", 0.0)
    home_odds = ah_market.get("home_odds", 2.0)
    away_odds = ah_market.get("away_odds", 2.0)

    engine = DemarginalisationEngine()
    probs = engine.run({"home": home_odds, "away": away_odds})

    return line, probs.get("home", 0.5), probs.get("away", 0.5)
