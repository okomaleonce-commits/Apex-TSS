"""
TSS LAYER 4 — CALIBRATION LAYER
================================
Corrections automatiques par contexte :
  - Ligue
  - Force des équipes (AH)
  - Mouvement de ligne
  - Période de saison
  - Type de marché cible
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("TSS.Layer4")


@dataclass
class CalibrationContext:
    """Contexte d'un match pour la calibration."""
    league: str
    matchday: Optional[int]       = None   # Journée de championnat
    ah_line: Optional[float]      = None   # Ligne AH home (ex: -1.5)
    is_midweek: bool              = False  # Mercredi / Jeudi
    target_odds: float            = 2.0   # Cote du marché cible
    line_moved: bool              = False  # Mouvement H-3→H-1 détecté
    line_move_pct: float          = 0.0   # Amplitude du mouvement
    season_matchdays_total: int   = 38    # Total journées (38 pour Top 5)


# ─────────────────────────────────────────────
#  TABLE DE CALIBRATION PAR LIGUE
# ─────────────────────────────────────────────

LEAGUE_CALIBRATIONS: dict[str, dict] = {
    # Format : {marché: facteur multiplicateur sur P_synth}
    "ligue_1": {
        "over25": 0.93,     # Ligue 1 défensive
        "btts":   0.95,
        "ic_min": 0.87,     # IC minimum relevé
    },
    "serie_a": {
        "over25": 0.96,
        "btts":   0.97,
        "ic_min_midweek": 0.90,
    },
    "bundesliga": {
        "over25": 1.03,     # Bundesliga offensive
        "btts":   1.02,
    },
    "premier_league": {
        "over25": 1.01,
        "btts":   1.00,
    },
    "la_liga": {
        "over25": 0.98,
        "btts":   0.98,
    },
    "default": {
        "over25": 1.00,
        "btts":   1.00,
        "ic_min": 0.85,
    }
}


def _get_league_config(league: str) -> dict:
    key = league.lower().replace(" ", "_").replace("-", "_")
    return LEAGUE_CALIBRATIONS.get(key, LEAGUE_CALIBRATIONS["default"])


# ─────────────────────────────────────────────
#  MOTEUR DE CALIBRATION
# ─────────────────────────────────────────────

class CalibrationLayer:
    """
    Applique les corrections contextuelles sur P_synth et les seuils du Signal Engine.
    Retourne P_synth corrigé + paramètres de gate ajustés.
    """

    def apply(
        self,
        target_market: str,
        p_synth: float,
        context: CalibrationContext
    ) -> tuple[float, dict, list[str]]:
        """
        Retourne :
          - p_synth_calibré
          - gate_overrides (dict de seuils ajustés)
          - flags de calibration
        """
        flags = []
        gate_overrides = {}
        p_out = p_synth

        config = _get_league_config(context.league)

        # ── Correction ligue sur P_synth
        market_factor = config.get(target_market, 1.00)
        if market_factor != 1.00:
            p_out *= market_factor
            flags.append(f"LEAGUE_FACTOR({context.league},{target_market}={market_factor})")

        # ── Correction gros favoris (AH > ±1.5)
        if context.ah_line is not None and abs(context.ah_line) > 1.5:
            if target_market == "btts":
                p_out *= 0.88
                flags.append(f"AH_GROS_FAVORI(line={context.ah_line},×0.88)")

        # ── Marché cible à cote élevée → SDT minimum relevé
        if context.target_odds > 3.00:
            gate_overrides["SDT_MIN"] = 0.75
            flags.append("HIGH_ODDS_SDT_RAISED(0.75)")

        # ── Midweek Serie A → IC minimum relevé
        if context.is_midweek and context.league.lower() in ("serie_a", "serie a"):
            gate_overrides["IC_MIN"] = config.get("ic_min_midweek", 0.90)
            flags.append("MIDWEEK_IC_RAISED(0.90)")

        # ── IC minimum par ligue
        league_ic_min = config.get("ic_min")
        if league_ic_min and "IC_MIN" not in gate_overrides:
            gate_overrides["IC_MIN"] = league_ic_min

        # ── Mouvement de ligne
        if context.line_moved:
            if context.line_move_pct >= 0.15:
                # Mouvement critique → NO BET recommendé
                gate_overrides["FORCE_NO_BET"] = True
                flags.append(f"LINE_MOVE_CRITICAL({context.line_move_pct*100:.1f}%)")
            else:
                # Mouvement modéré → recalibration
                correction = 1.0 - (context.line_move_pct * 0.5)
                p_out *= correction
                flags.append(f"LINE_MOVE_CORRECTION(×{correction:.3f})")

        # ── Début de saison (J ≤ 5) → réduire poids Module C
        if context.matchday is not None and context.matchday <= 5:
            # Pas de modification directe ici — flag pour Layer 2
            flags.append("EARLY_SEASON_LOW_DATA")

        # Clamp
        p_out = max(min(p_out, 1.0), 0.0)

        if flags:
            logger.info(f"[CalibLayer] {target_market} | Corrections: {flags} | P: {p_synth:.4f}→{p_out:.4f}")

        return round(p_out, 4), gate_overrides, flags
