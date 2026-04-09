"""
TSS LAYER 2 — TRIANGULATION CORE
=================================
Trois modules en parallèle :
  Module A — BTTS Resolver
  Module B — Over/Under Resolver (Poisson implicite)
  Module C — Score Vector Resolver (Dixon-Coles)

Chaque module produit P_synth pour les marchés cibles.
"""

import logging
import math
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("TSS.Layer2")

# ═══════════════════════════════════════════
#  UTILITAIRES POISSON / DIXON-COLES
# ═══════════════════════════════════════════

def poisson_pmf(k: int, lam: float) -> float:
    """P(X = k) pour X ~ Poisson(λ)."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def poisson_cdf(n: int, lam: float) -> float:
    """P(X ≤ n) pour X ~ Poisson(λ)."""
    return sum(poisson_pmf(k, lam) for k in range(n + 1))


def poisson_over(n: float, lam: float) -> float:
    """P(X > n) — gère les demi-lignes."""
    n_floor = int(n)
    return 1.0 - poisson_cdf(n_floor, lam)


def solve_lambda(p_over: float, line: float, max_iter: int = 100) -> float:
    """
    Résolution numérique de λ tel que P(X > line | Poisson(λ)) = p_over.
    Bisection sur [0.01, 15].
    """
    low, high = 0.01, 15.0
    for _ in range(max_iter):
        mid = (low + high) / 2.0
        p_mid = poisson_over(line, mid)
        if abs(p_mid - p_over) < 1e-6:
            return mid
        if p_mid < p_over:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def dixon_coles_correction(h: int, a: int, rho: float = -0.13) -> float:
    """
    Facteur de correction Dixon-Coles pour les faibles scores (0-0, 1-0, 0-1, 1-1).
    rho = -0.13 (valeur standard calibrée empiriquement).
    """
    if h == 0 and a == 0:
        return 1.0 - rho
    elif h == 1 and a == 0:
        return 1.0 + rho
    elif h == 0 and a == 1:
        return 1.0 + rho
    elif h == 1 and a == 1:
        return 1.0 - rho
    return 1.0


# ═══════════════════════════════════════════
#  DATACLASS RÉSULTAT TRIANGULATION
# ═══════════════════════════════════════════

@dataclass
class TriangulationResult:
    """Résultat brut de la triangulation pour un marché cible."""
    target_market: str
    p_module_A: Optional[float] = None
    p_module_B: Optional[float] = None
    p_module_C: Optional[float] = None
    p_synth: float = 0.0
    flags: list[str] = field(default_factory=list)

    def summary(self) -> str:
        parts = [f"TARGET={self.target_market}"]
        if self.p_module_A is not None:
            parts.append(f"A={self.p_module_A:.3f}")
        if self.p_module_B is not None:
            parts.append(f"B={self.p_module_B:.3f}")
        if self.p_module_C is not None:
            parts.append(f"C={self.p_module_C:.3f}")
        parts.append(f"P_synth={self.p_synth:.3f}")
        if self.flags:
            parts.append(f"FLAGS={self.flags}")
        return " | ".join(parts)


# ═══════════════════════════════════════════
#  MODULE A — BTTS RESOLVER
# ═══════════════════════════════════════════

# Facteurs de corrélation empiriques par tranche de Base = P(H>0.5) × P(A>0.5)
BTTS_CORRELATION_TABLE = [
    (0.20, 1.08),
    (0.30, 1.21),
    (0.40, 1.33),
    (0.50, 1.29),
    (0.60, 1.31),
    (1.00, 1.22),
]

def _btts_correlation_factor(base: float) -> float:
    for threshold, factor in BTTS_CORRELATION_TABLE:
        if base <= threshold:
            return factor
    return 1.22


class ModuleA_BTTSResolver:
    """
    Estime P(BTTS Yes) depuis les marchés satellites les plus liquides :
    - P(Home > 0.5)
    - P(Away > 0.5)
    - P(Over 1.5)   [contrainte plafond]
    - AH implicite  [correction gros favoris]
    """

    def compute(self, probs: dict[str, dict[str, float]]) -> tuple[float, list[str]]:
        flags = []

        # Extraire les probabilités nécessaires
        p_home_over05 = probs.get("home_over05", {}).get("over")
        p_away_over05 = probs.get("away_over05", {}).get("over")
        p_over15      = probs.get("over15", {}).get("over")

        if p_home_over05 is None or p_away_over05 is None:
            flags.append("MISSING_HOME_AWAY_OVER05")
            logger.warning("[ModuleA] Données home_over05 / away_over05 manquantes.")
            return 0.0, flags

        # Calcul base
        base = p_home_over05 * p_away_over05
        factor = _btts_correlation_factor(base)
        p_btts = base * factor

        # Contrainte plafond : BTTS ≤ Over 1.5
        if p_over15 is not None and p_btts > p_over15:
            p_btts = p_over15 * 0.92
            flags.append("BTTS_CAPPED_BY_OVER15")

        # Correction AH gros favoris
        ah_line = self._get_ah_line(probs)
        if ah_line is not None and abs(ah_line) > 1.5:
            p_btts *= 0.88
            flags.append(f"AH_CORRECTION_APPLIED(line={ah_line})")

        p_btts = min(max(p_btts, 0.0), 1.0)
        logger.debug(f"[ModuleA] base={base:.3f} factor={factor} P(BTTS)={p_btts:.3f}")
        return p_btts, flags

    @staticmethod
    def _get_ah_line(probs: dict) -> Optional[float]:
        """Extrait la ligne AH depuis le dict de probabilités si disponible."""
        ah = probs.get("ah", {})
        return ah.get("home_line")


# ═══════════════════════════════════════════
#  MODULE B — OVER/UNDER RESOLVER
# ═══════════════════════════════════════════

class ModuleB_OverUnderResolver:
    """
    Résout λ_total via Poisson inverse depuis Over/Under 2.5.
    Vérifie la cohérence interne avec Over 1.5.
    Estime P_synth pour Over/Under et BTTS depuis λ.
    """

    CONSISTENCY_THRESHOLD = 0.05  # 5 points d'écart tolérés

    def compute(self, probs: dict[str, dict[str, float]]) -> tuple[dict[str, float], list[str]]:
        """
        Retourne un dict de P_synth par marché cible + flags.
        """
        flags = []
        results = {}

        p_over25 = probs.get("over25", {}).get("over")
        p_over15 = probs.get("over15", {}).get("over")

        if p_over25 is None:
            flags.append("MISSING_OVER25")
            logger.warning("[ModuleB] P(Over 2.5) manquant.")
            return results, flags

        # Résolution de λ_total
        lam_total = solve_lambda(p_over25, 2.5)
        logger.debug(f"[ModuleB] λ_total={lam_total:.4f} depuis P(Over 2.5)={p_over25:.3f}")

        # Vérification cohérence Over 1.5
        if p_over15 is not None:
            p_over15_synth = poisson_over(1.5, lam_total)
            ecart = abs(p_over15_synth - p_over15)
            if ecart > self.CONSISTENCY_THRESHOLD:
                flags.append(f"OVER15_INCONSISTENCY(ecart={ecart:.3f})")
                logger.warning(f"[ModuleB] Incohérence Over 1.5 : synth={p_over15_synth:.3f} réel={p_over15:.3f}")

        # Estimations Over/Under depuis λ
        results["over25"] = poisson_over(2.5, lam_total)
        results["over15"] = poisson_over(1.5, lam_total)
        results["over35"] = poisson_over(3.5, lam_total)
        results["lambda_total"] = lam_total

        # Estimation BTTS depuis λ (approximation Poisson)
        # P(BTTS Yes) via intégration sur scores où les deux ≥ 1
        # Nécessite λ_home et λ_away séparés
        p_home_over05 = probs.get("home_over05", {}).get("over")
        p_away_over05 = probs.get("away_over05", {}).get("over")

        if p_home_over05 is not None and p_away_over05 is not None:
            lam_home = solve_lambda(p_home_over05, 0.5)
            lam_away = solve_lambda(p_away_over05, 0.5)
            results["lambda_home"] = lam_home
            results["lambda_away"] = lam_away

            # P(BTTS Yes) = P(H≥1) × P(A≥1) — Poisson
            p_btts = (1 - poisson_pmf(0, lam_home)) * (1 - poisson_pmf(0, lam_away))
            results["btts"] = p_btts
            logger.debug(f"[ModuleB] λ_home={lam_home:.3f} λ_away={lam_away:.3f} P(BTTS)={p_btts:.3f}")
        else:
            flags.append("MISSING_INDIVIDUAL_TOTALS")

        return results, flags


# ═══════════════════════════════════════════
#  MODULE C — SCORE VECTOR RESOLVER
# ═══════════════════════════════════════════

class ModuleC_ScoreVectorResolver:
    """
    Génère la distribution complète des scores via Dixon-Coles.
    Agrège pour produire P_synth de BTTS, Over/Under, et identifier
    les scores sous-évalués par le bookmaker.
    """

    MAX_GOALS = 7          # Limite de calcul par équipe
    RHO = -0.13            # Facteur Dixon-Coles standard

    def compute(
        self,
        lam_home: float,
        lam_away: float,
        book_exact_scores: Optional[dict[str, float]] = None
    ) -> tuple[dict[str, float], list[str]]:
        """
        lam_home, lam_away : extraits du Module B.
        book_exact_scores  : P_réelle(Score Exact) demarginalisée {h-a: prob}.
        """
        flags = []
        score_matrix = {}

        # Construire la matrice des scores
        for h in range(self.MAX_GOALS + 1):
            for a in range(self.MAX_GOALS + 1):
                p = (poisson_pmf(h, lam_home)
                     * poisson_pmf(a, lam_away)
                     * dixon_coles_correction(h, a, self.RHO))
                score_matrix[f"{h}-{a}"] = max(p, 0.0)

        # Normaliser
        total = sum(score_matrix.values())
        if total > 0:
            score_matrix = {k: v / total for k, v in score_matrix.items()}

        # Agréger les marchés
        p_btts = sum(v for k, v in score_matrix.items()
                     if int(k.split("-")[0]) >= 1 and int(k.split("-")[1]) >= 1)
        p_over25 = sum(v for k, v in score_matrix.items()
                       if int(k.split("-")[0]) + int(k.split("-")[1]) > 2)
        p_over15 = sum(v for k, v in score_matrix.items()
                       if int(k.split("-")[0]) + int(k.split("-")[1]) > 1)

        results = {
            "btts": p_btts,
            "over25": p_over25,
            "over15": p_over15,
            "score_matrix": score_matrix
        }

        # Détection scores sous-évalués vs book
        if book_exact_scores:
            undervalued = self._find_undervalued_scores(score_matrix, book_exact_scores)
            results["undervalued_scores"] = undervalued
            if undervalued:
                flags.append(f"UNDERVALUED_SCORES_DETECTED({len(undervalued)})")

        logger.debug(f"[ModuleC] P(BTTS)={p_btts:.3f} P(Over25)={p_over25:.3f}")
        return results, flags

    @staticmethod
    def _find_undervalued_scores(
        model_probs: dict[str, float],
        book_probs: dict[str, float],
        threshold: float = 0.15
    ) -> list[dict]:
        """Identifie les scores où P_modèle > P_book × (1 + threshold)."""
        undervalued = []
        for score, p_model in model_probs.items():
            p_book = book_probs.get(score, 0)
            if p_book > 0 and p_model > p_book * (1 + threshold):
                undervalued.append({
                    "score": score,
                    "p_model": round(p_model, 4),
                    "p_book": round(p_book, 4),
                    "edge": round((p_model - p_book) / p_book * 100, 2)
                })
        return sorted(undervalued, key=lambda x: x["edge"], reverse=True)


# ═══════════════════════════════════════════
#  ORCHESTRATEUR LAYER 2
# ═══════════════════════════════════════════

class TriangulationCore:
    """
    Lance les trois modules en parallèle et agrège les résultats.
    Pondérations : A=0.40, B=0.40, C=0.20
    """

    WEIGHTS = {"A": 0.40, "B": 0.40, "C": 0.20}

    def __init__(self):
        self.module_A = ModuleA_BTTSResolver()
        self.module_B = ModuleB_OverUnderResolver()
        self.module_C = ModuleC_ScoreVectorResolver()

    def run(
        self,
        probs: dict[str, dict[str, float]],
        book_exact_scores: Optional[dict[str, float]] = None
    ) -> dict[str, TriangulationResult]:
        """
        probs : dict demarginalisé depuis Layer 1.
        Retourne un dict {marché_cible: TriangulationResult}.
        """
        all_flags = []

        # ── Module A
        p_A_btts, flags_A = self.module_A.compute(probs)
        all_flags.extend(flags_A)

        # ── Module B
        p_B_dict, flags_B = self.module_B.compute(probs)
        all_flags.extend(flags_B)
        lam_home = p_B_dict.get("lambda_home", 1.2)
        lam_away = p_B_dict.get("lambda_away", 1.0)

        # ── Module C
        p_C_dict, flags_C = self.module_C.compute(lam_home, lam_away, book_exact_scores)
        all_flags.extend(flags_C)

        results = {}

        # ── BTTS
        p_B_btts = p_B_dict.get("btts")
        p_C_btts = p_C_dict.get("btts")
        results["btts"] = self._aggregate(
            "btts",
            p_A_btts if p_A_btts > 0 else None,
            p_B_btts,
            p_C_btts,
            flags_A + flags_B + flags_C
        )

        # ── Over 2.5
        p_A_o25 = None  # Module A ne cible pas Over 2.5 directement
        p_B_o25 = p_B_dict.get("over25")
        p_C_o25 = p_C_dict.get("over25")
        results["over25"] = self._aggregate(
            "over25", p_A_o25, p_B_o25, p_C_o25, flags_B + flags_C
        )

        # ── Over 1.5
        p_B_o15 = p_B_dict.get("over15")
        p_C_o15 = p_C_dict.get("over15")
        results["over15"] = self._aggregate(
            "over15", None, p_B_o15, p_C_o15, flags_B + flags_C
        )

        # Attacher la matrice des scores au résultat Over 2.5 pour usage downstream
        if "score_matrix" in p_C_dict:
            results["score_matrix"] = p_C_dict["score_matrix"]
        if "undervalued_scores" in p_C_dict:
            results["undervalued_scores"] = p_C_dict["undervalued_scores"]

        return results

    def _aggregate(
        self,
        target: str,
        p_A: Optional[float],
        p_B: Optional[float],
        p_C: Optional[float],
        flags: list[str]
    ) -> TriangulationResult:
        """Agrégation pondérée A/B/C avec redistribution si module absent."""
        available = {}
        if p_A is not None and p_A > 0:
            available["A"] = p_A
        if p_B is not None and p_B > 0:
            available["B"] = p_B
        if p_C is not None and p_C > 0:
            available["C"] = p_C

        if not available:
            return TriangulationResult(
                target_market=target,
                p_synth=0.0,
                flags=flags + ["NO_MODULE_AVAILABLE"]
            )

        # Redistribuer les poids des modules absents
        total_weight = sum(self.WEIGHTS[k] for k in available)
        p_synth = sum(
            available[k] * (self.WEIGHTS[k] / total_weight)
            for k in available
        )

        result = TriangulationResult(
            target_market=target,
            p_module_A=p_A,
            p_module_B=p_B,
            p_module_C=p_C,
            p_synth=round(min(max(p_synth, 0.0), 1.0), 4),
            flags=flags
        )
        logger.info(f"[TriCore] {result.summary()}")
        return result
