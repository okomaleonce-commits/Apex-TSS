"""
APEX-TSS — Walk-Forward Backtesting Engine
===========================================
Objectives:
  1. Valider le ROI global du TSS
  2. Calibrer les gates (DCS, EV seuil)
  3. Identifier les marchés profitables

Architecture:
  WalkForwardSplitter  → time-based train/test windows (by season)
  DixonColesModel      → fit on train, generate P_réelle on test
  OddsSimulator        → synthetic book odds (P_modèle * margin)
  TSSSignalEngine      → Gate-0 / Gate-1 / Gate-2 / Kelly
  BacktestRunner       → iterate all test matches, record signals & PnL
"""

import json
import logging
import warnings
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import poisson
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Tuple, Optional
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [BACKTEST] %(message)s")
log = logging.getLogger("backtest_engine")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. WALK-FORWARD SPLITTER
# ═══════════════════════════════════════════════════════════════════════════════

class WalkForwardSplitter:
    """
    Season-based walk-forward splits.
    Example with 4 seasons [S1, S2, S3, S4]:
      Fold 1: train=[S1,S2]  test=[S3]
      Fold 2: train=[S1,S2,S3]  test=[S4]
    """

    def __init__(self, min_train_seasons: int = 2):
        self.min_train = min_train_seasons

    def split(self, df: pd.DataFrame) -> List[Tuple[pd.DataFrame, pd.DataFrame, str]]:
        seasons = sorted(df["season"].unique())
        if len(seasons) < self.min_train + 1:
            raise ValueError(
                f"Need ≥ {self.min_train+1} seasons, got {len(seasons)}: {seasons}"
            )

        folds = []
        for i in range(self.min_train, len(seasons)):
            train_seasons = seasons[:i]
            test_season   = seasons[i]
            train_df = df[df["season"].isin(train_seasons)].copy()
            test_df  = df[df["season"] == test_season].copy()
            label    = f"train={'+'.join(train_seasons)} | test={test_season}"
            folds.append((train_df, test_df, label))
            log.info(f"Fold: {label}  "
                     f"train={len(train_df)} matches  test={len(test_df)} matches")

        return folds


# ═══════════════════════════════════════════════════════════════════════════════
# 2. DIXON-COLES MODEL
# ═══════════════════════════════════════════════════════════════════════════════

class DixonColesModel:
    """
    Full Dixon-Coles (1997) with:
      - Attack/Defense parameters per team
      - Home advantage (γ)
      - Low-score correction (ρ)
      - Exponential time weighting (ξ)
    """

    def __init__(self, xi: float = 0.0065):
        self.xi = xi   # time-decay rate
        self.params: Dict = {}
        self.teams: List[str] = []
        self.fitted: bool = False

    # ── rho correction ────────────────────────────────────────────────────────
    @staticmethod
    def _rho_correction(x, y, lam, mu, rho):
        if x == 0 and y == 0: return 1 - lam * mu * rho
        if x == 0 and y == 1: return 1 + lam * rho
        if x == 1 and y == 0: return 1 + mu * rho
        if x == 1 and y == 1: return 1 - rho
        return 1.0

    def _log_likelihood(self, params_vec, df, team_idx, ref_team):
        n_teams = len(self.teams)
        # unpack
        attack  = dict(zip(self.teams, params_vec[:n_teams]))
        defense = dict(zip(self.teams, params_vec[n_teams:2*n_teams]))
        gamma   = params_vec[2*n_teams]
        rho     = params_vec[2*n_teams + 1]

        # fix reference team for identifiability
        attack[ref_team]  = 1.0
        defense[ref_team] = 1.0

        ll = 0.0
        for _, row in df.iterrows():
            h, a = row["home"], row["away"]
            if h not in attack or a not in attack:
                continue
            lam = attack[h]  * defense[a] * gamma
            mu  = attack[a]  * defense[h]
            hg, ag = int(row["home_goals"]), int(row["away_goals"])

            tau = self._rho_correction(hg, ag, lam, mu, rho)
            if tau <= 0:
                tau = 1e-9

            ll += (
                row.get("weight", 1.0) * (
                    np.log(tau)
                    + poisson.logpmf(hg, lam)
                    + poisson.logpmf(ag, mu)
                )
            )
        return -ll  # minimise

    def fit(self, df: pd.DataFrame, reference_date: pd.Timestamp = None):
        df = df.copy()
        df = df.dropna(subset=["home_goals", "away_goals"])
        df["home_goals"] = df["home_goals"].astype(int)
        df["away_goals"] = df["away_goals"].astype(int)

        # Time weighting
        if reference_date is None:
            reference_date = df["date"].max()
        df["days_ago"] = (reference_date - df["date"]).dt.days.clip(lower=0)
        df["weight"]   = np.exp(-self.xi * df["days_ago"])

        self.teams = sorted(
            set(df["home"].tolist() + df["away"].tolist())
        )
        n = len(self.teams)
        ref = self.teams[0]

        # Initial params: attack=1, defense=1, gamma=1.35, rho=-0.1
        x0 = np.ones(2*n + 2)
        x0[2*n]     = 1.35   # home advantage
        x0[2*n + 1] = -0.1   # rho

        bounds = (
            [(0.1, 5.0)] * n    # attack
          + [(0.1, 5.0)] * n    # defense
          + [(1.0, 2.0)]        # gamma
          + [(-0.5, 0.5)]       # rho
        )

        res = minimize(
            self._log_likelihood,
            x0,
            args=(df, self.teams, ref),
            method="L-BFGS-B",
            bounds=bounds,
            options={"maxiter": 500, "ftol": 1e-8}
        )

        n_t = len(self.teams)
        self.params = {
            "attack":  dict(zip(self.teams, res.x[:n_t])),
            "defense": dict(zip(self.teams, res.x[n_t:2*n_t])),
            "gamma":   res.x[2*n_t],
            "rho":     res.x[2*n_t + 1],
        }
        self.params["attack"][ref]  = 1.0
        self.params["defense"][ref] = 1.0
        self.fitted = True
        log.info(f"Dixon-Coles fitted on {len(self.teams)} teams, "
                 f"γ={self.params['gamma']:.3f}, ρ={self.params['rho']:.3f}")

    def predict_probs(self, home: str, away: str, max_goals: int = 8) -> Dict:
        """Returns dict: {H, D, A, btts_yes, btts_no, over25, under25, over35, under35}"""
        if not self.fitted:
            raise RuntimeError("Model not fitted.")

        # Fallback for unknown teams → league average
        atk = self.params["attack"]
        dfn = self.params["defense"]
        avg_atk = np.mean(list(atk.values()))
        avg_def = np.mean(list(dfn.values()))

        lam = atk.get(home, avg_atk) * dfn.get(away, avg_def) * self.params["gamma"]
        mu  = atk.get(away, avg_atk) * dfn.get(home, avg_def)

        # Score matrix
        matrix = np.zeros((max_goals+1, max_goals+1))
        for i in range(max_goals+1):
            for j in range(max_goals+1):
                tau = self._rho_correction(i, j, lam, mu, self.params["rho"])
                matrix[i][j] = tau * poisson.pmf(i, lam) * poisson.pmf(j, mu)

        # Normalise
        matrix /= matrix.sum()

        p_home = float(np.tril(matrix, -1).sum())
        p_draw = float(np.diag(matrix).sum())
        p_away = float(np.triu(matrix, 1).sum())

        p_btts_yes = float(matrix[1:, 1:].sum())
        p_btts_no  = 1.0 - p_btts_yes

        total_goals = np.array([
            [i+j for j in range(max_goals+1)]
            for i in range(max_goals+1)
        ])
        p_over25  = float(matrix[total_goals >  2.5].sum())
        p_under25 = float(matrix[total_goals <= 2.5].sum())
        p_over35  = float(matrix[total_goals >  3.5].sum())
        p_under35 = float(matrix[total_goals <= 3.5].sum())

        return {
            "xg_home": lam, "xg_away": mu,
            "H": p_home, "D": p_draw, "A": p_away,
            "btts_yes": p_btts_yes, "btts_no": p_btts_no,
            "over2.5": p_over25, "under2.5": p_under25,
            "over3.5": p_over35, "under3.5": p_under35,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. ODDS SIMULATOR (synthetic book odds)
# ═══════════════════════════════════════════════════════════════════════════════

class OddsSimulator:
    """
    Generates synthetic book odds from true probabilities by adding a margin.
    Two modes:
      'proportional' — classical margin distribution
      'shin'         — Shin (1993) method (TSS default)
    """

    def __init__(self, margin: float = 0.055, method: str = "shin"):
        self.margin = margin
        self.method = method

    def _shin_demarg(self, odds_list: List[float]) -> List[float]:
        """Shin method: solve for z (insider prob), then extract true probs."""
        raw_probs = [1/o for o in odds_list]
        overround = sum(raw_probs)
        # Shin's z approximation
        n = len(raw_probs)
        z = (overround - 1) / (overround * (n - 1) / n + overround - 1 + 1e-9)
        z = max(0.001, min(z, 0.5))
        true_probs = [
            (np.sqrt(z**2 + 4*(1-z)*p**2/overround) - z) / (2*(1-z))
            for p in raw_probs
        ]
        return true_probs

    def simulate_odds(self, true_probs: Dict) -> Dict:
        """
        true_probs: dict from DixonColesModel.predict_probs()
        Returns: dict of simulated book odds for each market
        """
        def add_margin_proportional(probs):
            total = sum(probs)
            margined = [p * (1 + self.margin) / total for p in probs]
            return [1/p for p in margined]

        def add_margin_shin(probs):
            # Invert shin: inflate probs, convert to odds
            total = sum(probs) * (1 + self.margin)
            inflated = [p * total / sum(probs) for p in probs]
            return [max(1.01, 1/p) for p in inflated]

        fn = add_margin_proportional if self.method == "proportional" else add_margin_shin

        h, d, a = true_probs["H"], true_probs["D"], true_probs["A"]
        o_1x2  = fn([h, d, a])

        by  = true_probs["btts_yes"]
        bn  = true_probs["btts_no"]
        o_btts = fn([by, bn])

        ov = true_probs["over2.5"]
        un = true_probs["under2.5"]
        o_ou25 = fn([ov, un])

        ov3 = true_probs["over3.5"]
        un3 = true_probs["under3.5"]
        o_ou35 = fn([ov3, un3])

        return {
            "odds_H":        round(o_1x2[0], 3),
            "odds_D":        round(o_1x2[1], 3),
            "odds_A":        round(o_1x2[2], 3),
            "odds_btts_yes": round(o_btts[0], 3),
            "odds_btts_no":  round(o_btts[1], 3),
            "odds_over2.5":  round(o_ou25[0], 3),
            "odds_under2.5": round(o_ou25[1], 3),
            "odds_over3.5":  round(o_ou35[0], 3),
            "odds_under3.5": round(o_ou35[1], 3),
        }

    def demarginalize(self, odds_dict: Dict) -> Dict:
        """Shin demarg → extract true probs from book odds."""
        keys_1x2  = ["odds_H", "odds_D", "odds_A"]
        keys_btts = ["odds_btts_yes", "odds_btts_no"]
        keys_ou25 = ["odds_over2.5", "odds_under2.5"]
        keys_ou35 = ["odds_over3.5", "odds_under3.5"]

        def safe_demarg(keys):
            odds = [odds_dict.get(k, 2.0) for k in keys]
            return self._shin_demarg(odds)

        p1x2  = safe_demarg(keys_1x2)
        pbtts = safe_demarg(keys_btts)
        pou25 = safe_demarg(keys_ou25)
        pou35 = safe_demarg(keys_ou35)

        return {
            "P_H": p1x2[0], "P_D": p1x2[1], "P_A": p1x2[2],
            "P_btts_yes": pbtts[0], "P_btts_no": pbtts[1],
            "P_over2.5": pou25[0], "P_under2.5": pou25[1],
            "P_over3.5": pou35[0], "P_under3.5": pou35[1],
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. TSS SIGNAL ENGINE (gates + Kelly)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    match_id:   str
    date:       str
    home:       str
    away:       str
    league:     str
    season:     str
    market:     str
    p_synth:    float   # from model (Dixon-Coles)
    p_book:     float   # demarginalized from book odds
    odds:       float
    ev:         float
    edge:       float   # p_synth - p_book
    kelly_frac: float
    stake_pct:  float   # final stake % of bankroll
    decision:   str     # BET / NO BET
    reason:     str     # gate that triggered NO BET or BET confirmed
    # resolved post-match
    outcome:    str = ""    # WIN / LOSS / PUSH
    actual_result: str = "" # H / D / A / over / under / btts_yes / btts_no
    pnl_units:  float = 0.0

    def resolve(self, match_result: str, home_goals: int, away_goals: int):
        """Fill in outcome and PnL after match is played."""
        self.actual_result = self._market_result(match_result, home_goals, away_goals)
        if self.decision != "BET":
            self.outcome  = "NO BET"
            self.pnl_units = 0.0
            return

        won = self._is_win(self.actual_result)
        self.outcome  = "WIN" if won else "LOSS"
        self.pnl_units = (self.odds - 1) * self.stake_pct if won else -self.stake_pct

    def _market_result(self, result: str, hg: int, ag: int) -> str:
        total = hg + ag
        if self.market in ("H", "D", "A"):
            return result
        if self.market == "over2.5":  return "over2.5"  if total > 2.5  else "under2.5"
        if self.market == "under2.5": return "under2.5" if total <= 2.5 else "over2.5"
        if self.market == "over3.5":  return "over3.5"  if total > 3.5  else "under3.5"
        if self.market == "under3.5": return "under3.5" if total <= 3.5 else "over3.5"
        if self.market == "btts_yes": return "btts_yes" if (hg > 0 and ag > 0) else "btts_no"
        if self.market == "btts_no":  return "btts_no"  if not (hg > 0 and ag > 0) else "btts_yes"
        return ""

    def _is_win(self, actual: str) -> bool:
        return actual == self.market


class TSSSignalEngine:
    """
    Gate architecture:
      Gate-0: EV > EV_min (default 3%)
      Gate-1: edge = p_synth - p_book > EDGE_min (default 5%)
      Gate-2: odds in [ODDS_MIN, ODDS_MAX]
      Kelly:  fractional Kelly → stake % of TSS bankroll
    """

    MARKETS = ["H", "D", "A", "btts_yes", "btts_no",
               "over2.5", "under2.5", "over3.5", "under3.5"]

    # Market-specific moratoriums (APEX-ENGINE rules)
    MORATORIUM: Dict[str, List[str]] = {
        # Defensive markets under heavy audit moratorium
        "under2.5": [],   # allowed everywhere
        "btts_no":  [],   # allowed everywhere
        # No restrictions by default — add per league below
    }

    def __init__(self, config: Dict):
        self.ev_min      = config.get("ev_min",      0.03)
        self.edge_min    = config.get("edge_min",     0.05)
        self.odds_min    = config.get("odds_min",     1.40)
        self.odds_max    = config.get("odds_max",     4.50)
        self.kelly_frac  = config.get("kelly_fraction", 0.25)
        self.max_stake   = config.get("max_stake_pct",  0.03)
        self.min_stake   = config.get("min_stake_pct",  0.005)
        self.dcs_min     = config.get("dcs_min",      0.60)

    def _compute_ev(self, p: float, odds: float) -> float:
        return p * odds - 1.0

    def _compute_dcs(self, probs: Dict, odds_dict: Dict) -> float:
        """
        Simplified DCS: ratio of markets with sufficient data confidence.
        In live mode this includes lineup data, xG availability, etc.
        Here we proxy: DCS = 1.0 if xG available, else 0.75.
        """
        has_xg = probs.get("xg_home", 0) > 0
        return 1.0 if has_xg else 0.75

    def _kelly_stake(self, p: float, odds: float) -> float:
        b = odds - 1.0
        if b <= 0 or p <= 0:
            return 0.0
        k = (b * p - (1 - p)) / b
        k = max(0.0, k * self.kelly_frac)
        return round(min(k, self.max_stake), 4)

    def analyze_match(
        self,
        match_id: str, date: str, home: str, away: str,
        league: str, season: str,
        probs: Dict,      # Dixon-Coles true probs
        odds_dict: Dict,  # simulated book odds
        p_book_dict: Dict # demarginalized book probs
    ) -> List[Signal]:
        signals = []
        dcs = self._compute_dcs(probs, odds_dict)

        for market in self.MARKETS:
            p_synth = probs.get(market, None)
            odds    = odds_dict.get(f"odds_{market}", None)
            p_book  = p_book_dict.get(f"P_{market}", None)

            if p_synth is None or odds is None or p_book is None:
                continue

            ev   = self._compute_ev(p_synth, odds)
            edge = p_synth - p_book

            # ── Gate-0: DCS ──────────────────────────────────────────────────
            if dcs < self.dcs_min:
                decision = "NO BET"
                reason   = f"Gate-0 DCS={dcs:.2f} < {self.dcs_min}"
                kelly    = 0.0
                stake    = 0.0
            # ── Gate-1: EV ───────────────────────────────────────────────────
            elif ev < self.ev_min:
                decision = "NO BET"
                reason   = f"Gate-1 EV={ev:.3f} < {self.ev_min}"
                kelly    = 0.0
                stake    = 0.0
            # ── Gate-2: Edge ─────────────────────────────────────────────────
            elif edge < self.edge_min:
                decision = "NO BET"
                reason   = f"Gate-2 edge={edge:.3f} < {self.edge_min}"
                kelly    = 0.0
                stake    = 0.0
            # ── Gate-3: Odds range ───────────────────────────────────────────
            elif not (self.odds_min <= odds <= self.odds_max):
                decision = "NO BET"
                reason   = f"Gate-3 odds={odds:.2f} out of [{self.odds_min},{self.odds_max}]"
                kelly    = 0.0
                stake    = 0.0
            else:
                decision = "BET"
                reason   = "All gates passed"
                kelly    = self._kelly_stake(p_synth, odds)
                stake    = max(self.min_stake, kelly)

            signals.append(Signal(
                match_id=match_id, date=date, home=home, away=away,
                league=league, season=season, market=market,
                p_synth=round(p_synth, 4), p_book=round(p_book, 4),
                odds=round(odds, 3), ev=round(ev, 4), edge=round(edge, 4),
                kelly_frac=round(kelly, 4), stake_pct=round(stake, 4),
                decision=decision, reason=reason
            ))

        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# 5. BACKTEST RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

class BacktestRunner:
    def __init__(self, config: Dict = None):
        self.config     = config or self._default_config()
        self.splitter   = WalkForwardSplitter(min_train_seasons=2)
        self.odds_sim   = OddsSimulator(
            margin=self.config.get("book_margin", 0.055),
            method=self.config.get("demarg_method", "shin")
        )
        self.engine     = TSSSignalEngine(self.config)
        self.all_signals: List[Signal] = []

    @staticmethod
    def _default_config() -> Dict:
        return {
            "ev_min":         0.03,
            "edge_min":       0.05,
            "odds_min":       1.40,
            "odds_max":       4.50,
            "kelly_fraction": 0.25,
            "max_stake_pct":  0.030,
            "min_stake_pct":  0.005,
            "dcs_min":        0.60,
            "book_margin":    0.055,
            "demarg_method":  "shin",
            "xi":             0.0065,
        }

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        folds = self.splitter.split(df)
        self.all_signals = []

        for fold_idx, (train_df, test_df, label) in enumerate(folds, 1):
            log.info(f"\n{'='*60}")
            log.info(f"FOLD {fold_idx}: {label}")
            log.info(f"{'='*60}")

            # Fit one model per league (to avoid cross-league noise)
            leagues = test_df["league"].unique()

            for league in leagues:
                train_lg = train_df[train_df["league"] == league].copy()
                test_lg  = test_df[test_df["league"]  == league].copy()

                if len(train_lg) < 50:
                    log.warning(f"Skipping {league}: only {len(train_lg)} train matches")
                    continue

                log.info(f"\n── {league}: fitting DC on {len(train_lg)} matches …")
                model = DixonColesModel(xi=self.config["xi"])
                try:
                    model.fit(train_lg, reference_date=test_lg["date"].min())
                except Exception as e:
                    log.error(f"DC fit failed for {league}: {e}")
                    continue

                signals = self._process_test_matches(model, test_lg, league)
                self.all_signals.extend(signals)
                log.info(f"  → {len(signals)} signals | "
                         f"BET={sum(1 for s in signals if s.decision=='BET')}")

        return self._to_dataframe()

    def _process_test_matches(
        self, model: DixonColesModel, test_df: pd.DataFrame, league: str
    ) -> List[Signal]:
        signals = []
        for _, row in test_df.iterrows():
            home, away = row["home"], row["away"]
            try:
                probs    = model.predict_probs(home, away)
            except Exception as e:
                log.debug(f"Predict error {home} vs {away}: {e}")
                continue

            odds_dict  = self.odds_sim.simulate_odds(probs)
            p_book     = self.odds_sim.demarginalize(odds_dict)

            match_signals = self.engine.analyze_match(
                match_id=row["match_id"],
                date=str(row["date"].date()),
                home=home, away=away,
                league=league, season=row["season"],
                probs=probs,
                odds_dict=odds_dict,
                p_book_dict=p_book
            )

            # Resolve against actual result
            for sig in match_signals:
                sig.resolve(
                    match_result=row["result"],
                    home_goals=int(row["home_goals"]),
                    away_goals=int(row["away_goals"])
                )
            signals.extend(match_signals)

        return signals

    def _to_dataframe(self) -> pd.DataFrame:
        if not self.all_signals:
            return pd.DataFrame()
        return pd.DataFrame([asdict(s) for s in self.all_signals])


# ═══════════════════════════════════════════════════════════════════════════════
# 6. GATE CALIBRATION SEARCH
# ═══════════════════════════════════════════════════════════════════════════════

class GateCalibrator:
    """
    Grid search over (ev_min, edge_min) to find optimal gate thresholds
    maximising ROI while maintaining minimum bet volume.
    """

    def calibrate(
        self,
        df: pd.DataFrame,
        ev_range:   np.ndarray = None,
        edge_range: np.ndarray = None,
        min_bets:   int = 30
    ) -> pd.DataFrame:
        ev_range   = ev_range   or np.arange(0.01, 0.12, 0.01)
        edge_range = edge_range or np.arange(0.02, 0.15, 0.01)

        bet_df = df[df["decision"] == "BET"].copy()
        if bet_df.empty:
            return pd.DataFrame()

        results = []
        for ev_t in ev_range:
            for ed_t in edge_range:
                mask  = (bet_df["ev"] >= ev_t) & (bet_df["edge"] >= ed_t)
                sub   = bet_df[mask]
                if len(sub) < min_bets:
                    continue
                roi   = sub["pnl_units"].sum() / sub["stake_pct"].sum()
                yield_ = sub["pnl_units"].sum()
                results.append({
                    "ev_min":    round(ev_t, 3),
                    "edge_min":  round(ed_t, 3),
                    "n_bets":    len(sub),
                    "roi":       round(roi, 4),
                    "total_pnl": round(yield_, 4),
                    "win_rate":  round((sub["outcome"] == "WIN").mean(), 3),
                })

        res_df = pd.DataFrame(results).sort_values("roi", ascending=False)
        return res_df


# ═══════════════════════════════════════════════════════════════════════════════
# QUICK SMOKE TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Generate synthetic data to validate pipeline without real scraping
    np.random.seed(42)
    teams = [f"Team_{i}" for i in range(18)]
    seasons = ["2021-2022", "2022-2023", "2023-2024"]
    rows = []
    import hashlib
    from itertools import permutations

    for season in seasons:
        pairs = list(permutations(teams, 2))[:300]
        for i, (h, a) in enumerate(pairs):
            hg = np.random.poisson(1.4)
            ag = np.random.poisson(1.1)
            res = "H" if hg > ag else ("A" if ag > hg else "D")
            mid = hashlib.md5(f"{season}{i}{h}{a}".encode()).hexdigest()[:12]
            rows.append({
                "match_id": mid, "league": "EPL", "season": season,
                "date": pd.Timestamp(f"{season[:4]}-09-01") + pd.Timedelta(days=i),
                "home": h, "away": a,
                "home_goals": hg, "away_goals": ag,
                "xg_home": round(hg + np.random.normal(0, 0.3), 2),
                "xg_away": round(ag + np.random.normal(0, 0.3), 2),
                "result": res
            })

    df = pd.DataFrame(rows)
    print(f"Synthetic dataset: {len(df)} matches")

    runner = BacktestRunner()
    results = runner.run(df)
    print(f"\nSignals generated: {len(results)}")
    if not results.empty:
        bets = results[results["decision"] == "BET"]
        print(f"BETs: {len(bets)}")
        if not bets.empty:
            roi = bets["pnl_units"].sum() / bets["stake_pct"].sum()
            print(f"ROI: {roi:.2%}")
            print(bets[["market","ev","edge","odds","outcome","pnl_units"]].head(15))
