"""
APEX-TSS Walk-Forward Backtesting Engine v1.0
Architecture:
  - Splits temporels glissants (train_weeks + test_weeks)
  - Pour chaque match test: TSS simulé → BET/NO BET → outcome enregistré
  - Staking Kelly fractionné sur bankroll dédiée
  - Logs complets par match dans SQLite audit
"""

import os
import math
import json
import logging
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple, Any
from scipy.stats import poisson

logger = logging.getLogger("APEX-TSS.WalkForward")

AUDIT_DB = os.path.join(os.path.dirname(__file__), "..", "data", "backtest_audit.db")

# ─── Audit DB ──────────────────────────────────────────────────────────────

def init_audit_db():
    os.makedirs(os.path.dirname(AUDIT_DB), exist_ok=True)
    conn = sqlite3.connect(AUDIT_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            league TEXT,
            season TEXT,
            date TEXT,
            home TEXT,
            away TEXT,
            market TEXT,
            selection TEXT,
            odds REAL,
            p_model REAL,
            p_no_vig REAL,
            ev REAL,
            dcs REAL,
            signal TEXT,
            outcome TEXT,
            stake REAL,
            pnl REAL,
            bankroll_after REAL,
            window_start TEXT,
            window_end TEXT,
            notes TEXT
        )
    """)
    conn.commit()
    conn.close()

def log_bet(run_id: str, row: dict):
    conn = sqlite3.connect(AUDIT_DB)
    conn.execute("""
        INSERT INTO backtest_results
        (run_id, league, season, date, home, away, market, selection, odds,
         p_model, p_no_vig, ev, dcs, signal, outcome, stake, pnl,
         bankroll_after, window_start, window_end, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        run_id, row.get("league"), row.get("season"),
        str(row.get("date", "")), row.get("home"), row.get("away"),
        row.get("market"), row.get("selection"), row.get("odds"),
        row.get("p_model"), row.get("p_no_vig"), row.get("ev"),
        row.get("dcs"), row.get("signal"), row.get("outcome"),
        row.get("stake"), row.get("pnl"), row.get("bankroll_after"),
        str(row.get("window_start", "")), str(row.get("window_end", "")),
        row.get("notes", "")
    ))
    conn.commit()
    conn.close()


# ─── TSS Simulation Layer ───────────────────────────────────────────────────
# (Autonome — ne dépend pas des modules TSS en production pour le backtest)

def shin_demarginalize(odds_list: List[float]) -> List[float]:
    """Méthode Shin simplifiée pour retirer la marge bookmaker."""
    if not odds_list or any(o <= 1.0 for o in odds_list):
        return [1.0 / o for o in odds_list]
    raw_p = [1.0 / o for o in odds_list]
    total = sum(raw_p)
    margin = total - 1.0
    # Shin: z ≈ margin / (2*total - margin), ajustement itératif simplifié
    z = margin / (2 * total - margin) if total > 0 else 0
    corrected = [(p - z * p * (1 - p)) / (1 - z * sum(rp*(1-rp) for rp in raw_p))
                 for p in raw_p]
    s = sum(corrected)
    return [c / s for c in corrected] if s > 0 else raw_p


def poisson_probs(lambda_h: float, lambda_a: float, max_goals: int = 7) -> Dict:
    """Probabilités Poisson pour 1X2, BTTS, O/U 2.5."""
    p_matrix = np.zeros((max_goals + 1, max_goals + 1))
    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            p_matrix[i, j] = poisson.pmf(i, lambda_h) * poisson.pmf(j, lambda_a)

    p_home_win = float(np.sum(np.tril(p_matrix, -1)))
    p_draw     = float(np.trace(p_matrix))
    p_away_win = float(np.sum(np.triu(p_matrix, 1)))
    p_over25   = float(np.sum(p_matrix[i, j] for i in range(max_goals+1)
                              for j in range(max_goals+1) if i+j > 2))
    p_btts_yes = float(np.sum(p_matrix[i, j] for i in range(1, max_goals+1)
                              for j in range(1, max_goals+1)))
    return {
        "p_home": p_home_win, "p_draw": p_draw, "p_away": p_away_win,
        "p_over25": p_over25, "p_under25": 1.0 - p_over25,
        "p_btts_yes": p_btts_yes, "p_btts_no": 1.0 - p_btts_yes,
    }


def estimate_lambdas_from_history(
    df_train: pd.DataFrame, home: str, away: str
) -> Tuple[float, float]:
    """
    Estime λ_home et λ_away à partir de l'historique de la saison d'entraînement.
    Fallback sur moyennes de ligue si <5 matchs disponibles.
    """
    league_avg_h = df_train["goals_home"].mean() if not df_train.empty else 1.35
    league_avg_a = df_train["goals_away"].mean() if not df_train.empty else 1.10

    home_attack = df_train[df_train["home"] == home]["goals_home"].mean()
    home_defense = df_train[df_train["away"] == home]["goals_away"].mean()
    away_attack = df_train[df_train["away"] == away]["goals_away"].mean()
    away_defense = df_train[df_train["home"] == away]["goals_home"].mean()

    # Fallback NaN → moyenne ligue
    home_attack = home_attack if not np.isnan(home_attack) else league_avg_h
    home_defense = home_defense if not np.isnan(home_defense) else league_avg_a
    away_attack = away_attack if not np.isnan(away_attack) else league_avg_a
    away_defense = away_defense if not np.isnan(away_defense) else league_avg_h

    home_strength = (home_attack / league_avg_h) * (away_defense / league_avg_h)
    away_strength = (away_attack / league_avg_a) * (home_defense / league_avg_a)

    lambda_h = max(0.3, home_strength * league_avg_h * 1.10)   # +10% avantage domicile
    lambda_a = max(0.3, away_strength * league_avg_a)

    return lambda_h, lambda_a


def compute_dcs(df_train: pd.DataFrame, home: str, away: str) -> float:
    """
    DCS simplifié: pénalise les équipes avec peu d'historique.
    Score: 0.0 → 1.0
    """
    n_home = len(df_train[(df_train["home"] == home) | (df_train["away"] == home)])
    n_away = len(df_train[(df_train["home"] == away) | (df_train["away"] == away)])
    # 10 matchs minimum pour DCS = 1.0
    dcs = min(1.0, (n_home / 10) * 0.5 + (n_away / 10) * 0.5)
    return round(dcs, 3)


def evaluate_markets(
    row: pd.Series,
    df_train: pd.DataFrame,
    config: dict
) -> List[Dict]:
    """
    Évalue tous les marchés disponibles pour un match.
    Retourne une liste de signaux (BET/NO BET) par marché.
    """
    results = []
    dcs_min = config.get("dcs_threshold", 0.5)
    ev_min = config.get("ev_threshold", 0.03)
    kelly_fraction = config.get("kelly_fraction", 0.25)
    bankroll = config.get("bankroll", 1000.0)

    # λ estimation
    lambda_h, lambda_a = estimate_lambdas_from_history(df_train, row["home"], row["away"])
    probs = poisson_probs(lambda_h, lambda_a)
    dcs = compute_dcs(df_train, row["home"], row["away"])

    def analyze_market(market: str, selection: str, p_model: float, odds_col: str):
        if odds_col not in row or pd.isna(row.get(odds_col)):
            return None
        odds = float(row[odds_col])
        if odds <= 1.01:
            return None

        # Demarginalize 1X2
        if market == "1X2":
            h_odds = row.get("odds_h_b365", None)
            d_odds = row.get("odds_d_b365", None)
            a_odds = row.get("odds_a_b365", None)
            if h_odds and d_odds and a_odds:
                demar = shin_demarginalize([float(h_odds), float(d_odds), float(a_odds)])
                p_no_vig = demar[{"H": 0, "D": 1, "A": 2}[selection]]
            else:
                p_no_vig = 1.0 / odds
        else:
            o1 = row.get(odds_col)
            o2_map = {
                "BTTS_YES": "odds_btts_no_b365",
                "BTTS_NO": "odds_btts_yes_b365",
                "OVER25": "odds_under25_b365",
                "UNDER25": "odds_over25_b365",
            }
            o2_col = o2_map.get(market)
            o2 = row.get(o2_col) if o2_col else None
            if o2 and not pd.isna(o2):
                demar = shin_demarginalize([float(o1), float(o2)])
                p_no_vig = demar[0]
            else:
                p_no_vig = 1.0 / odds

        ev = (p_model * odds) - 1.0
        gate_dcs = dcs >= dcs_min
        gate_ev = ev >= ev_min

        # Signal
        if gate_dcs and gate_ev:
            # Kelly stake
            b = odds - 1.0
            kelly = (p_model * b - (1 - p_model)) / b
            kelly_capped = max(0, min(kelly * kelly_fraction, config.get("max_stake_pct", 0.05)))
            stake = round(bankroll * kelly_capped, 2)
            signal = "BET" if stake > 0 else "NO BET (Kelly=0)"
        else:
            stake = 0.0
            reasons = []
            if not gate_dcs:
                reasons.append(f"DCS={dcs:.2f}<{dcs_min}")
            if not gate_ev:
                reasons.append(f"EV={ev:.3f}<{ev_min}")
            signal = f"NO BET ({', '.join(reasons)})"

        return {
            "market": market, "selection": selection,
            "odds": odds, "p_model": round(p_model, 4),
            "p_no_vig": round(p_no_vig, 4), "ev": round(ev, 4),
            "dcs": dcs, "signal": signal, "stake": stake,
        }

    # Marchés 1X2
    for sel, p_col, o_col in [
        ("H", "p_home", "odds_h_b365"),
        ("D", "p_draw", "odds_d_b365"),
        ("A", "p_away", "odds_a_b365"),
    ]:
        r = analyze_market("1X2", sel, probs[p_col], o_col)
        if r:
            results.append(r)

    # BTTS
    r = analyze_market("BTTS_YES", "BTTS_YES", probs["p_btts_yes"], "odds_btts_yes_b365")
    if r: results.append(r)
    r = analyze_market("BTTS_NO", "BTTS_NO", probs["p_btts_no"], "odds_btts_no_b365")
    if r: results.append(r)

    # O/U 2.5
    r = analyze_market("OVER25", "OVER25", probs["p_over25"], "odds_over25_b365")
    if r: results.append(r)
    r = analyze_market("UNDER25", "UNDER25", probs["p_under25"], "odds_under25_b365")
    if r: results.append(r)

    return results


# ─── Walk-Forward Splitter ─────────────────────────────────────────────────

def generate_wf_windows(
    df: pd.DataFrame,
    train_weeks: int = 12,
    test_weeks: int = 4,
    step_weeks: int = 2,
) -> List[Tuple[pd.DataFrame, pd.DataFrame, datetime, datetime]]:
    """
    Génère les fenêtres walk-forward.
    Retourne: [(df_train, df_test, test_start, test_end), ...]
    """
    if "date" not in df.columns or df["date"].isna().all():
        logger.error("[WF] Colonne 'date' manquante ou vide")
        return []

    df = df.sort_values("date").dropna(subset=["date"])
    min_date = df["date"].min()
    max_date = df["date"].max()

    windows = []
    train_start = min_date
    train_end = train_start + timedelta(weeks=train_weeks)
    test_end = train_end + timedelta(weeks=test_weeks)

    while test_end <= max_date:
        df_train = df[df["date"] < train_end]
        df_test = df[(df["date"] >= train_end) & (df["date"] < test_end)]

        if len(df_train) >= 10 and len(df_test) >= 1:
            windows.append((df_train, df_test, train_end, test_end))

        train_end += timedelta(weeks=step_weeks)
        test_end += timedelta(weeks=step_weeks)

    logger.info(f"[WF] {len(windows)} fenêtres générées ({train_weeks}W train / {test_weeks}W test)")
    return windows


def determine_outcome(row: pd.Series, market: str, selection: str) -> str:
    """Détermine si le pari est gagnant selon le résultat réel."""
    gh = int(row.get("goals_home", -1))
    ga = int(row.get("goals_away", -1))
    if gh < 0 or ga < 0:
        return "UNKNOWN"

    actual_result = "H" if gh > ga else ("A" if ga > gh else "D")
    actual_btts = gh > 0 and ga > 0
    actual_total = gh + ga

    if market == "1X2":
        return "WIN" if actual_result == selection else "LOSE"
    elif market == "BTTS_YES":
        return "WIN" if actual_btts else "LOSE"
    elif market == "BTTS_NO":
        return "WIN" if not actual_btts else "LOSE"
    elif market == "OVER25":
        return "WIN" if actual_total > 2 else "LOSE"
    elif market == "UNDER25":
        return "WIN" if actual_total <= 2 else "LOSE"
    else:
        return "UNKNOWN"


# ─── Main Walk-Forward Runner ───────────────────────────────────────────────

def run_walk_forward(
    df: pd.DataFrame,
    config: dict,
    run_id: str,
    train_weeks: int = 12,
    test_weeks: int = 4,
    step_weeks: int = 2,
) -> pd.DataFrame:
    """
    Lance le backtest walk-forward complet.
    Retourne un DataFrame de tous les paris enregistrés.
    """
    init_audit_db()
    windows = generate_wf_windows(df, train_weeks, test_weeks, step_weeks)

    all_bets = []
    bankroll = config.get("bankroll", 1000.0)
    config_curr = {**config, "bankroll": bankroll}

    for df_train, df_test, w_start, w_end in windows:
        for _, match_row in df_test.iterrows():
            if match_row.get("odds_status") != "OK":
                continue   # Skip matches sans cotes

            signals = evaluate_markets(match_row, df_train, config_curr)

            for sig in signals:
                if "BET" not in sig["signal"] or sig["stake"] == 0:
                    # Log NO BET aussi pour analyse gate calibration
                    outcome = determine_outcome(match_row, sig["market"], sig["selection"])
                    record = {
                        **sig,
                        "league": match_row.get("league"),
                        "season": match_row.get("season"),
                        "date": match_row.get("date"),
                        "home": match_row.get("home"),
                        "away": match_row.get("away"),
                        "outcome": outcome,
                        "pnl": 0.0,
                        "bankroll_after": bankroll,
                        "window_start": w_start,
                        "window_end": w_end,
                    }
                    all_bets.append(record)
                    log_bet(run_id, record)
                    continue

                # BET actif
                outcome = determine_outcome(match_row, sig["market"], sig["selection"])
                if outcome == "WIN":
                    pnl = sig["stake"] * (sig["odds"] - 1.0)
                elif outcome == "LOSE":
                    pnl = -sig["stake"]
                else:
                    pnl = 0.0

                bankroll += pnl
                config_curr["bankroll"] = bankroll

                record = {
                    **sig,
                    "league": match_row.get("league"),
                    "season": match_row.get("season"),
                    "date": match_row.get("date"),
                    "home": match_row.get("home"),
                    "away": match_row.get("away"),
                    "outcome": outcome,
                    "pnl": round(pnl, 2),
                    "bankroll_after": round(bankroll, 2),
                    "window_start": w_start,
                    "window_end": w_end,
                }
                all_bets.append(record)
                log_bet(run_id, record)

    result_df = pd.DataFrame(all_bets)
    logger.info(f"[WF RUN {run_id}] Terminé. {len(result_df)} entrées. Bankroll finale: {bankroll:.2f}")
    return result_df
