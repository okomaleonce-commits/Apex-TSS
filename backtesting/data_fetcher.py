"""
APEX-TSS Backtesting — Data Fetcher v1.0
Sources:
  - FBref (match stats, scores, xG)
  - football-data.co.uk (historical odds: 1X2, BTTS, O/U, AH)
Cache: SQLite local pour éviter les re-scrapes
"""

import os
import time
import logging
import sqlite3
import hashlib
import json
import requests
import pandas as pd
from io import StringIO
from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta
from backtesting.league_registry import (
    LEAGUE_REGISTRY, get_fdco_url, get_fbref_schedule_url
)

logger = logging.getLogger("APEX-TSS.DataFetcher")

# ─── Cache SQLite ───────────────────────────────────────────────────────────

CACHE_DB = os.path.join(os.path.dirname(__file__), "..", "data", "cache.db")

def init_cache():
    os.makedirs(os.path.dirname(CACHE_DB), exist_ok=True)
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS http_cache (
            url_hash TEXT PRIMARY KEY,
            url TEXT,
            content TEXT,
            fetched_at TEXT,
            ttl_hours INTEGER
        )
    """)
    conn.commit()
    conn.close()

def cache_get(url: str, ttl_hours: int = 168) -> Optional[str]:
    """Retourne le contenu si en cache et non expiré (défaut: 7 jours)."""
    init_cache()
    h = hashlib.md5(url.encode()).hexdigest()
    conn = sqlite3.connect(CACHE_DB)
    row = conn.execute(
        "SELECT content, fetched_at, ttl_hours FROM http_cache WHERE url_hash=?", (h,)
    ).fetchone()
    conn.close()
    if row:
        fetched = datetime.fromisoformat(row[1])
        if datetime.utcnow() < fetched + timedelta(hours=row[2]):
            return row[0]
    return None

def cache_set(url: str, content: str, ttl_hours: int = 168):
    init_cache()
    h = hashlib.md5(url.encode()).hexdigest()
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""
        INSERT OR REPLACE INTO http_cache (url_hash, url, content, fetched_at, ttl_hours)
        VALUES (?, ?, ?, ?, ?)
    """, (h, url, content, datetime.utcnow().isoformat(), ttl_hours))
    conn.commit()
    conn.close()

# ─── HTTP Fetcher (poli, rate-limited) ─────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "APEX-TSS-Backtesting/1.0 (research-only)",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_url(url: str, ttl_hours: int = 168, delay: float = 3.0) -> Optional[str]:
    """Fetch URL avec cache + rate-limiting poli."""
    cached = cache_get(url, ttl_hours)
    if cached:
        logger.debug(f"[CACHE HIT] {url}")
        return cached
    logger.info(f"[FETCH] {url}")
    time.sleep(delay)   # Respecter les serveurs
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        cache_set(url, r.text, ttl_hours)
        return r.text
    except Exception as e:
        logger.error(f"[FETCH ERROR] {url} → {e}")
        return None

# ─── football-data.co.uk Odds Fetcher ──────────────────────────────────────

FDCO_ODDS_COLUMNS = {
    # 1X2
    "B365H": "odds_h_b365", "B365D": "odds_d_b365", "B365A": "odds_a_b365",
    "BWH": "odds_h_bw",   "BWD": "odds_d_bw",   "BWA": "odds_a_bw",
    # O/U 2.5
    "B365>2.5": "odds_over25_b365", "B365<2.5": "odds_under25_b365",
    # BTTS
    "B365CH": "odds_btts_yes_b365", "B365CA": "odds_btts_no_b365",
    # AH
    "AHh": "ah_handicap", "B365AHH": "odds_ah_h_b365", "B365AHA": "odds_ah_a_b365",
}

def fetch_fdco_odds(league_key: str, season: str) -> Optional[pd.DataFrame]:
    """
    Retourne un DataFrame avec cotes historiques de football-data.co.uk.
    season: '2021-22', '2022-23', etc.
    """
    league = LEAGUE_REGISTRY.get(league_key)
    if not league or not league.get("fdco_available"):
        logger.warning(f"[FDCO] {league_key}: ODDS_MISSING — pas de cotes disponibles")
        return None

    # Convertir saison: '2021-22' → '2122'
    parts = season.split("-")
    s_code = parts[0][2:] + parts[1][2:] if len(parts) == 2 else season[:2] + season[4:6]
    url = f"https://www.football-data.co.uk/mmz4281/{s_code}/{league['fdco_code']}.csv"

    content = fetch_url(url, ttl_hours=168*4, delay=2.0)
    if not content:
        return None

    try:
        df = pd.read_csv(StringIO(content), encoding="utf-8", on_bad_lines="skip")
    except Exception:
        try:
            df = pd.read_csv(StringIO(content), encoding="latin-1", on_bad_lines="skip")
        except Exception as e:
            logger.error(f"[FDCO PARSE] {league_key} {season}: {e}")
            return None

    # Normaliser les colonnes
    rename = {k: v for k, v in FDCO_ODDS_COLUMNS.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Normaliser la date
    for fmt in ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"]:
        try:
            df["date"] = pd.to_datetime(df["Date"], format=fmt)
            break
        except Exception:
            pass

    # Colonnes essentielles
    essential = ["HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR"]
    missing = [c for c in essential if c not in df.columns]
    if missing:
        logger.error(f"[FDCO] Colonnes manquantes: {missing}")
        return None

    df = df.rename(columns={
        "HomeTeam": "home", "AwayTeam": "away",
        "FTHG": "goals_home", "FTAG": "goals_away", "FTR": "result"
    })
    df["league"] = league_key
    df["season"] = season

    logger.info(f"[FDCO] {league_key} {season}: {len(df)} matches chargés")
    return df


# ─── FBref Stats Fetcher ────────────────────────────────────────────────────

FBREF_STAT_TYPES = ["shooting", "passing", "defense", "keeper", "misc"]

def fetch_fbref_schedule(league_key: str, season: str) -> Optional[pd.DataFrame]:
    """
    Scrape le calendrier FBref avec scores + xG.
    season: '2021-2022', '2022-2023', etc.
    """
    league = LEAGUE_REGISTRY.get(league_key)
    if not league:
        logger.error(f"[FBREF] Ligue inconnue: {league_key}")
        return None

    # FBref utilise format '2021-2022'
    fbref_season = season.replace("-", "-20") if len(season) == 7 else season
    # '2021-22' → '2021-2022'
    if "-" in season and len(season) == 7:
        y1, y2_short = season.split("-")
        y2 = y1[:2] + y2_short
        fbref_season = f"{y1}-{y2}"

    url = get_fbref_schedule_url(league["fbref_id"], league["fbref_slug"], fbref_season)

    html = fetch_url(url, ttl_hours=168*4, delay=4.0)
    if not html:
        return None

    try:
        tables = pd.read_html(StringIO(html))
    except Exception as e:
        logger.error(f"[FBREF PARSE] {league_key} {season}: {e}")
        return None

    # Le tableau de schedule est généralement le premier ou second
    df = None
    for t in tables:
        if "Home" in t.columns and "Away" in t.columns and "Score" in t.columns:
            df = t
            break
        if "Wk" in t.columns and "Home" in t.columns:
            df = t
            break

    if df is None:
        logger.error(f"[FBREF] Table schedule non trouvée pour {league_key} {season}")
        return None

    # Nettoyage
    df = df.dropna(subset=["Home", "Away"])
    df = df[df["Score"].notna() & df["Score"].str.contains("–|–|-", na=False)]

    # Parser le score
    def parse_score(s):
        for sep in ["–", "–", "-"]:
            if sep in str(s):
                parts = str(s).split(sep)
                if len(parts) == 2:
                    try:
                        return int(parts[0].strip()), int(parts[1].strip())
                    except ValueError:
                        pass
        return None, None

    df[["goals_home", "goals_away"]] = df["Score"].apply(
        lambda x: pd.Series(parse_score(x))
    )
    df = df.dropna(subset=["goals_home", "goals_away"])
    df["goals_home"] = df["goals_home"].astype(int)
    df["goals_away"] = df["goals_away"].astype(int)
    df["result"] = df.apply(
        lambda r: "H" if r.goals_home > r.goals_away
        else ("A" if r.goals_away > r.goals_home else "D"),
        axis=1
    )

    # Colonnes xG si disponibles
    if "xG" in df.columns:
        df = df.rename(columns={"xG": "xg_home"})
    if "xG.1" in df.columns:
        df = df.rename(columns={"xG.1": "xg_away"})

    # Date
    if "Date" in df.columns:
        df["date"] = pd.to_datetime(df["Date"], errors="coerce")

    df = df.rename(columns={"Home": "home", "Away": "away"})
    df["league"] = league_key
    df["season"] = season

    cols_keep = ["date", "home", "away", "goals_home", "goals_away", "result",
                 "league", "season"] + \
                [c for c in ["xg_home", "xg_away", "Wk", "Attendance", "Venue"] if c in df.columns]
    df = df[[c for c in cols_keep if c in df.columns]]

    logger.info(f"[FBREF] {league_key} {season}: {len(df)} matches avec scores")
    return df


# ─── Merger: FBref + FDCO ──────────────────────────────────────────────────

def build_match_dataset(
    league_key: str,
    season: str,
    require_odds: bool = True
) -> Optional[pd.DataFrame]:
    """
    Construit le dataset complet en fusionnant FBref (stats) + FDCO (cotes).
    Si require_odds=True et cotes indisponibles, retourne None avec flag ODDS_MISSING.
    """
    fbref_df = fetch_fbref_schedule(league_key, season)
    if fbref_df is None or fbref_df.empty:
        logger.error(f"[MERGE] FBref vide pour {league_key} {season}")
        return None

    fdco_df = fetch_fdco_odds(league_key, season)

    if fdco_df is None:
        if require_odds:
            logger.warning(f"[MERGE] {league_key} {season}: ODDS_MISSING — skipped")
            return None
        else:
            # Mode résultats seulement (pas de signal TSS possible, stats uniquement)
            fbref_df["odds_status"] = "ODDS_MISSING"
            return fbref_df

    # Fuzzy merge sur équipes + date (±3 jours)
    merged_rows = []
    for _, row in fbref_df.iterrows():
        if pd.isna(row.get("date")):
            continue
        candidates = fdco_df[
            (fdco_df["date"] >= row["date"] - timedelta(days=3)) &
            (fdco_df["date"] <= row["date"] + timedelta(days=3)) &
            (fdco_df["home"].str.lower().str[:5] == str(row["home"]).lower()[:5])
        ]
        if not candidates.empty:
            best = candidates.iloc[0]
            merged = {**row.to_dict(), **best.to_dict()}
            merged_rows.append(merged)
        else:
            # Match FBref sans odds — inclus mais flaggé
            row_d = row.to_dict()
            row_d["odds_status"] = "NO_ODDS_MATCH"
            merged_rows.append(row_d)

    if not merged_rows:
        return None

    result_df = pd.DataFrame(merged_rows)
    result_df["odds_status"] = result_df.get("odds_status", "OK").fillna("OK")
    logger.info(
        f"[MERGE] {league_key} {season}: {len(result_df)} matches fusionnés "
        f"({(result_df['odds_status']=='OK').sum()} avec cotes)"
    )
    return result_df


# ─── Batch Loader ──────────────────────────────────────────────────────────

def load_all_leagues(
    league_keys: list,
    seasons: list,
    require_odds: bool = True,
    save_parquet: bool = True
) -> pd.DataFrame:
    """
    Charge toutes les ligues + saisons et retourne un DataFrame consolidé.
    """
    frames = []
    skipped = []

    for league_key in league_keys:
        for season in seasons:
            league_info = LEAGUE_REGISTRY.get(league_key, {})
            if season not in league_info.get("seasons_available", []):
                logger.debug(f"[SKIP] {league_key} {season}: saison non disponible")
                continue

            logger.info(f"[LOAD] {league_key} | {season}")
            df = build_match_dataset(league_key, season, require_odds=require_odds)
            if df is not None and not df.empty:
                frames.append(df)
            else:
                skipped.append(f"{league_key}:{season}")

    if not frames:
        logger.error("[LOAD] Aucune donnée chargée.")
        return pd.DataFrame()

    all_df = pd.concat(frames, ignore_index=True)

    if skipped:
        logger.warning(f"[LOAD] {len(skipped)} combinaisons skippées: {skipped[:10]}")

    if save_parquet:
        out_path = os.path.join(os.path.dirname(__file__), "..", "data", "master_dataset.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        all_df.to_parquet(out_path, index=False)
        logger.info(f"[SAVE] Dataset sauvegardé: {out_path} ({len(all_df)} lignes)")

    return all_df
