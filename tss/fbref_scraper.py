"""
APEX-TSS — FBref Scraper Module
================================
Scrapes historical match data (results + xG) from FBref for 10 leagues.
Uses caching (SQLite) to avoid redundant requests.
"""

import time
import sqlite3
import logging
import hashlib
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SCRAPER] %(message)s")
log = logging.getLogger("fbref_scraper")

# ── League registry ──────────────────────────────────────────────────────────
LEAGUES = {
    "EPL":          {"fbref_id": 9,  "name": "Premier League",       "country": "ENG"},
    "La Liga":      {"fbref_id": 12, "name": "La Liga",               "country": "ESP"},
    "Bundesliga":   {"fbref_id": 20, "name": "Bundesliga",            "country": "GER"},
    "Serie A":      {"fbref_id": 11, "name": "Serie A",               "country": "ITA"},
    "Ligue 1":      {"fbref_id": 13, "name": "Ligue 1",               "country": "FRA"},
    "Eredivisie":   {"fbref_id": 23, "name": "Eredivisie",            "country": "NED"},
    "Belgian Pro":  {"fbref_id": 37, "name": "Belgian Pro League",    "country": "BEL"},
    "Brazil Serie A": {"fbref_id": 24, "name": "Serie A",             "country": "BRA"},
    "A-League":     {"fbref_id": 53, "name": "A-League Men",          "country": "AUS"},
    "AFC CL":       {"fbref_id": 43, "name": "AFC Champions League",  "country": "AFC"},
}

SEASONS = ["2021-2022", "2022-2023", "2023-2024", "2024-2025"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://fbref.com/",
}

RATE_LIMIT_DELAY = 4.0   # seconds between requests (FBref TOS)
CACHE_DB = Path("data/fbref_cache.db")


# ── Cache layer ───────────────────────────────────────────────────────────────
class FBrefCache:
    def __init__(self, db_path: Path = CACHE_DB):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS html_cache (
                url_hash TEXT PRIMARY KEY,
                url      TEXT,
                html     TEXT,
                fetched  TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id    TEXT PRIMARY KEY,
                league      TEXT,
                season      TEXT,
                date        TEXT,
                home        TEXT,
                away        TEXT,
                home_goals  INTEGER,
                away_goals  INTEGER,
                xg_home     REAL,
                xg_away     REAL,
                result      TEXT,   -- H / D / A
                fetched     TEXT
            )
        """)
        self.conn.commit()

    def get_html(self, url: str) -> Optional[str]:
        h = hashlib.md5(url.encode()).hexdigest()
        row = self.conn.execute(
            "SELECT html FROM html_cache WHERE url_hash=?", (h,)
        ).fetchone()
        return row[0] if row else None

    def set_html(self, url: str, html: str):
        h = hashlib.md5(url.encode()).hexdigest()
        self.conn.execute(
            "INSERT OR REPLACE INTO html_cache VALUES (?,?,?,?)",
            (h, url, html, datetime.utcnow().isoformat())
        )
        self.conn.commit()

    def upsert_match(self, row: dict):
        self.conn.execute("""
            INSERT OR REPLACE INTO matches VALUES
            (:match_id,:league,:season,:date,:home,:away,
             :home_goals,:away_goals,:xg_home,:xg_away,:result,:fetched)
        """, row)
        self.conn.commit()

    def load_matches(self, leagues: list = None, seasons: list = None) -> pd.DataFrame:
        q = "SELECT * FROM matches WHERE 1=1"
        params = []
        if leagues:
            q += f" AND league IN ({','.join(['?']*len(leagues))})"
            params += leagues
        if seasons:
            q += f" AND season IN ({','.join(['?']*len(seasons))})"
            params += seasons
        df = pd.read_sql(q, self.conn, params=params)
        df["date"] = pd.to_datetime(df["date"])
        return df


# ── FBref scraper ─────────────────────────────────────────────────────────────
class FBrefScraper:
    def __init__(self, cache: FBrefCache = None):
        self.cache = cache or FBrefCache()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch(self, url: str) -> str:
        cached = self.cache.get_html(url)
        if cached:
            log.debug(f"Cache hit: {url}")
            return cached

        log.info(f"Fetching: {url}")
        time.sleep(RATE_LIMIT_DELAY)
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        self.cache.set_html(url, r.text)
        return r.text

    def _build_url(self, league_key: str, season: str) -> str:
        lid = LEAGUES[league_key]["fbref_id"]
        s = season.replace("-", "-")
        return f"https://fbref.com/en/comps/{lid}/{s}/schedule/"

    def scrape_league_season(self, league_key: str, season: str) -> pd.DataFrame:
        url = self._build_url(league_key, season)
        try:
            html = self._fetch(url)
        except Exception as e:
            log.error(f"Failed to fetch {league_key} {season}: {e}")
            return pd.DataFrame()

        try:
            tables = pd.read_html(html, flavor="lxml")
        except Exception as e:
            log.error(f"HTML parse error for {league_key} {season}: {e}")
            return pd.DataFrame()

        # FBref schedule table is always the first large table
        df = None
        for t in tables:
            if "Home" in t.columns and "Away" in t.columns:
                df = t
                break

        if df is None:
            log.warning(f"No schedule table found for {league_key} {season}")
            return pd.DataFrame()

        return self._clean_schedule(df, league_key, season)

    def _clean_schedule(self, df: pd.DataFrame, league_key: str, season: str) -> pd.DataFrame:
        # Standardise column names (FBref can vary slightly by season)
        df.columns = [str(c).strip() for c in df.columns]

        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if "date" in cl:        col_map[c] = "date"
            elif "home" in cl and "xg" not in cl: col_map[c] = "home"
            elif "away" in cl and "xg" not in cl: col_map[c] = "away"
            elif "score" in cl:     col_map[c] = "score"
            elif cl == "xg":        col_map[c] = "xg_home"
            elif cl == "xg.1":      col_map[c] = "xg_away"
        df = df.rename(columns=col_map)

        needed = {"date", "home", "away", "score"}
        if not needed.issubset(df.columns):
            log.warning(f"Missing columns in {league_key} {season}: {needed - set(df.columns)}")
            return pd.DataFrame()

        # Drop non-match rows (headers repeated, future fixtures)
        df = df[df["score"].notna() & df["score"].str.contains("–|−|-", na=False)]
        df = df[df["date"].notna()].copy()

        # Parse score
        def parse_score(s):
            for sep in ["–", "−", "-"]:
                if sep in str(s):
                    parts = str(s).split(sep)
                    if len(parts) == 2:
                        try:
                            return int(parts[0].strip()), int(parts[1].strip())
                        except ValueError:
                            pass
            return None, None

        scores = df["score"].apply(parse_score)
        df["home_goals"] = scores.apply(lambda x: x[0])
        df["away_goals"] = scores.apply(lambda x: x[1])
        df = df[df["home_goals"].notna()].copy()

        # xG columns (optional — not all leagues have them)
        if "xg_home" not in df.columns: df["xg_home"] = None
        if "xg_away" not in df.columns: df["xg_away"] = None

        # Result
        def result(row):
            if row["home_goals"] > row["away_goals"]:  return "H"
            if row["home_goals"] < row["away_goals"]:  return "A"
            return "D"

        df["result"]  = df.apply(result, axis=1)
        df["league"]  = league_key
        df["season"]  = season
        df["date"]    = pd.to_datetime(df["date"], errors="coerce")
        df            = df.dropna(subset=["date"])

        # Stable match_id
        def make_id(row):
            raw = f"{row['date'].date()}_{row['home']}_{row['away']}_{league_key}"
            return hashlib.md5(raw.encode()).hexdigest()[:12]

        df["match_id"] = df.apply(make_id, axis=1)
        df["fetched"]  = datetime.utcnow().isoformat()

        cols = ["match_id","league","season","date","home","away",
                "home_goals","away_goals","xg_home","xg_away","result","fetched"]
        return df[cols].reset_index(drop=True)

    def scrape_all(
        self,
        leagues: list = None,
        seasons: list = None,
        save_cache: bool = True
    ) -> pd.DataFrame:
        leagues = leagues or list(LEAGUES.keys())
        seasons = seasons or SEASONS
        frames  = []

        for lg in leagues:
            for s in seasons:
                log.info(f"━ Scraping {lg} | {s}")
                df = self.scrape_league_season(lg, s)
                if df.empty:
                    continue
                frames.append(df)
                if save_cache:
                    for _, row in df.iterrows():
                        self.cache.upsert_match(row.to_dict())
                log.info(f"  ✓ {len(df)} matches stored")

        if not frames:
            log.warning("No data scraped.")
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        log.info(f"\n✅ Total scraped: {len(combined)} matches across "
                 f"{combined['league'].nunique()} leagues / "
                 f"{combined['season'].nunique()} seasons")
        return combined


# ── CLI quick test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = FBrefScraper()
    # Test with EPL 2023-2024 only
    df = scraper.scrape_all(
        leagues=["EPL", "Serie A"],
        seasons=["2023-2024"],
        save_cache=True
    )
    print(df.tail(10).to_string())
    print(f"\nShape: {df.shape}")
