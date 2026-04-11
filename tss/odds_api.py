"""
APEX-TSS — Real Odds Fetcher (The Odds API)
=============================================
Fetches live bookmaker odds and merges them with fixtures
from TheSportsDB for the /scan command.

API: https://the-odds-api.com (500 req/month free)
"""

import os
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional
from difflib import SequenceMatcher

log = logging.getLogger("odds_api")

ODDS_API_KEY  = os.environ.get("ODDS_API_KEY", "4556cbebcaea0e8301f1c176bdb64e31")
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# The Odds API sport keys per league
SPORT_KEYS = {
    "EPL":        "soccer_epl",
    "Serie A":    "soccer_italy_serie_a",
    "La Liga":    "soccer_spain_la_liga",
    "Bundesliga": "soccer_germany_bundesliga",
    "Ligue 1":    "soccer_france_ligue_one",
    "Eredivisie": "soccer_netherlands_eredivisie",
    "Belgian Pro":"soccer_belgium_first_div",
}

PREFERRED_BOOKMAKERS = [
    "betfair_ex_eu", "pinnacle", "bet365", "unibet_eu",
    "williamhill", "bwin", "betclic",
]

REGIONS = "eu"


# ═══════════════════════════════════════════════════════════════════════════════
# 1. FETCH ODDS FROM API
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_sport_odds(sport_key: str) -> List[Dict]:
    """Fetch h2h + totals odds for a sport."""
    try:
        r = requests.get(
            f"{ODDS_API_BASE}/sports/{sport_key}/odds/",
            params={
                "apiKey":      ODDS_API_KEY,
                "regions":     REGIONS,
                "markets":     "h2h,totals",
                "oddsFormat":  "decimal",
                "dateFormat":  "iso",
            },
            timeout=15
        )
        remaining = r.headers.get("x-requests-remaining", "?")
        log.info(f"  OddsAPI {sport_key}: {r.status_code} | {remaining} req remaining")

        if r.status_code == 200:
            return r.json()
        elif r.status_code == 401:
            log.error("OddsAPI: invalid API key")
        elif r.status_code == 429:
            log.error("OddsAPI: quota exceeded")
        else:
            log.warning(f"OddsAPI {r.status_code}: {r.text[:150]}")
    except Exception as e:
        log.error(f"OddsAPI fetch error [{sport_key}]: {e}")
    return []


def _extract_odds(match_data: Dict) -> Dict:
    """
    Extract best available odds from bookmaker list.
    Priority: Pinnacle > Bet365 > any available.
    Returns: {odds_H, odds_D, odds_A, odds_over2.5, odds_under2.5, bookie}
    """
    bookmakers = match_data.get("bookmakers", [])
    if not bookmakers:
        return {}

    # Sort by preference
    def bk_priority(bk):
        title = bk.get("key", "")
        try:
            return PREFERRED_BOOKMAKERS.index(title)
        except ValueError:
            return 99

    bookmakers = sorted(bookmakers, key=bk_priority)

    result = {}
    bookie_used = ""

    for bk in bookmakers:
        bk_result = {}
        for market in bk.get("markets", []):
            key      = market.get("key", "")
            outcomes = market.get("outcomes", [])

            if key == "h2h":
                odds_map = {o["name"]: o["price"] for o in outcomes}
                home = match_data.get("home_team", "")
                away = match_data.get("away_team", "")

                if home in odds_map and away in odds_map:
                    bk_result["odds_H"] = round(odds_map[home], 3)
                    bk_result["odds_A"] = round(odds_map[away], 3)
                    # Draw (3-way market)
                    draw_key = next(
                        (k for k in odds_map if k not in (home, away)), None
                    )
                    if draw_key:
                        bk_result["odds_D"] = round(odds_map[draw_key], 3)

            elif key == "totals":
                for o in outcomes:
                    pt = o.get("point", 0)
                    if pt == 2.5:
                        if o["name"] == "Over":
                            bk_result["odds_over2.5"]  = round(o["price"], 3)
                        elif o["name"] == "Under":
                            bk_result["odds_under2.5"] = round(o["price"], 3)
                    elif pt == 3.5:
                        if o["name"] == "Over":
                            bk_result["odds_over3.5"]  = round(o["price"], 3)
                        elif o["name"] == "Under":
                            bk_result["odds_under3.5"] = round(o["price"], 3)

        # Use first bookmaker that has h2h odds
        if "odds_H" in bk_result and not result:
            result     = bk_result
            bookie_used = bk.get("title", bk.get("key", "unknown"))
            if "odds_D" not in result:
                result["odds_D"] = None   # some markets are 2-way

    result["bookie_used"] = bookie_used
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 2. TEAM NAME MATCHER
# ═══════════════════════════════════════════════════════════════════════════════

# Team name normalization map: TheSportsDB → Odds API canonical
TSDB_TO_ODDS_MAP = {
    # Eredivisie
    "AFC Ajax":            "Ajax",
    "Ajax Amsterdam":      "Ajax",
    "Feyenoord Rotterdam": "Feyenoord",
    "PSV Eindhoven":       "PSV",
    "AZ Alkmaar":          "AZ",
    "FC Twente":           "Twente",
    "FC Utrecht":          "Utrecht",
    "NEC Nijmegen":        "NEC",
    # Belgian Pro
    "Club Brugge KV":      "Club Brugge",
    "RSC Anderlecht":      "Anderlecht",
    "KRC Genk":            "Genk",
    "KAA Gent":            "Gent",
    "Standard de Liège":   "Standard Liege",
    "Royal Antwerp FC":    "Antwerp",
    # EPL
    "Manchester City FC":  "Manchester City",
    "Manchester United FC":"Manchester United",
    "Tottenham Hotspur FC":"Tottenham Hotspur",
    "Arsenal FC":          "Arsenal",
    "Chelsea FC":          "Chelsea",
    "Liverpool FC":        "Liverpool",
    # La Liga
    "FC Barcelona":        "Barcelona",
    "Real Madrid CF":      "Real Madrid",
    "Club Atlético de Madrid": "Atletico Madrid",
    "Athletic Club de Bilbao": "Athletic Club",
    # Bundesliga
    "FC Bayern München":   "Bayern Munich",
    "Borussia Dortmund":   "Borussia Dortmund",
    "Bayer 04 Leverkusen": "Bayer Leverkusen",
    # Serie A
    "AC Milan":            "AC Milan",
    "FC Internazionale":   "Inter Milan",
    "Juventus FC":         "Juventus",
    # Ligue 1
    "Paris Saint-Germain FC": "Paris Saint-Germain",
    "Olympique de Marseille": "Marseille",
    "Olympique Lyonnais":  "Lyon",
}

def _normalise_team_name(name: str) -> str:
    """Apply canonical mapping + strip common suffixes."""
    name = TSDB_TO_ODDS_MAP.get(name, name)
    # Strip common suffixes/prefixes for fuzzy matching
    clean = name.lower()
    for tok in ["fc", "sc", "ac", "afc", "rsc", "kv", "krc", "kaa",
                "cf", "fk", "sk", "bk", "if", "il", "vfl", "vfb",
                "1.", "1 ", "fsv", "tsv"]:
        clean = clean.replace(tok, " ")
    return " ".join(clean.split())


def _name_sim(a: str, b: str) -> float:
    na = "".join(c for c in _normalise_team_name(a) if c.isalnum())
    nb = "".join(c for c in _normalise_team_name(b) if c.isalnum())
    return SequenceMatcher(None, na, nb).ratio()


def _match_team(api_name: str, fixture_name: str) -> float:
    """Multi-strategy similarity for API vs fixture team names."""
    # Apply canonical mappings first
    a_norm = _normalise_team_name(api_name)
    f_norm = _normalise_team_name(fixture_name)
    scores = [
        _name_sim(api_name, fixture_name),      # full names
        _name_sim(a_norm, f_norm),               # normalised
        _name_sim(api_name.split()[0], fixture_name.split()[0]),  # first word
        _name_sim(api_name.split()[-1], fixture_name.split()[-1]),# last word
    ]
    return max(scores)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. MERGE ODDS INTO FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_fixtures_with_odds(fixtures: List[Dict]) -> List[Dict]:
    """
    For each fixture, fetch real bookmaker odds and attach them.
    Fixtures without odds keep odds=None (TSS uses synthetic fallback).
    """
    # Group fixtures by league
    by_league: Dict[str, List[int]] = {}
    for i, fix in enumerate(fixtures):
        by_league.setdefault(fix["league"], []).append(i)

    # Fetch odds per league (one API call per league)
    for league, indices in by_league.items():
        sport_key = SPORT_KEYS.get(league)
        if not sport_key:
            continue

        api_matches = _fetch_sport_odds(sport_key)
        if not api_matches:
            continue

        # Match each fixture to an API entry
        for idx in indices:
            fix        = fixtures[idx]
            fix_home   = fix["home"]
            fix_away   = fix["away"]
            fix_date   = fix["date"]

            best_score = 0.0
            best_entry = None

            for entry in api_matches:
                # Date filter: within ±1 day
                try:
                    entry_date = entry["commence_time"][:10]
                    d_delta    = abs(
                        (datetime.fromisoformat(fix_date) -
                         datetime.fromisoformat(entry_date)).days
                    )
                    if d_delta > 1:
                        continue
                except Exception:
                    pass

                api_home = entry.get("home_team", "")
                api_away = entry.get("away_team", "")

                score = (
                    _match_team(api_home, fix_home) +
                    _match_team(api_away, fix_away)
                ) / 2

                if score > best_score and score >= 0.58:
                    best_score = score
                    best_entry = entry

            if best_entry:
                odds = _extract_odds(best_entry)
                fixtures[idx].update(odds)
                fixtures[idx]["odds_matched"]  = True
                fixtures[idx]["odds_score"]    = round(best_score, 3)
                log.debug(f"  Matched: {fix_home} vs {fix_away} → "
                          f"{best_entry['home_team']} (score={best_score:.2f}, "
                          f"bookie={odds.get('bookie_used','-')})")
            else:
                fixtures[idx]["odds_matched"] = False
                best_name = best_entry["home_team"] if best_entry else "none"
                log.info(f"  UNMATCHED: {fix_home} vs {fix_away} | Best: {best_name} (score={best_score:.2f})")

    matched = sum(1 for f in fixtures if f.get("odds_matched"))
    log.info(f"Odds enrichment: {matched}/{len(fixtures)} fixtures matched")
    return fixtures


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SHIN DEMARGINALIZATION (for real odds → P_book)
# ═══════════════════════════════════════════════════════════════════════════════

def demarginalize_odds(fix: Dict) -> Dict:
    """
    Convert real book odds → P_book via Shin method.
    Returns dict: {P_H, P_D, P_A, P_over2.5, P_under2.5, ...}
    """
    import numpy as np

    def shin(odds_list):
        raw   = [1/o for o in odds_list if o and o > 1.0]
        if len(raw) < 2:
            return [1/len(odds_list)] * len(odds_list)
        total = sum(raw)
        n     = len(raw)
        z     = (total - 1) / (total * (n-1)/n + total - 1 + 1e-9)
        z     = max(0.001, min(z, 0.5))
        return [
            (np.sqrt(z**2 + 4*(1-z)*p**2/total) - z) / (2*(1-z))
            for p in raw
        ]

    result = {}

    # 1X2
    h = fix.get("odds_H")
    d = fix.get("odds_D")
    a = fix.get("odds_A")
    if h and a:
        if d:
            p = shin([h, d, a])
            result["P_H"], result["P_D"], result["P_A"] = p[0], p[1], p[2]
        else:
            p = shin([h, a])
            result["P_H"], result["P_A"] = p[0], p[1]
            result["P_D"] = 0.0

    # Over/Under 2.5
    ov = fix.get("odds_over2.5")
    un = fix.get("odds_under2.5")
    if ov and un:
        p = shin([ov, un])
        result["P_over2.5"], result["P_under2.5"] = p[0], p[1]

    # Over/Under 3.5
    ov3 = fix.get("odds_over3.5")
    un3 = fix.get("odds_under3.5")
    if ov3 and un3:
        p = shin([ov3, un3])
        result["P_over3.5"], result["P_under3.5"] = p[0], p[1]

    return result
