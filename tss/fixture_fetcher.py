"""
APEX-TSS — Fixture Fetcher
============================
Source primaire : TheSportsDB (gratuit, sans clé API)
Source backup   : football-data.org (si clé valide)

Commandes:
  /scan today          → matchs du jour
  /scan 48h            → prochaines 48 heures
  /scan week           → 7 prochains jours
  /scan 12/04          → date spécifique
  /scan 12/04-14/04    → plage de dates
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from difflib import SequenceMatcher

# ── Known clubs per league (filter out wrong-division teams) ──────────────────
KNOWN_CLUBS = {
    "EPL": {
        "Arsenal","Chelsea","Liverpool","Manchester City","Manchester United",
        "Tottenham","Newcastle United","Brighton","Aston Villa","West Ham United",
        "Wolves","Everton","Fulham","Brentford","Crystal Palace","Bournemouth",
        "Nottingham Forest","Leicester City","Southampton","Ipswich Town",
        "Luton Town","Sheffield United","Burnley","Watford","Leeds United",
        # Common variations
        "Man City","Man United","Spurs","Nott'm Forest","Newcastle",
        "Wolverhampton","West Ham","Brighton & Hove Albion",
    },
    "Serie A": {
        "Napoli","Inter","Milan","AC Milan","Juventus","Roma","Lazio","Atalanta",
        "Fiorentina","Bologna","Torino","Monza","Lecce","Genoa","Hellas Verona",
        "Cagliari","Empoli","Udinese","Como","Venezia","Parma","Salernitana",
    },
    "La Liga": {
        "Real Madrid","Barcelona","Atletico Madrid","Athletic Club","Villarreal",
        "Real Sociedad","Real Betis","Valencia","Celta Vigo","Getafe","Osasuna",
        "Girona","Las Palmas","UD Las Palmas","Alaves","Deportivo Alaves",
        "Rayo Vallecano","Vallecano","Mallorca","Espanyol","Sevilla",
        "Real Valladolid","Valladolid","Leganes","CD Leganes",
        "Levante","Levante UD","Oviedo","Real Oviedo",
        "Burgos","Elche","Huesca","Mirandes","Sporting Gijon",
    },
    "Bundesliga": {
        "Bayern Munich","Borussia Dortmund","Bayer Leverkusen","RB Leipzig",
        "Eintracht Frankfurt","VfL Wolfsburg","VfB Stuttgart","SC Freiburg",
        "TSG Hoffenheim","FC Augsburg","1. FSV Mainz 05","Werder Bremen",
        "1. FC Union Berlin","1. FC Heidenheim","VfL Bochum",
        "Holstein Kiel","FC St. Pauli","Borussia Monchengladbach",
    },
    "Ligue 1": {
        "Paris Saint-Germain","Olympique de Marseille","Olympique Lyonnais",
        "AS Monaco","OGC Nice","RC Lens","LOSC Lille","Stade Rennais",
        "Montpellier HSC","RC Strasbourg","Toulouse FC","FC Nantes",
        "Stade de Reims","Stade Brestois","Le Havre AC","AJ Auxerre",
        "SCO Angers","AS Saint-Etienne","FC Metz","PSG","Lyon","Marseille",
    },
}

# Explicit non-league clubs to always reject (Club World Cup, friendlies...)
# Club World Cup 2025 + non-European teams that appear under European sport keys
ALWAYS_REJECT = {
    # Oceania
    "Adelaide United", "Auckland City", "Wellington Phoenix",
    # Africa
    "Al Ahly", "Mamelodi Sundowns", "Wydad", "ES Tunis",
    # Asia
    "Al Hilal", "Al Ain", "Urawa Red Diamonds", "Jeonbuk",
    "Ulsan", "Al Nassr", "Al Ittihad",
    # Americas
    "Seattle Sounders", "Monterrey", "Club Leon", "Pachuca",
    "Fluminense", "Flamengo", "CF Montreal", "Inter Miami",
    "LA Galaxy", "Los Angeles FC", "Portland Timbers",
    "New York City FC", "Atlanta United",
    # Middle East
    "Al Qadsiah",
}

def _is_valid_club(team: str, league: str) -> bool:
    """Return True if team is known for this league (or no whitelist exists)."""
    # Explicit reject list — substring match (handles "Adelaide United FC" etc.)
    team_stripped = team.strip().lower()
    if any(r.lower() in team_stripped or team_stripped in r.lower()
           for r in ALWAYS_REJECT):
        return False

    clubs = KNOWN_CLUBS.get(league)
    if not clubs:
        return True  # No filter for leagues without whitelist

    team_lower = team.lower().strip()
    for club in clubs:
        club_lower = club.lower()
        # Exact substring match
        if club_lower in team_lower or team_lower in club_lower:
            return True
        # Fuzzy match — raised threshold to 0.87 to avoid false positives
        if SequenceMatcher(None, team_lower, club_lower).ratio() > 0.87:
            return True
    return False


import re

log = logging.getLogger("fixture_fetcher")

FDORG_TOKEN = os.environ.get("FDORG_TOKEN", "b8b980d46849a1fc55c8bd271bcad18c")
FDORG_BASE  = "https://api.football-data.org/v4"
TSDB_BASE   = "https://www.thesportsdb.com/api/v1/json/3"

# TheSportsDB league IDs
# Only leagues with verified TheSportsDB IDs returning correct clubs
TSDB_LEAGUES = {
    "EPL":        4328,   # ✅ Verified
    "Serie A":    4332,   # ✅ Verified
    "La Liga":    4335,   # ✅ Verified
    "Bundesliga": 4331,   # ✅ Verified
    "Ligue 1":    4334,   # ✅ Verified
    # Eredivisie/Belgian Pro removed — IDs 4337/4397 return English clubs (wrong)
    # Will be re-added when correct IDs are confirmed
}

# football-data.org competition codes (backup)
FDORG_LEAGUES = {
    "EPL":        "PL",
    "Serie A":    "SA",
    "La Liga":    "PD",
    "Bundesliga": "BL1",
    "Ligue 1":    "FL1",
    "Eredivisie": "DED",
    "Belgian Pro":"BSA",
}

CACHED_LEAGUES = list(TSDB_LEAGUES.keys())  # 5 verified leagues only


# ═══════════════════════════════════════════════════════════════════════════════
# THESPORTSDB (primary — free, no auth)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_tsdb_league(league: str, date_from: date, date_to: date) -> List[Dict]:
    """
    Fetch upcoming fixtures using /eventsnextleague.php.
    /eventsday.php is unreliable for future dates (TheSportsDB data lag).
    Club whitelist ensures only correct-division teams pass through.
    """
    league_id = TSDB_LEAGUES.get(league)
    if not league_id:
        return []

    matches = []
    try:
        url = f"{TSDB_BASE}/eventsnextleague.php?id={league_id}"
        r   = requests.get(url, timeout=15)
        if r.status_code != 200:
            log.warning(f"TheSportsDB {r.status_code} for {league}")
            return []

        events = r.json().get("events") or []
        log.info(f"  {league} ({league_id}): {len(events)} raw events from TSDB")

        for ev in events:
            ev_date_str = ev.get("dateEvent", "")
            home        = ev.get("strHomeTeam", "")
            away        = ev.get("strAwayTeam", "")
            ev_league   = ev.get("strLeague", "?")

            log.info(f"    → {home} vs {away} | {ev_date_str} | strLeague={ev_league!r}")

            if not ev_date_str or not home or not away:
                continue

            try:
                ev_date = datetime.strptime(ev_date_str, "%Y-%m-%d").date()
            except Exception:
                continue

            if not (date_from <= ev_date <= date_to):
                log.info(f"    SKIP date out of range: {ev_date} not in [{date_from},{date_to}]")
                continue

            # Strict club whitelist — blocks wrong-division clubs
            if not (_is_valid_club(home, league) and _is_valid_club(away, league)):
                log.info(f"    SKIP whitelist: {home} vs {away} not in {league}")
                continue

            time_str = (ev.get("strTime") or "")[:5]
            matches.append({
                "league":   league,
                "home":     home,
                "away":     away,
                "date":     ev_date_str,
                "time":     time_str,
                "status":   "SCHEDULED",
                "match_id": str(ev.get("idEvent", "")),
                "source":   "TheSportsDB",
            })

    except Exception as e:
        log.error(f"TheSportsDB error [{league}]: {e}")

    return matches


# League name → TSDB strLeague matching
TSDB_LEAGUE_NAMES = {
    "EPL":        ["Premier League", "English Premier League"],
    "Serie A":    ["Serie A", "Italian Serie A"],
    "La Liga":    ["La Liga", "Spanish La Liga"],
    "Bundesliga": ["Bundesliga", "German Bundesliga", "1. Bundesliga"],
    "Ligue 1":    ["Ligue 1", "French Ligue 1"],
    "Eredivisie": ["Eredivisie", "Dutch Eredivisie"],
    "Belgian Pro":["Belgian First Division A", "Jupiler Pro League"],
}

def _league_matches(ev_league_str: str, league_key: str) -> bool:
    """Check if TheSportsDB strLeague matches our league key."""
    ev_lower = ev_league_str.lower()
    for name in TSDB_LEAGUE_NAMES.get(league_key, []):
        if name.lower() in ev_lower or ev_lower in name.lower():
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# FOOTBALL-DATA.ORG (backup — requires valid token)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_fdorg_league(league: str, date_from: str, date_to: str) -> List[Dict]:
    """Fallback: fetch from football-data.org if TSDB returns nothing."""
    comp_id = FDORG_LEAGUES.get(league)
    if not comp_id:
        return []
    matches = []
    try:
        url = f"{FDORG_BASE}/competitions/{comp_id}/matches"
        r   = requests.get(url,
                           headers={"X-Auth-Token": FDORG_TOKEN},
                           params={"dateFrom": date_from, "dateTo": date_to},
                           timeout=15)
        if r.status_code == 200:
            for m in r.json().get("matches", []):
                if m.get("status") in ("SCHEDULED","TIMED","IN_PLAY","LIVE"):
                    matches.append({
                        "league":   league,
                        "home":     m["homeTeam"]["name"],
                        "away":     m["awayTeam"]["name"],
                        "date":     m["utcDate"][:10],
                        "time":     m["utcDate"][11:16],
                        "status":   m["status"],
                        "match_id": str(m["id"]),
                        "source":   "football-data.org",
                    })
        else:
            log.debug(f"FDORG {r.status_code} for {league}")
    except Exception as e:
        log.debug(f"FDORG error [{league}]: {e}")
    return matches


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_odds_api_fixtures(date_from: str, date_to: str) -> List[Dict]:
    """
    Primary fixture source: The Odds API /events endpoint.
    Returns upcoming fixtures WITH odds — always up to date for current season.
    """
    import os
    api_key = os.environ.get("ODDS_API_KEY", "4556cbebcaea0e8301f1c176bdb64e31")
    base    = "https://api.the-odds-api.com/v4"

    SPORT_KEYS = {
        "EPL":        "soccer_epl",
        "Serie A":    "soccer_italy_serie_a",
        "La Liga":    "soccer_spain_la_liga",
        "Bundesliga": "soccer_germany_bundesliga",
        "Ligue 1":    "soccer_france_ligue_one",
    }

    d_from = datetime.strptime(date_from, "%Y-%m-%d")
    d_to   = datetime.strptime(date_to,   "%Y-%m-%d")
    # Add 1 day to date_to to include full day
    from datetime import timezone
    commence_to = d_to.replace(hour=23, minute=59).strftime("%Y-%m-%dT%H:%M:%SZ")

    all_matches = []
    seen        = set()

    for league, sport_key in SPORT_KEYS.items():
        try:
            r = requests.get(
                f"{base}/sports/{sport_key}/events/",
                params={
                    "apiKey":        api_key,
                    "dateFormat":    "iso",
                    "commenceTimeTo": commence_to,
                },
                timeout=15
            )
            remaining = r.headers.get("x-requests-remaining", "?")
            if r.status_code != 200:
                log.warning(f"  OddsAPI events {sport_key}: {r.status_code}")
                continue

            events = r.json()
            log.info(f"  {league}: {len(events)} events from OddsAPI ({remaining} req left)")

            for ev in events:
                commence = ev.get("commence_time", "")
                if not commence:
                    continue
                # Parse date
                ev_date = datetime.strptime(commence[:10], "%Y-%m-%d")
                if not (d_from <= ev_date <= d_to):
                    continue

                home = ev.get("home_team", "")
                away = ev.get("away_team", "")
                if not home or not away:
                    continue

                ev_id = ev.get("id", f"{home}{away}{commence[:10]}")
                if ev_id in seen:
                    continue
                seen.add(ev_id)

                # DEBUG: log exact strings from OddsAPI
                log.info(f"    OddsAPI raw: home={home!r} away={away!r}")
                valid_h = _is_valid_club(home, league)
                valid_a = _is_valid_club(away, league)
                log.info(f"    Whitelist: {home!r}={valid_h} {away!r}={valid_a}")
                if not (valid_h and valid_a):
                    log.info(f"    SKIP wrong clubs: {home!r} vs {away!r} [{league}]")
                    continue

                time_str = commence[11:16] if len(commence) > 10 else ""
                all_matches.append({
                    "league":   league,
                    "home":     home,
                    "away":     away,
                    "date":     commence[:10],
                    "time":     time_str,
                    "status":   "SCHEDULED",
                    "match_id": str(ev_id),
                    "source":   "OddsAPI-events",
                })

        except Exception as e:
            log.error(f"OddsAPI events error [{league}]: {e}")

    return sorted(all_matches, key=lambda x: (x["date"], x["time"]))


def _fetch_fixtures(date_from: str, date_to: str) -> List[Dict]:
    """
    Fixture pipeline:
    1. OddsAPI /events (primary — always has current season data)
    2. TheSportsDB (fallback — may lag on future dates)
    """
    # Primary: Odds API events
    matches = _fetch_odds_api_fixtures(date_from, date_to)
    if matches:
        log.info(f"Fixtures from OddsAPI: {len(matches)} total")
        return matches

    # Fallback: TheSportsDB
    log.warning("OddsAPI returned 0 fixtures — trying TheSportsDB fallback")
    d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    d_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()
    all_matches = []
    for league in CACHED_LEAGUES:
        m = _fetch_tsdb_league(league, d_from, d_to)
        if m:
            log.info(f"  {league}: {len(m)} fixtures (TheSportsDB)")
        all_matches.extend(m)

    seen, unique = set(), []
    for m in all_matches:
        key = (m["date"], m["home"][:8], m["away"][:8])
        if key not in seen:
            seen.add(key)
            unique.append(m)
    return sorted(unique, key=lambda x: (x["date"], x["time"]))


# ═══════════════════════════════════════════════════════════════════════════════
# WINDOW PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_scan_window(text: str) -> Tuple[str, str, str]:
    """
    Parse time window from command text.
    Returns: (date_from_str, date_to_str, label)
    """
    text  = text.strip().lower()
    today = datetime.utcnow().date()

    if text in ("today", "aujourd'hui", "auj", "ce soir", "jour"):
        d_from, d_to = today, today
        label = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"

    elif re.match(r"^\d+h$", text):
        # Any Nh format: 6h, 8h, 24h, 48h...
        hours  = int(text[:-1])
        days   = max(1, (hours + 23) // 24)
        d_from = today
        d_to   = today + timedelta(days=days)
        label  = f"Prochaines {text} ({today.strftime('%d/%m')} → {d_to.strftime('%d/%m')})"

    elif text in ("48h", "48"):
        d_from = today
        d_to   = today + timedelta(days=2)
        label  = f"Prochaines 48h ({today.strftime('%d/%m')} → {d_to.strftime('%d/%m')})"

    elif text in ("week", "semaine", "7j", "7d"):
        d_from = today
        d_to   = today + timedelta(days=7)
        label  = f"Cette semaine ({today.strftime('%d/%m')} → {(today+timedelta(days=7)).strftime('%d/%m')})"

    elif text in ("tomorrow", "demain"):
        d_from = d_to = today + timedelta(days=1)
        label  = f"Demain {d_from.strftime('%d/%m/%Y')}"

    elif "-" in text and "/" in text and text.count("/") >= 2:
        # Range: 12/04-14/04
        try:
            parts  = text.split("-", 1)
            d_from = _parse_date(parts[0].strip(), today.year)
            d_to   = _parse_date(parts[1].strip(), today.year)
            label  = f"{d_from.strftime('%d/%m')} → {d_to.strftime('%d/%m')}"
        except Exception:
            d_from = d_to = today
            label  = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"

    elif "/" in text:
        try:
            d_from = d_to = _parse_date(text, today.year)
            label  = d_from.strftime('%d/%m/%Y')
        except Exception:
            d_from = d_to = today
            label  = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"
    else:
        d_from = d_to = today
        label  = f"Aujourd'hui {today.strftime('%d/%m/%Y')}"

    return str(d_from), str(d_to), label


def _parse_date(text: str, year: int) -> date:
    text = text.strip()
    m = re.match(r"(\d{1,2})[/\-.](\d{1,2})(?:[/\-.](\d{2,4}))?", text)
    if m:
        d  = int(m.group(1))
        mo = int(m.group(2))
        y  = int(m.group(3)) if m.group(3) else year
        y  = 2000 + y if y < 100 else y
        return date(y, mo, d)
    raise ValueError(f"Cannot parse date: {text}")


def get_fixtures(window_text: str) -> Tuple[List[Dict], str]:
    """Main entry point. Returns (fixtures, label)."""
    date_from, date_to, label = parse_scan_window(window_text)
    log.info(f"Fetching fixtures: {date_from} → {date_to}")
    fixtures = _fetch_fixtures(date_from, date_to)
    log.info(f"Found {len(fixtures)} fixtures in window")
    return fixtures, label
