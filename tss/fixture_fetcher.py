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
        "Girona","UD Las Palmas","Deportivo Alaves","Rayo Vallecano","Mallorca",
        "Espanyol","Sevilla","Real Valladolid","CD Leganes",
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

def _is_valid_club(team: str, league: str) -> bool:
    """Return True if team is known for this league (or no whitelist exists)."""
    clubs = KNOWN_CLUBS.get(league)
    if not clubs:
        return True  # No filter for leagues without whitelist
    team_lower = team.lower().strip()
    for club in clubs:
        if (club.lower() in team_lower or team_lower in club.lower() or
                SequenceMatcher(None, team_lower, club.lower()).ratio() > 0.82):
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

def _fetch_tsdb_day(target_date: date) -> List[Dict]:
    """
    Fetch ALL soccer matches on a specific date using TheSportsDB /eventsday.php.
    This is date-accurate (unlike /eventsnextleague which ignores date filters).
    """
    url  = f"{TSDB_BASE}/eventsday.php?d={target_date}&s=Soccer"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return []
        return r.json().get("events") or []
    except Exception as e:
        log.error(f"TheSportsDB day fetch error [{target_date}]: {e}")
        return []


def _fetch_tsdb_league(league: str, date_from: date, date_to: date) -> List[Dict]:
    """
    Fetch fixtures for a league in a date range.
    Strategy:
      1. Try /eventsday.php per day (accurate date filter)
      2. Fallback: /eventsnextleague.php (less accurate but broader)
    """
    league_id = TSDB_LEAGUES.get(league)
    matches   = []
    seen_ids  = set()

    # Strategy 1: day-by-day fetch (date-accurate)
    current = date_from
    while current <= date_to:
        day_events = _fetch_tsdb_day(current)
        for ev in day_events:
            ev_id   = str(ev.get("idEvent", ""))
            if ev_id in seen_ids:
                continue
            # Filter by league
            ev_league = ev.get("strLeague", "")
            log.info(f"  TSDB event: {ev.get('strHomeTeam')} vs {ev.get('strAwayTeam')} | strLeague={ev_league!r}")
            if not _league_matches(ev_league, league):
                continue
            home = ev.get("strHomeTeam", "")
            away = ev.get("strAwayTeam", "")
            if not home or not away:
                continue
            if not (_is_valid_club(home, league) and _is_valid_club(away, league)):
                continue
            seen_ids.add(ev_id)
            time_str = (ev.get("strTime") or "")[:5]
            matches.append({
                "league":   league,
                "home":     home,
                "away":     away,
                "date":     str(current),
                "time":     time_str,
                "status":   "SCHEDULED",
                "match_id": ev_id,
                "source":   "TheSportsDB-day",
            })
        current += timedelta(days=1)

    # Strategy 2: fallback to next-league endpoint if day fetch returned nothing
    if not matches and league_id:
        log.info(f"  {league}: day-fetch returned 0 → trying nextleague fallback")
        try:
            url = f"{TSDB_BASE}/eventsnextleague.php?id={league_id}"
            r   = requests.get(url, timeout=15)
            if r.status_code == 200:
                for ev in (r.json().get("events") or []):
                    ev_date_str = ev.get("dateEvent", "")
                    if not ev_date_str:
                        continue
                    try:
                        ev_date = datetime.strptime(ev_date_str, "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if not (date_from <= ev_date <= date_to):
                        continue
                    home = ev.get("strHomeTeam", "")
                    away = ev.get("strAwayTeam", "")
                    ev_str_league = ev.get("strLeague", "?")
                    log.info(f"  FALLBACK event: {home} vs {away} | strLeague={ev_str_league!r}")
                    if not home or not away:
                        continue
                    if not (_is_valid_club(home, league) and _is_valid_club(away, league)):
                        log.info(f"    → SKIP: clubs not in {league} whitelist")
                        continue
                    time_str = (ev.get("strTime") or "")[:5]
                    ev_id = str(ev.get("idEvent", ""))
                    if ev_id not in seen_ids:
                        seen_ids.add(ev_id)
                        matches.append({
                            "league":   league,
                            "home":     home,
                            "away":     away,
                            "date":     ev_date_str,
                            "time":     time_str,
                            "status":   "SCHEDULED",
                            "match_id": ev_id,
                            "source":   "TheSportsDB-next",
                        })
        except Exception as e:
            log.debug(f"Fallback error [{league}]: {e}")

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

def _fetch_fixtures(date_from: str, date_to: str) -> List[Dict]:
    """Fetch fixtures from TheSportsDB (primary) across all cached leagues."""
    d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    d_to   = datetime.strptime(date_to,   "%Y-%m-%d").date()

    all_matches = []
    for league in CACHED_LEAGUES:
        # Primary: TheSportsDB
        matches = _fetch_tsdb_league(league, d_from, d_to)

        # Backup: football-data.org
        if not matches:
            matches = _fetch_fdorg_league(league, date_from, date_to)

        if matches:
            log.info(f"  {league}: {len(matches)} fixtures ({matches[0]['source']})")
        all_matches.extend(matches)

    # Deduplicate by (date, home, away)
    seen = set()
    unique = []
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
