#!/usr/bin/env python3
"""APEX-HOCKEY — agent AD (DISCOVERY) : résolution des matchs.

Trois modes d'entrée, une seule sortie : `fixtures.json`.

    match  — les deux équipes ; à défaut de date, le prochain match programmé
    league — tous les matchs de la ligue dans la fenêtre (défaut : 24 h)
    date   — tous les matchs du jour dans les ligues filtrées

Une date saisie est interprétée dans `timezone_input` puis convertie en UTC.
Un match de NHL joué le soir en Amérique du Nord tombe le lendemain en UTC :
la sélection se fait sur la **date locale saisie**, jamais sur la date UTC.

Sources : calendrier officiel NHL (api-web.nhle.com) recoupé avec le
catalogue d'affiches The Odds API. Un calendrier injoignable est déclaré
`leagues_unreachable` — jamais complété de mémoire.
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from apex_common import match_id_of, now_utc, slug  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "runs_hockey", ".cache")
NHL_API = "https://api-web.nhle.com/v1"
ODDS_API = "https://api.the-odds-api.com/v4"
CACHE_TTL = 20 * 60

# Registre des ligues. `ot_rules` n'est renseigné que là où la règle est
# établie par le protocole ; ailleurs il reste None et A0 doit la vérifier —
# supposer une règle de prolongation fausse la conversion 3-way → moneyline.
LEAGUES = {
    "NHL": {
        "tier": "TIER_A", "odds_key": "icehockey_nhl", "official": "nhl_api",
        "ot_rules": {"regular": {"format": "3v3", "minutes": 5, "shootout": True},
                     "playoff": {"format": "5v5", "minutes": None, "shootout": False}},
    },
    "NHL_PRESEASON": {
        "tier": "TIER_C", "odds_key": "icehockey_nhl_preseason", "official": "nhl_api",
        "ot_rules": None,
        "notes": "Présaison : alignements non représentatifs, gardiens en rotation.",
    },
    "SHL":     {"tier": "TIER_B", "odds_key": "icehockey_sweden_hockey_league",
                "official": None, "ot_rules": None},
    "Liiga":   {"tier": "TIER_B", "odds_key": "icehockey_liiga",
                "official": None, "ot_rules": None},
    "HockeyAllsvenskan": {"tier": "TIER_C", "odds_key": "icehockey_sweden_allsvenskan",
                          "official": None, "ot_rules": None},
    "Mestis":  {"tier": "TIER_C", "odds_key": "icehockey_mestis",
                "official": None, "ot_rules": None},
    # Ligues du protocole sans flux d'affiches accessible : elles gardent leur
    # tier, mais leurs matchs n'arrivent qu'en mode `match` explicite.
    "KHL":       {"tier": "TIER_B", "odds_key": None, "official": None, "ot_rules": None},
    "DEL":       {"tier": "TIER_B", "odds_key": None, "official": None, "ot_rules": None},
    "NL":        {"tier": "TIER_B", "odds_key": None, "official": None, "ot_rules": None},
    "Extraliga": {"tier": "TIER_C", "odds_key": None, "official": None, "ot_rules": None},
    "AHL":       {"tier": "TIER_C", "odds_key": None, "official": None, "ot_rules": None},
}

LEAGUE_ALIASES = {
    "nhl": "NHL", "national hockey league": "NHL",
    "nhl preseason": "NHL_PRESEASON", "presaison nhl": "NHL_PRESEASON",
    "shl": "SHL", "svenska hockeyligan": "SHL",
    "liiga": "Liiga", "sm liiga": "Liiga",
    "allsvenskan": "HockeyAllsvenskan", "hockeyallsvenskan": "HockeyAllsvenskan",
    "mestis": "Mestis", "khl": "KHL", "del": "DEL",
    "national league": "NL", "nl": "NL", "swiss national league": "NL",
    "extraliga": "Extraliga", "ahl": "AHL",
}

TIER_DEFAULT = ["TIER_A", "TIER_B"]

# États NHL : seul un match encore à venir est jouable.
NHL_SCHEDULED = {"FUT", "PRE"}
NHL_STARTED = {"LIVE", "CRIT", "FINAL", "OFF"}


# --------------------------------------------------------------------------
# Cache réseau
# --------------------------------------------------------------------------

def _cache(name: str, fetch):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{name}.json")
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < CACHE_TTL:
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            pass
    data = fetch()
    if data is not None:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
    return data


def _odds_key() -> str:
    return os.environ.get("ODDS_API_KEY", "4556cbebcaea0e8301f1c176bdb64e31")


def nhl_schedule(day: str):
    """Calendrier officiel NHL pour une date (source primaire, TIER_A)."""
    def fetch():
        try:
            r = requests.get(f"{NHL_API}/schedule/{day}", timeout=25)
            return r.json() if r.status_code == 200 else None
        except requests.RequestException:
            return None
    return _cache(f"nhl_schedule_{day}", fetch)


def odds_events(sport_key: str):
    """Affiches The Odds API (source de recoupement + présence de cotes)."""
    def fetch():
        try:
            r = requests.get(f"{ODDS_API}/sports/{sport_key}/events",
                             params={"apiKey": _odds_key(), "dateFormat": "iso"},
                             timeout=25)
            return r.json() if r.status_code == 200 else None
        except requests.RequestException:
            return None
    return _cache(f"odds_{sport_key}", fetch)


# --------------------------------------------------------------------------
# Normalisation de la requête
# --------------------------------------------------------------------------

def resolve_league_name(text: str) -> str | None:
    key = " ".join(slug(text).split("_"))
    if key in LEAGUE_ALIASES:
        return LEAGUE_ALIASES[key]
    for alias, league in LEAGUE_ALIASES.items():
        if alias == key:
            return league
    best, score = None, 0.0
    for alias, league in LEAGUE_ALIASES.items():
        ratio = SequenceMatcher(None, key, alias).ratio()
        if ratio > score:
            best, score = league, ratio
    return best if score >= 0.85 else None


def normalize_request(spec: dict) -> dict:
    """Écrit la requête canonique : fenêtre UTC + ligues ciblées."""
    req = dict(spec.get("request") or spec)
    mode = req.get("mode")
    if mode not in ("match", "league", "date"):
        raise SystemExit("ERREUR: request.mode doit valoir match, league ou date.")

    tz_name = spec.get("timezone_input") or req.get("timezone_input") or "UTC"
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        raise SystemExit(f"ERREUR: fuseau '{tz_name}' inconnu.")

    now = datetime.now(timezone.utc)
    leagues, tiers = [], TIER_DEFAULT

    if mode == "date":
        day = req.get("date")
        if not day:
            raise SystemExit("ERREUR: mode date sans champ date.")
        local_start = datetime.fromisoformat(day).replace(tzinfo=tz)
        local_end = local_start + timedelta(days=1)
        win_from, win_to = local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc)
        wanted = req.get("leagues_filter") or []
        if req.get("include_tier_c"):
            tiers = ["TIER_A", "TIER_B", "TIER_C"]
        leagues = [resolve_league_name(x) for x in wanted] if wanted else [
            name for name, cfg in LEAGUES.items() if cfg["tier"] in tiers]
        leagues = [x for x in leagues if x]

    elif mode == "league":
        league = resolve_league_name(req.get("league") or "")
        if not league:
            raise SystemExit(f"ERREUR: ligue '{req.get('league')}' non reconnue. "
                             f"Connues : {', '.join(sorted(LEAGUES))}.")
        leagues = [league]
        window = req.get("window") or {}
        if window.get("from"):
            win_from = datetime.fromisoformat(window["from"]).replace(tzinfo=tz).astimezone(timezone.utc)
        else:
            win_from = now
        if window.get("to"):
            win_to = (datetime.fromisoformat(window["to"]).replace(tzinfo=tz)
                      + timedelta(days=1)).astimezone(timezone.utc)
        else:
            win_to = win_from + timedelta(hours=24)

    else:  # match
        m = req.get("match") or {}
        if not m.get("home") or not m.get("away"):
            raise SystemExit("ERREUR: mode match sans home/away.")
        league = resolve_league_name(m.get("league") or "") if m.get("league") else None
        leagues = [league] if league else [
            name for name, cfg in LEAGUES.items() if cfg["tier"] in TIER_DEFAULT]
        if m.get("date"):
            local_start = datetime.fromisoformat(m["date"]).replace(tzinfo=tz)
            win_from = local_start.astimezone(timezone.utc)
            win_to = (local_start + timedelta(days=1)).astimezone(timezone.utc)
        else:
            # Pas de date : on cherche le prochain match programmé entre elles.
            win_from, win_to = now, now + timedelta(days=30)

    return {
        "mode": mode,
        "timezone_input": tz_name,
        "window_from_utc": win_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_to_utc": win_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "local_date": req.get("date") or (req.get("match") or {}).get("date"),
        "leagues": leagues,
        "tiers": tiers,
        "target_match": req.get("match") if mode == "match" else None,
        "max_matches": spec.get("max_matches", 12),
        "executed_at_utc": now_utc(),
    }


# --------------------------------------------------------------------------
# Collecte des affiches
# --------------------------------------------------------------------------

def _days_of(window_from: str, window_to: str, step: int = 1) -> list[str]:
    start = datetime.strptime(window_from, "%Y-%m-%dT%H:%M:%SZ").date()
    end = datetime.strptime(window_to, "%Y-%m-%dT%H:%M:%SZ").date()
    days = [str(start + timedelta(days=i)) for i in range(0, (end - start).days + 1, step)]
    if str(end) not in days:
        days.append(str(end))
    return days


def _nhl_team_name(team: dict) -> str:
    """« Minnesota Wild » : place + surnom. Le surnom seul ne se recoupe pas
    avec les noms complets de la seconde source."""
    place = (team.get("placeName") or {}).get("default", "")
    common = (team.get("commonName") or {}).get("default", "")
    full = f"{place} {common}".strip()
    return full or team.get("abbrev", "?")


def nhl_fixtures(window_from: str, window_to: str, league: str) -> tuple[list[dict], bool]:
    """Affiches NHL depuis le calendrier officiel. Retourne (matchs, joignable)."""
    want_preseason = league == "NHL_PRESEASON"
    out, reachable, seen = [], False, set()
    # /schedule/<date> renvoie la semaine entière : on interroge de 7 en 7
    # jours et on dédoublonne, sinon une même partie ressort autant de fois
    # qu'elle apparaît dans les semaines qui se chevauchent.
    for day in _days_of(window_from, window_to, step=7):
        data = nhl_schedule(day)
        if data is None:
            continue
        reachable = True
        for week in data.get("gameWeek", []):
            for game in week.get("games", []) or []:
                start = game.get("startTimeUTC", "")
                if not (window_from <= start <= window_to):
                    continue
                # gameType 1 = présaison, 2 = saison régulière, 3 = playoffs.
                gtype = game.get("gameType")
                if want_preseason != (gtype == 1):
                    continue
                state = game.get("gameState", "")
                home = game.get("homeTeam", {}) or {}
                away = game.get("awayTeam", {}) or {}
                signature = (game.get("id") or
                             (home.get("abbrev"), away.get("abbrev"), start))
                if signature in seen:
                    continue
                seen.add(signature)
                out.append({
                    "home": _nhl_team_name(home),
                    "away": _nhl_team_name(away),
                    "home_abbrev": home.get("abbrev"), "away_abbrev": away.get("abbrev"),
                    "league": league,
                    "game_type": "playoff" if gtype == 3 else "regular",
                    "puck_drop_utc": start,
                    "venue": (game.get("venue") or {}).get("default"),
                    "nhl_game_state": state,
                    "source_url": f"{NHL_API}/schedule/{day}",
                })
    return out, reachable


def odds_fixtures(league: str, window_from: str, window_to: str) -> tuple[list[dict], bool]:
    cfg = LEAGUES[league]
    if not cfg["odds_key"]:
        return [], False
    data = odds_events(cfg["odds_key"])
    if data is None:
        return [], False
    out = []
    for ev in data:
        start = ev.get("commence_time", "")
        if window_from <= start <= window_to:
            out.append({
                "home": ev.get("home_team"), "away": ev.get("away_team"),
                "league": league, "game_type": "regular",
                "puck_drop_utc": start, "venue": None,
                "odds_event_id": ev.get("id"),
                "source_url": f"{ODDS_API}/sports/{cfg['odds_key']}/events",
            })
    return out, True


TIME_TOLERANCE_MIN = 15  # sous ce seuil, simple imprécision de flux


def _team_match(x: str, y: str) -> float:
    """Rapproche « Wild » et « Minnesota Wild ».

    Les flux nomment les équipes différemment (surnom seul, ville + surnom,
    accents). Une inclusion de tokens vaut identité ; la similarité brute ne
    sert que de repli.
    """
    tx, ty = set(slug(x or "").split("_")) - {""}, set(slug(y or "").split("_")) - {""}
    if not tx or not ty:
        return 0.0
    if tx <= ty or ty <= tx:
        return 1.0
    overlap = len(tx & ty) / min(len(tx), len(ty))
    return max(overlap, SequenceMatcher(None, slug(x or ""), slug(y or "")).ratio())


def _minutes_apart(a: str, b: str) -> float:
    try:
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        return abs((datetime.strptime(a, fmt) - datetime.strptime(b, fmt)).total_seconds()) / 60
    except ValueError:
        return 9999


def _same_fixture(a: dict, b: dict) -> bool:
    """Deux sources décrivent-elles le même match ?"""
    pair = (_team_match(a["home"], b["home"]) + _team_match(a["away"], b["away"])) / 2
    # Un match tard le soir peut basculer de date UTC selon la source :
    # on tolère 6 h d'écart pour l'appariement, l'heure officielle tranche.
    return pair >= 0.75 and _minutes_apart(a["puck_drop_utc"], b["puck_drop_utc"]) <= 360


def merge_sources(primary: list[dict], secondary: list[dict]) -> list[dict]:
    """Recoupe les deux sources. L'officielle fait foi sur l'heure."""
    merged = []
    used = set()
    for fx in primary:
        twin = next((i for i, s in enumerate(secondary)
                     if i not in used and _same_fixture(fx, s)), None)
        flags, urls = [], [fx["source_url"]]
        if twin is None:
            flags.append("SINGLE_SOURCE")
        else:
            used.add(twin)
            other = secondary[twin]
            urls.append(other["source_url"])
            gap = _minutes_apart(other["puck_drop_utc"], fx["puck_drop_utc"])
            if gap > TIME_TOLERANCE_MIN:
                # Heure divergente : la source officielle l'emporte, mais
                # l'écart est signalé pour que A1 vérifie avant de coter.
                flags.append(f"TIME_CONFLICT_{int(gap)}MIN")
            if other.get("odds_event_id"):
                fx["odds_event_id"] = other["odds_event_id"]
        merged.append({**fx, "flags": flags, "source_urls": urls})

    for i, fx in enumerate(secondary):
        if i not in used:
            merged.append({**fx, "flags": ["SINGLE_SOURCE"],
                           "source_urls": [fx["source_url"]]})
    return merged


def classify(fx: dict, request: dict) -> str:
    cfg = LEAGUES.get(fx["league"], {})
    if fx.get("nhl_game_state") == "PPD":
        return "POSTPONED"
    if fx.get("nhl_game_state") in NHL_STARTED:
        return "EXCLUDED_STARTED"
    if fx["puck_drop_utc"] <= request["executed_at_utc"]:
        return "EXCLUDED_STARTED"
    if cfg.get("tier") == "TIER_C" and "TIER_C" not in request["tiers"]:
        return "EXCLUDED_TIER_C"
    return "SCHEDULED"


def discover(request: dict) -> dict:
    win_from, win_to = request["window_from_utc"], request["window_to_utc"]
    fixtures, unreachable = [], []

    def collect(league: str):
        cfg = LEAGUES[league]
        primary, ok_primary = ([], False)
        if cfg["official"] == "nhl_api":
            primary, ok_primary = nhl_fixtures(win_from, win_to, league)
        secondary, ok_secondary = odds_fixtures(league, win_from, win_to)
        if not ok_primary and not ok_secondary:
            return league, None
        if not primary:
            primary, secondary = secondary, []
        return league, merge_sources(primary, secondary)

    with ThreadPoolExecutor(max_workers=6) as pool:
        for league, found in pool.map(collect, request["leagues"]):
            if found is None:
                unreachable.append({"league": league,
                                    "reason": "calendrier et catalogue d'affiches injoignables"})
                continue
            fixtures += found

    # Mode match : ne retenir que l'affiche demandée.
    ambiguous = []
    if request["mode"] == "match":
        target = request["target_match"] or {}
        want_home, want_away = slug(target.get("home", "")), slug(target.get("away", ""))

        def score(fx):
            direct = (SequenceMatcher(None, want_home, slug(fx["home"])).ratio()
                      + SequenceMatcher(None, want_away, slug(fx["away"])).ratio()) / 2
            swapped = (SequenceMatcher(None, want_home, slug(fx["away"])).ratio()
                       + SequenceMatcher(None, want_away, slug(fx["home"])).ratio()) / 2
            return max(direct, swapped)

        ranked = sorted(((score(f), f) for f in fixtures), key=lambda t: -t[0])
        strong = [(s, f) for s, f in ranked if s >= 0.75]
        if not strong:
            fixtures = []
        elif len(strong) > 1 and abs(strong[0][0] - strong[1][0]) < 0.05:
            # Deux candidats également plausibles : on ne devine pas.
            ambiguous = [{"home": f["home"], "away": f["away"], "league": f["league"],
                          "puck_drop_utc": f["puck_drop_utc"], "similarity": round(s, 3)}
                         for s, f in strong[:4]]
            fixtures = []
        else:
            # Sans date fournie, c'est le prochain match programmé qui compte.
            fixtures = [min((f for s, f in strong if s >= strong[0][0] - 0.05),
                            key=lambda f: f["puck_drop_utc"])]

    enriched = []
    for fx in fixtures:
        cfg = LEAGUES.get(fx["league"], {})
        status = classify(fx, request)
        flags = list(fx.get("flags", []))
        if cfg.get("ot_rules") is None and status == "SCHEDULED":
            # A0 devra établir la règle : la supposer fausserait la
            # conversion 3-way (temps réglementaire) → moneyline (avec OT).
            flags.append("OT_RULES_UNVERIFIED")
        if not cfg.get("odds_key"):
            flags.append("NO_ODDS_FEED")
        enriched.append({
            "match_id": match_id_of(fx["home"], fx["away"], fx["puck_drop_utc"]),
            "home": fx["home"], "away": fx["away"], "league": fx["league"],
            "tier": cfg.get("tier", "TIER_C"),
            "game_type": fx.get("game_type", "regular"),
            "series_state": None,
            "puck_drop_utc": fx["puck_drop_utc"],
            "venue": fx.get("venue"),
            "odds_event_id": fx.get("odds_event_id"),
            "odds_sport_key": cfg.get("odds_key"),
            "ot_rules": cfg.get("ot_rules"),
            "fixture_status": status,
            "flags": flags,
            "source_urls": fx.get("source_urls", []),
        })
    enriched.sort(key=lambda f: (f["puck_drop_utc"], f["match_id"]))

    scheduled = [f for f in enriched if f["fixture_status"] == "SCHEDULED"]
    if ambiguous:
        status = "AMBIGUOUS"
    elif unreachable and not scheduled:
        status = "DATA_REQUEST"
    elif not scheduled:
        status = "EMPTY"
    else:
        status = "OK"

    return {
        "agent": "AD_DISCOVERY",
        "request_mode": request["mode"],
        "window_utc": {"from": win_from, "to": win_to},
        "timezone_input": request["timezone_input"],
        "status": status,
        "fixtures": enriched,
        "ambiguous_candidates": ambiguous,
        "leagues_unreachable": unreachable,
        "generated_at_utc": now_utc(),
    }


if __name__ == "__main__":
    import argparse
    from apex_common import load_input, write_json
    parser = argparse.ArgumentParser(description="APEX-HOCKEY DISCOVERY")
    parser.add_argument("--input", required=True, help="Bloc INPUT yaml/json")
    parser.add_argument("--out-dir", help="Dossier du run (écrit request.json + fixtures.json)")
    args = parser.parse_args()

    spec = load_input(args.input)
    request = normalize_request(spec)
    fixtures = discover(request)
    if args.out_dir:
        write_json(os.path.join(args.out_dir, "request.json"), request)
        write_json(os.path.join(args.out_dir, "fixtures.json"), fixtures)
    print(json.dumps(fixtures, ensure_ascii=False, indent=2))
