#!/usr/bin/env python3
"""APEX-TURF — agent AD (DISCOVERY) : résolution des courses.

Trois modes d'entrée, une seule sortie : `fixtures.json`.

    race    — une course précise (R1C4, « Prix d'Amérique », « Quinté du jour »)
    meeting — toutes les courses d'une réunion ou d'un hippodrome
    date    — toutes les courses du jour, filtrables par pays et discipline

Source de programme : l'API publique de l'opérateur PMU, qui expose le
programme officiel (réunions, courses, disciplines, distances, heures de
départ, statuts) et les partants. Les pays et opérateurs qu'elle ne couvre
pas sont déclarés `meetings_unreachable` — jamais complétés de mémoire.

La date saisie est interprétée dans `timezone_input`. Les heures sont
conservées en UTC **et** en heure locale de l'hippodrome : une réunion
américaine ou hongkongaise peut tomber sur une autre date UTC que sa date
locale, et c'est la date locale qui décide de l'appartenance à la journée.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from apex_common import now_utc, slug  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(ROOT, "runs_turf", ".cache")
PMU_BASE = "https://online.turfinfo.api.pmu.fr/rest/client/1"
CACHE_TTL = 10 * 60

# Statuts de course de l'opérateur → statut d'affiche du protocole.
RUN_STATUSES = {
    "FIN_COURSE", "ARRIVEE_PROVISOIRE", "ARRIVEE_DEFINITIVE",
    "ARRIVEE_DEFINITIVE_COMPLETE", "COURSE_ARRIVEE", "DEPART_CONFIRME",
}
ABANDON_STATUSES = {"ANNULEE", "COURSE_ANNULEE", "REPORTEE"}

DISCIPLINE_MAP = {
    "PLAT": "plat",
    "HAIE": "obstacle", "STEEPLE_CHASE": "obstacle", "STEEPLECHASE": "obstacle",
    "CROSS": "obstacle", "OBSTACLE": "obstacle",
    "ATTELE": "trot_attele", "TROT_ATTELE": "trot_attele",
    "MONTE": "trot_monte", "TROT_MONTE": "trot_monte",
}

# Couverture data par pays. Le tier conditionne la dégradation imposée par A0.
COUNTRY_TIERS = {
    "FRA": "TIER_A", "GBR": "TIER_A", "IRE": "TIER_A", "HKG": "TIER_A",
    "USA": "TIER_A", "GER": "TIER_B", "ITA": "TIER_B", "ESP": "TIER_B",
    "SWE": "TIER_B", "NOR": "TIER_B", "FIN": "TIER_B", "DEN": "TIER_B",
    "BEL": "TIER_B", "AUS": "TIER_B", "JPN": "TIER_B", "UAE": "TIER_B",
}
DEFAULT_TIER = "TIER_C"

# Le Quinté+ est unique dans une journée : il identifie une course à lui
# seul. Le MULTI, lui, est porté par une dizaine de courses par jour — les
# confondre rend « le Quinté du jour » ambigu alors qu'il ne l'est pas.
QUINTE_BETS = {"QUINTE_PLUS", "E_QUINTE_PLUS"}
PREMIUM_BETS = QUINTE_BETS | {"MULTI", "E_MULTI"}
PREMIUM_CLASSES = {"GROUPE_I", "GROUPE_II", "GROUPE_III", "LISTED"}


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


def programme(day: str):
    """Programme officiel d'une journée. `day` au format YYYY-MM-DD."""
    ddmmyyyy = datetime.strptime(day, "%Y-%m-%d").strftime("%d%m%Y")

    def fetch():
        try:
            r = requests.get(f"{PMU_BASE}/programme/{ddmmyyyy}", timeout=30)
            return r.json() if r.status_code == 200 else None
        except (requests.RequestException, ValueError):
            return None
    return _cache(f"programme_{day}", fetch)


def participants(day: str, reunion: int, course: int):
    ddmmyyyy = datetime.strptime(day, "%Y-%m-%d").strftime("%d%m%Y")

    def fetch():
        try:
            r = requests.get(
                f"{PMU_BASE}/programme/{ddmmyyyy}/R{reunion}/C{course}/participants",
                timeout=30)
            return r.json() if r.status_code == 200 else None
        except (requests.RequestException, ValueError):
            return None
    return _cache(f"participants_{day}_R{reunion}C{course}", fetch)


def _epoch_to_utc(ms) -> str | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _local_time(ms, offset_ms) -> str | None:
    """`timezoneOffset` de l'opérateur est en millisecondes, pas en minutes."""
    if not ms:
        return None
    minutes = int((offset_ms or 0) / 60000)
    if abs(minutes) >= 24 * 60:
        return None  # décalage aberrant : on n'invente pas d'heure locale
    tz = timezone(timedelta(minutes=minutes))
    return datetime.fromtimestamp(ms / 1000, tz=tz).strftime("%Y-%m-%dT%H:%M:%S%z")


# --------------------------------------------------------------------------
# Normalisation de la requête
# --------------------------------------------------------------------------

RACE_CODE = re.compile(r"^\s*R(\d+)\s*C(\d+)\s*$", re.IGNORECASE)


def normalize_request(spec: dict) -> dict:
    req = dict(spec.get("request") or spec)
    mode = req.get("mode")
    if mode not in ("race", "meeting", "date"):
        raise SystemExit("ERREUR: request.mode doit valoir race, meeting ou date.")

    tz_name = spec.get("timezone_input") or "UTC"
    today = datetime.now(timezone.utc).date()

    if mode == "date":
        day = req.get("date")
        if not day:
            raise SystemExit("ERREUR: mode date sans champ date.")
        days = [day]
    elif mode == "meeting":
        window = req.get("window") or {}
        start = window.get("from") or str(today)
        end = window.get("to") or start
        s, e = datetime.fromisoformat(start).date(), datetime.fromisoformat(end).date()
        days = [str(s + timedelta(days=i)) for i in range((e - s).days + 1)]
    else:  # race
        race = req.get("race") or {}
        if race.get("date"):
            days = [race["date"]]
        else:
            # Prochaine occurrence : on balaie les 8 prochains jours.
            days = [str(today + timedelta(days=i)) for i in range(8)]

    return {
        "mode": mode,
        "timezone_input": tz_name,
        "days_local": days,
        "countries_filter": [c.upper() for c in (req.get("countries_filter") or [])],
        "discipline_filter": [d.lower() for d in (req.get("discipline_filter") or [])],
        "premium_only": bool(req.get("premium_only")),
        "include_tier_c": bool(req.get("include_tier_c")),
        "target_race": req.get("race") if mode == "race" else None,
        "target_meeting": req.get("meeting") if mode == "meeting" else None,
        "max_races": spec.get("max_races", 10),
        "allowed_bet_types": spec.get("allowed_bet_types", ["WIN", "PLACE"]),
        "betting_system": spec.get("betting_system", "auto"),
        "executed_at_utc": now_utc(),
    }


# --------------------------------------------------------------------------
# Extraction des courses
# --------------------------------------------------------------------------

def _race_id(racecourse: str, reunion: int, course: int, day: str) -> str:
    return f"{slug(racecourse)}_r{reunion}c{course}_{day.replace('-', '')}"


def _classify(course: dict, tier: str, request: dict, off_utc: str | None) -> str:
    statut = (course.get("statut") or "").upper()
    if statut in ABANDON_STATUSES:
        return "ABANDONED"
    if statut in RUN_STATUSES:
        return "EXCLUDED_RUN"
    if off_utc and off_utc <= request["executed_at_utc"]:
        return "EXCLUDED_RUN"
    if tier == "TIER_C" and not request["include_tier_c"]:
        return "EXCLUDED_TIER_C"
    return "SCHEDULED"


def races_of_day(day: str, request: dict) -> tuple[list[dict], bool]:
    data = programme(day)
    if data is None:
        return [], False
    out = []
    for reu in (data.get("programme") or {}).get("reunions", []) or []:
        country = ((reu.get("pays") or {}).get("code") or "?").upper()
        if request["countries_filter"] and country not in request["countries_filter"]:
            continue
        tier = COUNTRY_TIERS.get(country, DEFAULT_TIER)
        hippo = reu.get("hippodrome") or {}
        racecourse = hippo.get("libelleLong") or hippo.get("libelleCourt") or "?"
        offset = reu.get("timezoneOffset")
        num_reunion = reu.get("numOfficiel") or reu.get("numExterne")

        for c in reu.get("courses", []) or []:
            discipline = DISCIPLINE_MAP.get((c.get("discipline") or "").upper(),
                                            (c.get("discipline") or "?").lower())
            if request["discipline_filter"] and discipline not in request["discipline_filter"]:
                continue
            bet_types = {p.get("typePari") for p in (c.get("paris") or [])}
            race_class = c.get("categorieParticularite") or c.get("categorieStatut") or "?"
            if request["premium_only"] and not (
                    bet_types & PREMIUM_BETS or race_class.upper() in PREMIUM_CLASSES):
                continue

            off_utc = _epoch_to_utc(c.get("heureDepart"))
            num_course = c.get("numExterne") or c.get("numOrdre")
            flags = []
            if not c.get("nombreDeclaresPartants"):
                flags.append("FIELD_SIZE_MISSING")
            if (reu.get("penetrometre") or {}) in (None, {}):
                flags.append("PENETROMETER_MISSING")
            # Une seule source de programme : le recoupement exigé par le
            # protocole reste à faire par l'agent DISCOVERY.
            flags.append("SINGLE_SOURCE")

            out.append({
                "race_id": _race_id(racecourse, num_reunion, num_course, day),
                "racecourse": racecourse, "country": country,
                "meeting_code": f"R{num_reunion}",
                "race_number": num_course,
                "race_name": c.get("libelle") or c.get("libelleCourt") or "?",
                "discipline": discipline,
                "distance_m": c.get("distance"),
                "race_class": race_class,
                "field_size_declared": c.get("nombreDeclaresPartants"),
                "off_time_utc": off_utc,
                "off_time_local": _local_time(c.get("heureDepart"), offset),
                "local_date": day,
                "bet_support": sorted(bet_types & PREMIUM_BETS) or None,
                "is_quinte": bool(bet_types & QUINTE_BETS),
                "available_bets": sorted(bet_types),
                "tier": tier,
                "operator_statut": c.get("statut"),
                "meteo": (reu.get("meteo") or {}).get("nebulositeLibelleCourt"),
                "penetrometre": reu.get("penetrometre"),
                "fixture_status": _classify(c, tier, request, off_utc),
                "flags": flags,
                "source_urls": [
                    f"{PMU_BASE}/programme/{datetime.strptime(day, '%Y-%m-%d').strftime('%d%m%Y')}"],
            })
    return out, True


# --------------------------------------------------------------------------
# Sélection selon le mode
# --------------------------------------------------------------------------

def _countries_in_programme(day: str) -> set[str]:
    """Pays réellement présents dans le programme, filtres ignorés."""
    data = programme(day)
    if data is None:
        return set()
    return {((reu.get("pays") or {}).get("code") or "?").upper()
            for reu in (data.get("programme") or {}).get("reunions", []) or []}


def _match_text(query: str, candidate: str) -> float:
    q, c = slug(query), slug(candidate)
    if not q or not c:
        return 0.0
    if q in c or c in q:
        return 1.0
    return SequenceMatcher(None, q, c).ratio()


def select_race(races: list[dict], target: dict) -> tuple[list[dict], list[dict]]:
    """Retient la course désignée, ou renvoie les candidats si c'est ambigu."""
    identifier = (target.get("identifier") or "").strip()
    racecourse = target.get("racecourse")
    country = (target.get("country") or "").upper()

    pool = races
    if racecourse:
        pool = [r for r in pool if _match_text(racecourse, r["racecourse"]) >= 0.60] or pool
    if country:
        pool = [r for r in pool if r["country"].startswith(country)] or pool

    code = RACE_CODE.match(identifier)
    if code:
        reunion, course = int(code.group(1)), int(code.group(2))
        hits = [r for r in pool if r["meeting_code"] == f"R{reunion}"
                and r["race_number"] == course]
        if len(hits) == 1:
            return hits, []
        # « R1C4 » sans hippodrome est ambigu dès qu'il y a plusieurs
        # programmes : le code de réunion n'est unique que dans une journée
        # d'un opérateur donné.
        return ([], hits) if hits else ([], [])

    low = slug(identifier)
    if "quinte" in low:
        hits = [r for r in pool if r.get("is_quinte")]
        # Un jour sans Quinté+ ne renvoie rien plutôt qu'une course approchante.
        return (hits, []) if len(hits) == 1 else ([], hits)

    scored = sorted(((max(_match_text(identifier, r["race_name"]),
                          _match_text(identifier, r["racecourse"])), r)
                     for r in pool), key=lambda t: -t[0])
    strong = [(s, r) for s, r in scored if s >= 0.72]
    if not strong:
        return [], []
    if len(strong) > 1 and abs(strong[0][0] - strong[1][0]) < 0.05:
        return [], [r for _, r in strong[:5]]
    # Prochaine occurrence : la plus proche dans le temps parmi les meilleures.
    best = [r for s, r in strong if s >= strong[0][0] - 0.05]
    return [min(best, key=lambda r: r["off_time_utc"] or "9")], []


def select_meeting(races: list[dict], target: dict) -> list[dict]:
    racecourse = target.get("racecourse")
    code = target.get("meeting_code")
    country = (target.get("country") or "").upper()
    pool = races
    if country:
        pool = [r for r in pool if r["country"].startswith(country)]
    if code:
        pool = [r for r in pool if r["meeting_code"].upper() == str(code).upper()]
    if racecourse:
        pool = [r for r in pool if _match_text(racecourse, r["racecourse"]) >= 0.60]
    if not (code or racecourse):
        # Prochaine réunion programmée : celle dont la 1re course part en premier.
        scheduled = [r for r in pool if r["fixture_status"] == "SCHEDULED"]
        if scheduled:
            first = min(scheduled, key=lambda r: r["off_time_utc"] or "9")
            pool = [r for r in pool if r["racecourse"] == first["racecourse"]
                    and r["local_date"] == first["local_date"]]
    return pool


def discover(request: dict) -> dict:
    races, unreachable = [], []
    countries_seen: set[str] = set()
    for day in request["days_local"]:
        found, ok = races_of_day(day, request)
        if not ok:
            unreachable.append({"day": day, "reason": "programme de la journée injoignable"})
            continue
        races += found
        countries_seen |= _countries_in_programme(day)

    # Un pays demandé mais absent du programme consulté n'autorise pas à
    # conclure « aucune course » : la source ne le couvre simplement pas.
    for code in request["countries_filter"]:
        if code not in countries_seen:
            unreachable.append({
                "country": code,
                "reason": "pays absent du programme consulté — la source de "
                          "programme ne le couvre pas ; fournir les courses en "
                          "mode race explicite ou ajouter une source pour ce pays"})

    ambiguous = []
    if request["mode"] == "race":
        races, ambiguous = select_race(races, request["target_race"] or {})
    elif request["mode"] == "meeting":
        races = select_meeting(races, request["target_meeting"] or {})

    races.sort(key=lambda r: (r["off_time_utc"] or "9", r["race_id"]))
    scheduled = [r for r in races if r["fixture_status"] == "SCHEDULED"]

    if ambiguous:
        status = "AMBIGUOUS"
    elif unreachable and not scheduled:
        status = "DATA_REQUEST"
    elif any("country" in u for u in unreachable) and request["countries_filter"]:
        status = "DATA_REQUEST" if not scheduled else "OK"
    elif not scheduled:
        status = "EMPTY"
    else:
        status = "OK"

    window = [r["off_time_utc"] for r in races if r["off_time_utc"]]
    return {
        "agent": "AD_DISCOVERY",
        "request_mode": request["mode"],
        "window_utc": {"from": min(window) if window else None,
                       "to": max(window) if window else None},
        "days_local": request["days_local"],
        "timezone_input": request["timezone_input"],
        "status": status,
        "fixtures": races,
        "ambiguous_candidates": [
            {k: r[k] for k in ("race_id", "racecourse", "country", "meeting_code",
                               "race_number", "race_name", "off_time_utc", "discipline")}
            for r in ambiguous],
        "meetings_unreachable": unreachable,
        "generated_at_utc": now_utc(),
    }


if __name__ == "__main__":
    import argparse
    from apex_common import load_input, write_json
    parser = argparse.ArgumentParser(description="APEX-TURF DISCOVERY")
    parser.add_argument("--input", required=True)
    parser.add_argument("--out-dir")
    args = parser.parse_args()
    spec = load_input(args.input)
    request = normalize_request(spec)
    fixtures = discover(request)
    if args.out_dir:
        write_json(os.path.join(args.out_dir, "request.json"), request)
        write_json(os.path.join(args.out_dir, "fixtures.json"), fixtures)
    print(json.dumps(fixtures, ensure_ascii=False, indent=2))
