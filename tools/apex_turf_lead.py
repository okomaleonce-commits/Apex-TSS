#!/usr/bin/env python3
"""APEX-TURF-LEAD — outillage mécanique de la chaîne hippique.

    init      résout la requête (course | réunion | date) et crée le run
    triage    passe légère sur la qualité des données quand il y a trop de courses
    check     valide l'enveloppe et évalue G0→G6 pour une course
    finalize  recalcule les edges, applique la bankroll, produit les livrables

Le conducteur ne juge pas : il lit le champ `status` des JSON sur disque.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import apex_turf_discovery as discovery  # noqa: E402
from apex_common import (as_float, dig, load_input, now_utc, read_json,  # noqa: E402
                         recompute_edge, validate_envelope, write_json)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOURNAL = os.path.join(ROOT, "journal", "apex_turf_journal.csv")

JOURNAL_COLUMNS = [
    "date", "race_id", "racecourse", "country", "discipline", "distance_m",
    "race_class", "field_size", "going", "tier", "drs", "vs", "bet_type",
    "betting_system", "runner_number", "runner_name", "p_model", "fair_odds",
    "market_odds", "final_odds", "takeout_pct", "edge_pct", "stake_units",
    "verdict", "blocking_gate", "finish_position", "pnl_units",
]

STEPS = [
    ("A0_ROUTER",        "00_routing.json"),
    ("A1_DATA",          "01_scraper.json"),
    ("A1_DATA",          "02_t1_integrity.json"),
    ("A2_CONTEXTE",      "03_t2_context.json"),
    ("A3_DECLARATIONS",  "04_t3_declarations.json"),
    ("A4_PROFIL",        "05_t4_race_profile.json"),
    ("A5_PRICING",       "06_t5_pricing.json"),
    ("A6_RISQUE",        "07_t6_volatility.json"),
    ("A7_MARCHE",        "08_t7_market.json"),
    ("A8_DECISION",      "09_t8_decision.json"),
    ("A9_CONSEIL",       "10_t9_council.json"),
    ("A10_PREFLIGHT",    "11_preflight.json"),
]

DRS_FLOOR = 55.0
MIN_FIELD_SIZE = 5
EDGE_TOLERANCE_PTS = 0.1
WIN_SUM_TOLERANCE = 0.005
PLACE_SUM_TOLERANCE = 0.01


# --------------------------------------------------------------------------
# Contrôles de pricing
# --------------------------------------------------------------------------

def validate_pricing(a5) -> list[str]:
    errors = []
    payload = (a5 or {}).get("payload") or {}
    runners = payload.get("runners") or []
    if not runners:
        return ["06_t5_pricing.json: aucun partant valorisé"]

    total_win = sum(as_float(r.get("p_win")) or 0.0 for r in runners)
    void = as_float(payload.get("p_void_race")) or 0.0
    if abs(total_win + void - 1.0) > WIN_SUM_TOLERANCE:
        errors.append(f"06_t5_pricing.json: somme p_win + p_course_sans_vainqueur "
                      f"= {total_win + void:.4f} hors tolérance ±{WIN_SUM_TOLERANCE}")

    places = payload.get("places_paid")
    if places:
        total_place = sum(as_float(r.get("p_place")) or 0.0 for r in runners)
        if abs(total_place - float(places)) > PLACE_SUM_TOLERANCE + void:
            errors.append(f"06_t5_pricing.json: somme p_place = {total_place:.4f} "
                          f"≠ places payées {places} ±{PLACE_SUM_TOLERANCE}")
    else:
        errors.append("06_t5_pricing.json: nombre de places payées non renseigné — "
                      "il doit venir du règlement de l'opérateur, jamais d'une supposition")

    for flag in (a5 or {}).get("flags", []):
        if str(flag).startswith(("WIN_SUM_ERROR", "PLACE_SUM_ERROR",
                                 "MISSING_REQUIRED_PARAM")):
            errors.append(f"06_t5_pricing.json: {flag}")
    return errors


def field_is_final(a3) -> tuple[bool, str]:
    payload = (a3 or {}).get("payload") or {}
    final = bool(payload.get("field_final"))
    risk = (payload.get("weather_risk_change_before_off") or "UNKNOWN").upper()
    return final and risk != "HIGH", risk


# --------------------------------------------------------------------------
# Gates
# --------------------------------------------------------------------------

def evaluate_gates(race_dir: str) -> dict:
    def f(name):
        return read_json(os.path.join(race_dir, name))

    errors, gate_log = [], []

    def pending(label):
        gate_log.append(f"{label} PENDING (fichier amont absent)")
        return {"blocking_gate": None, "verdict": "PENDING", "reason": None,
                "gate_log": gate_log, "schema_errors": errors,
                "next_step": next_step(race_dir), "complete": False}

    def blocked(gate, verdict, reason):
        return {"blocking_gate": gate, "verdict": verdict, "reason": reason,
                "gate_log": gate_log, "schema_errors": errors,
                "next_step": None, "complete": True}

    envelope = ["race_id", "agent", "status", "flags", "confidence", "payload",
                "missing_fields", "sources", "generated_at_utc"]

    routing = f("00_routing.json")
    if routing is None:
        return pending("A0")
    errors += validate_envelope(routing, "00_routing.json", required=envelope)
    tier = dig(routing.get("payload") or {}, "tier", default="TIER_C")

    # --- G0 ---------------------------------------------------------------
    scraper = f("01_scraper.json")
    if scraper is None:
        return pending("G0")
    errors += validate_envelope(scraper, "01_scraper.json", required=envelope)
    if not (scraper.get("payload") or {}) or scraper.get("status") == "DATA_REQUEST":
        return blocked("G0", "DATA_REQUEST", "01_scraper.json vide ou statut DATA_REQUEST")
    gate_log.append("G0 PASS")

    # --- G1 ---------------------------------------------------------------
    t1 = f("02_t1_integrity.json")
    if t1 is None:
        return pending("G1")
    errors += validate_envelope(t1, "02_t1_integrity.json", required=envelope)
    drs = as_float(dig(t1.get("payload") or {}, "drs", "data_reliability_score"))
    if t1.get("status") == "ABORT":
        return blocked("G1", "NO_BET (data)", "T1 status = ABORT")
    if drs is not None and drs < DRS_FLOOR:
        return blocked("G1", "NO_BET (data)", f"DRS {drs} < {DRS_FLOOR}")
    gate_log.append(f"G1 PASS (DRS={drs})")

    t2 = f("03_t2_context.json")
    if t2 is None:
        return pending("A2")
    errors += validate_envelope(t2, "03_t2_context.json", required=envelope)

    # --- G2 : déclarations et terrain -------------------------------------
    t3 = f("04_t3_declarations.json")
    if t3 is None:
        return pending("G2")
    errors += validate_envelope(t3, "04_t3_declarations.json", required=envelope)
    stable, risk = field_is_final(t3)
    if t3.get("status") == "WAIT_DECLARATIONS" or not stable:
        return blocked("G2", "WAIT_DECLARATIONS",
                       f"partants non définitifs ou terrain instable "
                       f"(risque de changement : {risk})")
    runners_declared = as_float(dig(t3.get("payload") or {}, "runners_declared"))
    gate_log.append(f"G2 PASS (champ définitif, {runners_declared} partants)")

    t4 = f("05_t4_race_profile.json")
    if t4 is None:
        return pending("A4")
    errors += validate_envelope(t4, "05_t4_race_profile.json", required=envelope)

    t5 = f("06_t5_pricing.json")
    if t5 is None:
        return pending("A5")
    errors += validate_envelope(t5, "06_t5_pricing.json", required=envelope)
    errors += validate_pricing(t5)
    field_at_pricing = as_float(dig(t5.get("payload") or {}, "field_size_at_pricing"))

    # --- G3 : volatilité ---------------------------------------------------
    t6 = f("07_t6_volatility.json")
    if t6 is None:
        return pending("G3")
    errors += validate_envelope(t6, "07_t6_volatility.json", required=envelope)
    if t6.get("status") == "NO_BET":
        return blocked("G3", "NO_BET (volatility)", "T6 status = NO_BET")
    gate_log.append("G3 PASS")

    # --- G4 : marché -------------------------------------------------------
    t7 = f("08_t7_market.json")
    if t7 is None:
        return pending("G4")
    errors += validate_envelope(t7, "08_t7_market.json", required=envelope)
    allowed = dig(t7.get("payload") or {}, "bets_with_value",
                  "allowed_bets_with_value", default=[]) or []
    if t7.get("status") == "NO_BET" or not allowed:
        return blocked("G4", "NO_BET (market)",
                       "Aucun pari autorisé avec value après prélèvement")
    gate_log.append(f"G4 PASS ({len(allowed)} pari(s) avec value)")

    # --- A8 ----------------------------------------------------------------
    t8 = f("09_t8_decision.json")
    if t8 is None:
        return pending("A8")
    errors += validate_envelope(t8, "09_t8_decision.json", required=envelope)
    p8 = t8.get("payload") or {}
    verdict = dig(p8, "verdict", default=t8.get("status"))
    edge = recompute_edge(p8.get("p_model"), p8.get("market_odds"),
                          p8.get("edge_pct"), EDGE_TOLERANCE_PTS)
    if edge["mismatch"]:
        errors.append(f"09_t8_decision.json: edge déclaré {edge['declared_edge_pct']} vs "
                      f"recalculé {edge['computed_edge_pct']} (écart {edge['delta']})")

    system = (p8.get("betting_system") or "").upper()
    if verdict == "BET":
        if not p8.get("bet_type"):
            errors.append("09_t8_decision.json: bet_type absent")
        if not system:
            errors.append("09_t8_decision.json: betting_system absent — mutuel, "
                          "cote fixe et exchange ne se calculent pas pareil")
        if system == "MUTUEL":
            if not p8.get("market_odds_is_estimate"):
                # Au mutuel le rapport n'est connu qu'au départ : le présenter
                # comme ferme est une erreur de nature, pas de précision.
                errors.append("09_t8_decision.json: betting_system MUTUEL mais "
                              "market_odds_is_estimate n'est pas true")
            if as_float(p8.get("min_acceptable_odds")) is None:
                errors.append("09_t8_decision.json: rapport minimum absent alors "
                              "que le rapport final est inconnu au moment du pari")
        if as_float(p8.get("takeout_pct")) is None:
            errors.append("09_t8_decision.json: takeout_pct non documenté")

    # --- G5 : conseil ------------------------------------------------------
    if verdict == "BET":
        t9 = f("10_t9_council.json")
        if t9 is None:
            return pending("G5")
        errors += validate_envelope(t9, "10_t9_council.json", required=envelope)
        council = dig(t9.get("payload") or {}, "council_verdict", "COUNCIL_VERDICT")
        if council == "VETO":
            return blocked("G5", "NO_BET (council)", "Conseil VETO — override A8")
        gate_log.append(f"G5 PASS (conseil={council})")
    else:
        gate_log.append(f"G5 SKIP (A8 verdict={verdict})")

    # --- G6 : preflight ----------------------------------------------------
    pre = f("11_preflight.json")
    if pre is None:
        return pending("G6")
    errors += validate_envelope(pre, "11_preflight.json", required=envelope)
    pre_flags = set(pre.get("flags") or [])
    pre_payload = pre.get("payload") or {}

    if pre.get("status") == "NO_BET" or pre_flags & {"TAKEOUT_UNDOCUMENTED", "BRIER_FAIL"}:
        return blocked("G6", "NO_BET (preflight)",
                       "Prélèvement non documenté ou Brier ≤ probabilités du marché")

    # Le champ a-t-il bougé depuis le pricing ? Un non-partant change toutes
    # les probabilités : le pari ne porte plus sur la course valorisée.
    field_now = as_float(dig(pre_payload, "field_size_now", "runners_now",
                             default=runners_declared))
    if (verdict == "BET" and field_at_pricing is not None and field_now is not None
            and int(field_now) != int(field_at_pricing)):
        return blocked("G6", "WAIT_DECLARATIONS",
                       f"champ modifié depuis le pricing "
                       f"({int(field_at_pricing)} → {int(field_now)} partants) — "
                       f"relancer A3 à A10 sur le champ réel")

    beats_market = pre_payload.get("beats_market_baseline")
    if verdict == "BET" and beats_market is False:
        # Ne pas battre les probabilités du marché sur l'historique, c'est
        # n'avoir aucun edge démontré, quelle que soit la cote affichée.
        gate_log.append("G6 RETROGRADE (le modèle ne bat pas la baseline de marché)")
        verdict = "INDICATIF"
    elif beats_market is None and verdict == "BET":
        gate_log.append("G6 RETROGRADE (comparaison au marché non fournie)")
        verdict = "INDICATIF"
    else:
        gate_log.append("G6 PASS")

    if tier == "TIER_C" and verdict == "BET":
        gate_log.append("TIER_C → plafonné INDICATIF")
        verdict = "INDICATIF"

    return {"blocking_gate": None, "verdict": verdict, "reason": None,
            "gate_log": gate_log, "schema_errors": errors, "edge": edge,
            "next_step": None, "complete": True}


def next_step(race_dir: str):
    for agent, filename in STEPS:
        if read_json(os.path.join(race_dir, filename)) is None:
            return {"agent": agent, "expected_file": filename}
    return None


# --------------------------------------------------------------------------
# Commandes
# --------------------------------------------------------------------------

def cmd_init(args) -> int:
    spec = load_input(args.input)
    request = discovery.normalize_request(spec)
    fixtures = discovery.discover(request)

    day = args.run or request["days_local"][0]
    run_dir = os.path.join(ROOT, "runs_turf", day)
    write_json(os.path.join(run_dir, "request.json"), request)
    write_json(os.path.join(run_dir, "fixtures.json"), fixtures)
    write_json(os.path.join(run_dir, "_input.json"), spec)
    ensure_journal()

    if fixtures["status"] != "OK":
        reason = {
            "EMPTY": "aucune course dans la fenêtre",
            "AMBIGUOUS": "désignation ambiguë — poser la question avec les candidats",
            "DATA_REQUEST": "programme ou pays injoignable",
        }[fixtures["status"]]
        print(json.dumps({"run_dir": os.path.relpath(run_dir, ROOT),
                          "blocking_gate": "G-1", "status": fixtures["status"],
                          "reason": reason,
                          "ambiguous_candidates": fixtures["ambiguous_candidates"],
                          "meetings_unreachable": fixtures["meetings_unreachable"]},
                         ensure_ascii=False, indent=2))
        return 3

    scheduled = [r for r in fixtures["fixtures"] if r["fixture_status"] == "SCHEDULED"]
    for fx in scheduled:
        rdir = os.path.join(run_dir, fx["race_id"])
        os.makedirs(rdir, exist_ok=True)
        write_json(os.path.join(rdir, "_race.json"), fx)
        sources = os.path.join(rdir, "sources.md")
        if not os.path.exists(sources):
            with open(sources, "w", encoding="utf-8") as fh:
                fh.write(f"# Sources — {fx['racecourse']} {fx['meeting_code']}"
                         f"C{fx['race_number']} · {fx['race_name']}\n\n"
                         f"| URL | retrieved_at_utc | agent |\n|---|---|---|\n")
                for url in fx["source_urls"]:
                    fh.write(f"| {url} | {fixtures['generated_at_utc']} | AD_DISCOVERY |\n")

    max_races = int(spec.get("max_races", 10))
    print(json.dumps({
        "run_dir": os.path.relpath(run_dir, ROOT),
        "mode": request["mode"],
        "window_utc": fixtures["window_utc"],
        "scheduled": len(scheduled),
        "excluded": {s: sum(1 for f in fixtures["fixtures"] if f["fixture_status"] == s)
                     for s in ("ABANDONED", "EXCLUDED_RUN", "EXCLUDED_TIER_C")},
        "meetings_unreachable": fixtures["meetings_unreachable"],
        "triage_required": len(scheduled) > max_races,
        "max_races": max_races,
    }, ensure_ascii=False, indent=2))
    return 0


def cmd_triage(args) -> int:
    """Tri sur la QUALITÉ DES DONNÉES, jamais sur un « bon coup » supposé."""
    run_dir = args.run if os.path.isabs(args.run) else os.path.join(ROOT, args.run)
    fixtures = read_json(os.path.join(run_dir, "fixtures.json")) or {}
    spec = read_json(os.path.join(run_dir, "_input.json")) or {}
    max_races = int(spec.get("max_races", 10))

    rows = []
    for fx in fixtures.get("fixtures", []):
        if fx["fixture_status"] != "SCHEDULED":
            continue
        rdir = os.path.join(run_dir, fx["race_id"])
        t1 = read_json(os.path.join(rdir, "02_t1_integrity.json")) or {}
        drs = as_float(dig(t1.get("payload") or {}, "drs", "data_reliability_score"))
        no_history = bool(dig(t1.get("payload") or {}, "majority_without_history",
                              default=False))
        field = fx.get("field_size_declared") or 0

        reasons = []
        if drs is not None and drs < DRS_FLOOR:
            reasons.append(f"DRS {drs} < {DRS_FLOOR}")
        if fx["tier"] == "TIER_C":
            reasons.append("pays TIER_C")
        if field < MIN_FIELD_SIZE:
            reasons.append(f"{field} partants < {MIN_FIELD_SIZE} — marché trop étroit")
        if no_history:
            reasons.append("majorité de partants sans historique exploitable")

        rows.append({"race_id": fx["race_id"], "racecourse": fx["racecourse"],
                     "discipline": fx["discipline"], "tier": fx["tier"],
                     "drs": drs, "field_size": field,
                     "off_time_utc": fx["off_time_utc"],
                     "excluded_reasons": reasons})

    kept = [r for r in rows if not r["excluded_reasons"]]
    kept.sort(key=lambda r: (-(r["drs"] if r["drs"] is not None else -1),
                             r["off_time_utc"] or "9"))
    selected_ids = {r["race_id"] for r in kept[:max_races]}

    for row in rows:
        if row["race_id"] in selected_ids:
            row["triage"] = "SELECTED"
        else:
            row["triage"] = "TRIAGED_OUT"
            row["triage_reason"] = (" ; ".join(row["excluded_reasons"])
                                    or f"hors des {max_races} premières au classement qualité")

    result = {"agent": "AT_TRIAGE", "max_races": max_races,
              "criteria": "DRS décroissant, puis proximité du départ",
              "selected": sorted(selected_ids), "rows": rows,
              "generated_at_utc": now_utc()}
    write_json(os.path.join(run_dir, "triage.json"), result)
    print(json.dumps({"selected": len(selected_ids),
                      "triaged_out": len(rows) - len(selected_ids)},
                     ensure_ascii=False, indent=2))
    return 0


def cmd_check(args) -> int:
    result = evaluate_gates(args.race_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["schema_errors"] else 0


def ensure_journal() -> None:
    os.makedirs(os.path.dirname(JOURNAL), exist_ok=True)
    if not os.path.exists(JOURNAL):
        with open(JOURNAL, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(JOURNAL_COLUMNS)


def append_journal(rows: list[dict]) -> int:
    ensure_journal()
    with open(JOURNAL, encoding="utf-8") as fh:
        existing = {r["race_id"] for r in csv.DictReader(fh) if r.get("race_id")}
    new = [r for r in rows if r["race_id"] not in existing]
    with open(JOURNAL, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=JOURNAL_COLUMNS)
        for row in new:
            writer.writerow({c: row.get(c, "") for c in JOURNAL_COLUMNS})
    return len(new)


def collect(run_dir: str) -> list[dict]:
    fixtures = read_json(os.path.join(run_dir, "fixtures.json")) or {}
    triage = read_json(os.path.join(run_dir, "triage.json")) or {}
    triage_by_id = {r["race_id"]: r for r in triage.get("rows", [])}

    rows = []
    for fx in fixtures.get("fixtures", []):
        rdir = os.path.join(run_dir, fx["race_id"])
        entry = {"fixture": fx, "race_id": fx["race_id"],
                 "triage": triage_by_id.get(fx["race_id"], {})}

        if fx["fixture_status"] != "SCHEDULED":
            entry.update({"verdict": fx["fixture_status"], "blocking_gate": "",
                          "reason": "exclue par DISCOVERY", "gate_log": [],
                          "schema_errors": [], "decision": {}, "drs": None, "vs": None,
                          "going": None, "edge": recompute_edge(None, None),
                          "confidence": 0.0})
            rows.append(entry)
            continue

        if entry["triage"].get("triage") == "TRIAGED_OUT":
            entry.update({"verdict": "TRIAGED_OUT", "blocking_gate": "",
                          "reason": entry["triage"].get("triage_reason", ""),
                          "gate_log": [], "schema_errors": [], "decision": {},
                          "drs": entry["triage"].get("drs"), "vs": None, "going": None,
                          "edge": recompute_edge(None, None), "confidence": 0.0})
            rows.append(entry)
            continue

        gates = evaluate_gates(rdir)
        t1 = read_json(os.path.join(rdir, "02_t1_integrity.json")) or {}
        t3 = read_json(os.path.join(rdir, "04_t3_declarations.json")) or {}
        t6 = read_json(os.path.join(rdir, "07_t6_volatility.json")) or {}
        t8 = read_json(os.path.join(rdir, "09_t8_decision.json")) or {}
        t9 = read_json(os.path.join(rdir, "10_t9_council.json")) or {}
        p8 = t8.get("payload") or {}
        entry.update({
            "verdict": gates["verdict"], "blocking_gate": gates["blocking_gate"] or "",
            "reason": gates["reason"] or "", "gate_log": gates["gate_log"],
            "schema_errors": gates["schema_errors"], "decision": p8,
            "drs": as_float(dig(t1.get("payload") or {}, "drs", "data_reliability_score")),
            "vs": as_float(dig(t6.get("payload") or {}, "vs", "volatility_score")),
            "going": dig((t3.get("payload") or {}).get("going_official") or {}, "label"),
            "runners_final": as_float(dig(t3.get("payload") or {}, "runners_declared")),
            "council": dig(t9.get("payload") or {}, "council_verdict", "COUNCIL_VERDICT"),
            "edge": gates.get("edge") or recompute_edge(p8.get("p_model"),
                                                        p8.get("market_odds"),
                                                        p8.get("edge_pct")),
            "confidence": as_float(t8.get("confidence")) or 0.0,
        })
        rows.append(entry)
    return rows


def apply_bankroll(rows: list[dict], spec: dict) -> list[str]:
    notes = []
    max_stake = as_float(spec.get("max_stake_per_bet")) or 1.0
    max_daily = as_float(spec.get("max_daily_exposure")) or 4.0
    bets = [r for r in rows if r["verdict"] == "BET"]

    for r in bets:
        stake = as_float(r["decision"].get("stake_units")) or 0.0
        r["stake_final"] = min(stake, max_stake)
        if stake > max_stake:
            notes.append(f"{r['race_id']} : mise {stake}u plafonnée à {max_stake}u")

    # Un seul pari par course : gagnant et placé sur le même cheval, ou deux
    # chevaux de la même course, portent le même classement.
    by_race: dict[str, dict] = {}
    for r in sorted(bets, key=lambda x: -(x["edge"]["computed_edge_pct"] or -999)):
        if r["race_id"] in by_race:
            r["stake_final"] = 0.0
            r["verdict"] = "NO_BET (corrélé)"
            notes.append(f"{r['race_id']} : second pari écarté (même course)")
        else:
            by_race[r["race_id"]] = r

    kept = [r for r in bets if r.get("stake_final", 0) > 0]
    total = sum(r["stake_final"] for r in kept)
    if total > max_daily:
        excess = total - max_daily
        for r in sorted(kept, key=lambda x: x["confidence"]):
            if excess <= 1e-9:
                break
            cut = min(r["stake_final"], excess)
            r["stake_final"] = round(r["stake_final"] - cut, 3)
            excess -= cut
            notes.append(f"{r['race_id']} : mise réduite de {cut}u (exposition > {max_daily}u)")
        notes.append(f"Exposition ramenée de {total:.2f}u à "
                     f"{sum(r['stake_final'] for r in kept):.2f}u")

    # Concentration : informatif, ne réduit aucune mise.
    people: dict[str, list[str]] = {}
    for r in kept:
        for role in ("jockey", "driver", "trainer", "stable"):
            who = r["decision"].get(role)
            if who:
                people.setdefault(f"{role}:{who}", []).append(r["race_id"])
    for key, races in people.items():
        if len(races) > 1:
            notes.append(f"CONCENTRATION — {key} sur {len(races)} paris : {', '.join(races)}")
    return notes


def fmt(value, digits=2):
    return "—" if value is None else f"{value:.{digits}f}"


def cmd_finalize(args) -> int:
    run_dir = args.run if os.path.isabs(args.run) else os.path.join(ROOT, args.run)
    spec = read_json(os.path.join(run_dir, "_input.json")) or {}
    request = read_json(os.path.join(run_dir, "request.json")) or {}
    fixtures = read_json(os.path.join(run_dir, "fixtures.json")) or {}
    rows = collect(run_dir)
    notes = apply_bankroll(rows, spec)
    run_label = os.path.basename(run_dir)

    lines = [
        f"# APEX-TURF — Synthèse du run {run_label}", "",
        f"- **Mode d'entrée** : `{request.get('mode','?')}`",
        f"- **Fenêtre UTC couverte** : {fixtures.get('window_utc',{}).get('from','?')} → "
        f"{fixtures.get('window_utc',{}).get('to','?')}",
        f"- **Journée(s) locale(s)** : {', '.join(request.get('days_local', []))} "
        f"({request.get('timezone_input','?')})",
        f"- **Courses trouvées** : {len(fixtures.get('fixtures', []))}", "",
        "| Course | Hippodrome | Discipline | Partants | Terrain | Tier | DRS | VS | Pari | Système | N° / Cheval | p_model | Fair | Cote | Edge % | Mise (u) | Verdict | Gate |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        fx, d = r["fixture"], r["decision"]
        runner = "—"
        if d.get("runner_number"):
            runner = f"{d.get('runner_number')} {d.get('runner_name','')}".strip()
        lines.append("| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            f"{fx['meeting_code']}C{fx['race_number']}", fx["racecourse"],
            fx["discipline"],
            int(r.get("runners_final") or fx.get("field_size_declared") or 0) or "—",
            r.get("going") or "—", fx["tier"], fmt(r.get("drs"), 1), fmt(r.get("vs"), 1),
            d.get("bet_type", "—") or "—", d.get("betting_system", "—") or "—", runner,
            fmt(as_float(d.get("p_model")), 4), fmt(as_float(d.get("fair_odds"))),
            fmt(as_float(d.get("market_odds"))), fmt(r["edge"]["computed_edge_pct"]),
            fmt(r.get("stake_final")), r["verdict"], r["blocking_gate"] or "—"))

    bets = [r for r in rows if r["verdict"] == "BET" and r.get("stake_final", 0) > 0]
    if bets:
        lines += ["", "## Conditions d'annulation"]
        for r in bets:
            d, fx = r["decision"], r["fixture"]
            estimate = d.get("market_odds_is_estimate")
            lines += [
                "", f"### {fx['racecourse']} {fx['meeting_code']}C{fx['race_number']} — "
                    f"{d.get('bet_type')} {d.get('runner_number')} {d.get('runner_name','')}",
                f"- Système : **{d.get('betting_system')}**"
                + (" — rapport **estimé**, non ferme jusqu'au départ" if estimate else ""),
                f"- Rapport minimum acceptable : **{fmt(as_float(d.get('min_acceptable_odds')))}**",
                f"- Prélèvement documenté : {fmt(as_float(d.get('takeout_pct')))} %",
                f"- Champ au pricing : {int(d.get('field_size_at_pricing') or 0) or '—'} partants",
                f"- Mise retenue : **{fmt(r.get('stake_final'))}u**",
            ]
            for cond in d.get("invalidation_conditions") or []:
                lines.append(f"- {cond}")

    if notes:
        lines += ["", "## Contrôle de bankroll"] + [f"- {n}" for n in notes]

    pending = [r for r in rows if r["verdict"] == "PENDING"]
    if pending:
        lines += ["", "## Pipelines inachevés (non journalisés)"]
        for r in pending:
            step = next_step(os.path.join(run_dir, r["race_id"])) or {}
            lines.append(f"- {r['race_id']} — prochain agent : {step.get('agent','?')}")

    if fixtures.get("meetings_unreachable"):
        lines += ["", "## Programmes injoignables"] + [
            f"- {u.get('country') or u.get('day')} : {u['reason']}"
            for u in fixtures["meetings_unreachable"]]

    errors = [e for r in rows for e in r["schema_errors"]]
    if errors:
        lines += ["", "## Anomalies de schéma"] + [f"- {e}" for e in errors]

    with open(os.path.join(run_dir, "SYNTHESE.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    tg = []
    for r in bets:
        d, fx = r["decision"], r["fixture"]
        tg += [
            f"APEX-TURF {run_label} — {fx['racecourse']}",
            f"{fx['meeting_code']}C{fx['race_number']} {fx['race_name'][:34]}",
            f"{fx['discipline']} {fx.get('distance_m','?')}m | "
            f"{int(r.get('runners_final') or 0) or '?'} partants | terrain {r.get('going') or '?'}",
            f"Pari: {d.get('bet_type')} ({d.get('betting_system')})",
            f"Cheval: {d.get('runner_number')} {d.get('runner_name','')}",
            f"Rapport mini: {fmt(as_float(d.get('min_acceptable_odds')))}"
            + (" (rapport estime, non ferme)" if d.get("market_odds_is_estimate") else ""),
            f"p_model: {fmt(as_float(d.get('p_model')), 4)} | Fair: {fmt(as_float(d.get('fair_odds')))}",
            f"Prelevement: {fmt(as_float(d.get('takeout_pct')))}%",
            f"Edge: {fmt(r['edge']['computed_edge_pct'])}%",
            f"Mise: {fmt(r.get('stake_final'))}u",
            "Analyse probabiliste, aucun gain garanti.",
            "",
        ]
    if not bets:
        tg = [f"APEX-TURF {run_label} — 0 pari retenu.",
              "Analyse probabiliste, aucun gain garanti."]
    with open(os.path.join(run_dir, "telegram.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(tg).rstrip() + "\n")

    payload = {"run": run_label, "mode": request.get("mode"),
               "window_utc": fixtures.get("window_utc"),
               "generated_at_utc": now_utc(), "bankroll_notes": notes, "races": []}
    for r in rows:
        rdir = os.path.join(run_dir, r["race_id"])
        payload["races"].append({
            "race_id": r["race_id"],
            "fixture_status": r["fixture"]["fixture_status"],
            "a8_envelope": read_json(os.path.join(rdir, "09_t8_decision.json")),
            "council_verdict": r.get("council"),
            "preflight": read_json(os.path.join(rdir, "11_preflight.json")),
            "final_verdict": r["verdict"], "blocking_gate": r["blocking_gate"],
            "stake_final": r.get("stake_final", 0.0),
            "edge_recomputed_pct": r["edge"]["computed_edge_pct"],
            "gate_log": r["gate_log"],
        })
    write_json(os.path.join(run_dir, "synthese.json"), payload)

    journal_rows = []
    for r in [x for x in rows if x["verdict"] != "PENDING"]:
        d, fx = r["decision"], r["fixture"]
        journal_rows.append({
            "date": fx.get("local_date") or (fx.get("off_time_utc") or "")[:10],
            "race_id": r["race_id"], "racecourse": fx["racecourse"],
            "country": fx["country"], "discipline": fx["discipline"],
            "distance_m": fx.get("distance_m", ""), "race_class": fx.get("race_class", ""),
            "field_size": int(r.get("runners_final") or fx.get("field_size_declared") or 0) or "",
            "going": r.get("going") or "", "tier": fx["tier"],
            "drs": r.get("drs") if r.get("drs") is not None else "",
            "vs": r.get("vs") if r.get("vs") is not None else "",
            "bet_type": d.get("bet_type", ""), "betting_system": d.get("betting_system", ""),
            "runner_number": d.get("runner_number", ""), "runner_name": d.get("runner_name", ""),
            "p_model": d.get("p_model", ""), "fair_odds": d.get("fair_odds", ""),
            "market_odds": d.get("market_odds", ""), "final_odds": "",
            "takeout_pct": d.get("takeout_pct", ""),
            "edge_pct": r["edge"]["computed_edge_pct"] if r["edge"]["computed_edge_pct"] is not None else "",
            "stake_units": r.get("stake_final", 0.0),
            "verdict": r["verdict"], "blocking_gate": r["blocking_gate"],
            "finish_position": "", "pnl_units": "",
        })
    added = append_journal(journal_rows)

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["verdict"].split(" ")[0]] = counts.get(r["verdict"].split(" ")[0], 0) + 1
    missing = sorted({m for r in rows for m in
                      ((read_json(os.path.join(run_dir, r["race_id"],
                                               "02_t1_integrity.json")) or {}
                        ).get("missing_fields") or [])})
    undocumented = [r["race_id"] for r in bets
                    if as_float(r["decision"].get("takeout_pct")) is None]

    print(json.dumps({
        "mode": request.get("mode"), "window_utc": fixtures.get("window_utc"),
        "races_total": len(fixtures.get("fixtures", [])),
        "counts": counts,
        "total_exposure_units": round(sum(r.get("stake_final", 0.0) for r in bets), 3),
        "bets_without_documented_takeout": undocumented,
        "journal_rows_added": added,
        "missing_data": missing,
        "schema_errors": errors,
    }, ensure_ascii=False, indent=2))
    return 1 if (errors or undocumented) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="APEX-TURF-LEAD")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="Résout la requête et crée le run")
    p.add_argument("--input", required=True)
    p.add_argument("--run")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("triage", help="Passe légère sur la qualité des données")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_triage)

    p = sub.add_parser("check", help="Évalue G0→G6 pour une course")
    p.add_argument("--race-dir", required=True)
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("finalize", help="Bankroll + livrables + journal")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_finalize)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
