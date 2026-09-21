#!/usr/bin/env python3
"""APEX-HOCKEY-LEAD — outillage mécanique de la chaîne hockey.

Le conducteur ne juge pas : il lit le champ `status` des JSON sur disque et
applique les gates. Ce module rend mécaniques les points qui ne doivent
jamais dépendre d'une lecture narrative :

    init      résout la requête (match | ligue | date) et crée le run
    triage    passe légère sur la qualité des données quand il y a trop de matchs
    check     valide l'enveloppe et évalue G0→G7 pour un match
    finalize  recalcule les edges, applique la bankroll, produit les livrables

Usage :
    python3 tools/apex_hockey_lead.py init     --input <input.yaml>
    python3 tools/apex_hockey_lead.py triage   --run runs_hockey/<run>
    python3 tools/apex_hockey_lead.py check    --match-dir runs_hockey/<run>/<match_id>
    python3 tools/apex_hockey_lead.py finalize --run runs_hockey/<run>
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import apex_hockey_discovery as discovery  # noqa: E402
from apex_common import (as_float, dig, load_input, now_utc, read_json,  # noqa: E402
                         recompute_edge, validate_envelope, write_json)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOURNAL = os.path.join(ROOT, "journal", "apex_hockey_journal.csv")

JOURNAL_COLUMNS = [
    "date", "match_id", "league", "tier", "drs", "vs", "home_goalie",
    "away_goalie", "goalie_status", "market_type", "line", "selection",
    "p_model", "fair_odds", "market_odds", "edge_pct", "stake_units",
    "verdict", "blocking_gate", "result_reg", "result_final", "went_ot",
    "pnl_units",
]

STEPS = [
    ("A0_ROUTER",     "00_routing.json"),
    ("A1_DATA",       "01_scraper.json"),
    ("A1_DATA",       "02_h1_integrity.json"),
    ("A2_CONTEXTE",   "03_h2_context_schedule.json"),
    ("A3_GOALIES",    "04_h3_goalies_lineups.json"),
    ("A4_TACTIQUE",   "05_h4_tactical_special_teams.json"),
    ("A5_PRICING",    "06_h5_pricing.json"),
    ("A6_RISQUE",     "07_h6_volatility.json"),
    ("A7_MARCHE",     "08_h7_market.json"),
    ("A8_DECISION",   "09_h8_decision.json"),
    ("A9_CONSEIL",    "10_h9_council.json"),
    ("A10_PREFLIGHT", "11_preflight.json"),
]

DRS_FLOOR = 60.0
PROB_TOLERANCE = 0.005
EDGE_TOLERANCE_PTS = 0.1
MIN_GAMES_EARLY_SEASON = 10
CONFIRMED = "CONFIRMED"


# --------------------------------------------------------------------------
# Gates
# --------------------------------------------------------------------------

def goalies_confirmed(a3) -> tuple[bool, str]:
    """(les deux partants confirmés, libellé du statut le plus faible)."""
    payload = (a3 or {}).get("payload") or {}
    statuses = []
    for side in ("home_goalie", "away_goalie"):
        statuses.append(((payload.get(side) or {}).get("status") or "UNKNOWN").upper())
    worst = "UNKNOWN" if "UNKNOWN" in statuses else (
        "PROBABLE" if "PROBABLE" in statuses else CONFIRMED)
    return all(s == CONFIRMED for s in statuses), worst


def validate_pricing(a5) -> list[str]:
    """Contrôles du protocole sur les probabilités de A5."""
    errors = []
    probs = dig((a5 or {}).get("payload") or {}, "probabilities", default={}) or {}
    trio = [as_float(probs.get(k)) for k in ("p_reg_home", "p_reg_draw", "p_reg_away")]
    if all(v is not None for v in trio):
        total = sum(trio)
        if abs(total - 1.0) > PROB_TOLERANCE:
            errors.append(f"06_h5_pricing.json: somme 1/X/2 réglementaire = {total:.4f} "
                          f"hors tolérance ±{PROB_TOLERANCE}")
    else:
        errors.append("06_h5_pricing.json: probabilités de temps réglementaire incomplètes")
    for flag in a5.get("flags", []) if a5 else []:
        if str(flag).startswith(("REG_SUM_ERROR", "ML_SUM_ERROR")):
            errors.append(f"06_h5_pricing.json: {flag}")
    return errors


def evaluate_gates(match_dir: str) -> dict:
    def f(name):
        return read_json(os.path.join(match_dir, name))

    errors, gate_log = [], []

    def pending(label):
        gate_log.append(f"{label} PENDING (fichier amont absent)")
        return {"blocking_gate": None, "verdict": "PENDING", "reason": None,
                "gate_log": gate_log, "schema_errors": errors,
                "next_step": next_step(match_dir), "complete": False}

    def blocked(gate, verdict, reason):
        return {"blocking_gate": gate, "verdict": verdict, "reason": reason,
                "gate_log": gate_log, "schema_errors": errors,
                "next_step": None, "complete": True}

    routing = f("00_routing.json")
    if routing is None:
        return pending("A0")
    errors += validate_envelope(routing, "00_routing.json")
    tier = dig(routing.get("payload") or {}, "tier", default="TIER_C")

    # --- G0 : collecte absente ------------------------------------------
    scraper = f("01_scraper.json")
    if scraper is None:
        return pending("G0")
    errors += validate_envelope(scraper, "01_scraper.json")
    if not (scraper.get("payload") or {}) or scraper.get("status") == "DATA_REQUEST":
        return blocked("G0", "DATA_REQUEST", "01_scraper.json vide ou statut DATA_REQUEST")
    gate_log.append("G0 PASS")

    # --- G1 : intégrité des données --------------------------------------
    h1 = f("02_h1_integrity.json")
    if h1 is None:
        return pending("G1")
    errors += validate_envelope(h1, "02_h1_integrity.json")
    drs = as_float(dig(h1.get("payload") or {}, "drs", "data_reliability_score"))
    if h1.get("status") == "ABORT":
        return blocked("G1", "NO_BET (data)", "H1 status = ABORT")
    if drs is not None and drs < DRS_FLOOR:
        return blocked("G1", "NO_BET (data)", f"DRS {drs} < {DRS_FLOOR}")
    gate_log.append(f"G1 PASS (DRS={drs})")

    # --- G2 : contexte / calendrier --------------------------------------
    h2 = f("03_h2_context_schedule.json")
    if h2 is None:
        return pending("G2")
    errors += validate_envelope(h2, "03_h2_context_schedule.json")
    p2 = h2.get("payload") or {}
    dead = "DEAD_RUBBER" in (h2.get("flags") or []) or dig(p2, "flag") == "DEAD_RUBBER"
    if dead and not dig(p2, "dead_rubber_compensated", "compensated", default=False):
        return blocked("G2", "NO_BET (context)", "DEAD_RUBBER non compensé")
    gate_log.append("G2 PASS")

    # --- G3, premier passage : A3 peut bloquer seul ----------------------
    h3 = f("04_h3_goalies_lineups.json")
    if h3 is None:
        return pending("G3")
    errors += validate_envelope(h3, "04_h3_goalies_lineups.json")
    confirmed, worst = goalies_confirmed(h3)
    if h3.get("status") == "WAIT_GOALIE":
        return blocked("G3", "WAIT_GOALIE", "A3 : partant non confirmé, value dépendante")
    gate_log.append(f"G3 partants={worst}"
                    f"{' PASS' if confirmed else ' — à revérifier à la décision'}")

    h4 = f("05_h4_tactical_special_teams.json")
    if h4 is None:
        return pending("A4")
    errors += validate_envelope(h4, "05_h4_tactical_special_teams.json")

    h5 = f("06_h5_pricing.json")
    if h5 is None:
        return pending("A5")
    errors += validate_envelope(h5, "06_h5_pricing.json")
    errors += validate_pricing(h5)

    # --- G4 : volatilité --------------------------------------------------
    h6 = f("07_h6_volatility.json")
    if h6 is None:
        return pending("G4")
    errors += validate_envelope(h6, "07_h6_volatility.json")
    if h6.get("status") in {"NO_BET", "LIVE_ONLY"}:
        return blocked("G4", h6["status"], f"H6 status = {h6['status']} (propagé tel quel)")
    gate_log.append("G4 PASS")

    # --- G5 : marché ------------------------------------------------------
    h7 = f("08_h7_market.json")
    if h7 is None:
        return pending("G5")
    errors += validate_envelope(h7, "08_h7_market.json")
    allowed = dig(h7.get("payload") or {}, "markets_with_value",
                  "allowed_markets_with_value", default=[]) or []
    if h7.get("status") == "NO_BET" or not allowed:
        return blocked("G5", "NO_BET (market)", "Aucun marché autorisé avec value confirmée")
    gate_log.append(f"G5 PASS ({len(allowed)} marché(s))")

    # --- A8 : décision ----------------------------------------------------
    h8 = f("09_h8_decision.json")
    if h8 is None:
        return pending("A8")
    errors += validate_envelope(h8, "09_h8_decision.json")
    p8 = h8.get("payload") or {}
    verdict = dig(p8, "verdict", default=h8.get("status"))
    edge = recompute_edge(p8.get("p_model"), p8.get("market_odds"),
                          p8.get("edge_pct"), EDGE_TOLERANCE_PTS)
    if edge["mismatch"]:
        errors.append(f"09_h8_decision.json: edge déclaré {edge['declared_edge_pct']} vs "
                      f"recalculé {edge['computed_edge_pct']} (écart {edge['delta']})")
    if not p8.get("market_type"):
        # Premier piège du hockey : moneyline (avec prolongation) confondue
        # avec le 3-way (temps réglementaire seul).
        errors.append("09_h8_decision.json: market_type absent — "
                      "prolongation incluse ou non, indéterminé")

    # --- G3, second passage : la value dépend-elle du gardien ? ----------
    if verdict == "BET" and p8.get("depends_on_goalie") and not confirmed:
        return blocked("G3", "WAIT_GOALIE",
                       f"value dépendante du gardien, partant le plus faible = {worst}")
    if verdict == "BET":
        gate_log.append("G3 PASS (value non dépendante ou partants confirmés)")

    # --- G6 : conseil (seulement si BET) ---------------------------------
    if verdict == "BET":
        h9 = f("10_h9_council.json")
        if h9 is None:
            return pending("G6")
        errors += validate_envelope(h9, "10_h9_council.json")
        council = dig(h9.get("payload") or {}, "council_verdict", "COUNCIL_VERDICT")
        if council == "VETO":
            return blocked("G6", "NO_BET (council)", "Conseil VETO — override A8")
        gate_log.append(f"G6 PASS (conseil={council})")
    else:
        gate_log.append(f"G6 SKIP (A8 verdict={verdict})")

    # --- G7 : preflight ---------------------------------------------------
    pre = f("11_preflight.json")
    if pre is None:
        return pending("G7")
    errors += validate_envelope(pre, "11_preflight.json")
    pre_flags = set(pre.get("flags") or [])
    pre_payload = pre.get("payload") or {}
    if pre.get("status") == "NO_BET" or pre_flags & {"NULL_MARGIN", "ABERRANT_MARGIN", "BRIER_FAIL"}:
        return blocked("G7", "NO_BET (preflight)",
                       "Marge nulle/aberrante ou Brier ≤ baseline naïve hockey")
    games = as_float(dig(pre_payload, "min_games_played", "games_played_min"))
    early = "EARLY_SEASON" in pre_flags or (games is not None and games < MIN_GAMES_EARLY_SEASON)
    if early:
        gate_log.append(f"G7 RETROGRADE (échantillon début de saison : {games} matchs)")
        verdict = "INDICATIF" if verdict == "BET" else verdict
    else:
        gate_log.append("G7 PASS")

    # TIER_C sans ligne sharp : plafonné à INDICATIF par le routage.
    if tier == "TIER_C" and verdict == "BET" and not dig(
            h7.get("payload") or {}, "sharp_reference", default=None):
        gate_log.append("TIER_C sans référence sharp → plafonné INDICATIF")
        verdict = "INDICATIF"

    return {"blocking_gate": None, "verdict": verdict, "reason": None,
            "gate_log": gate_log, "schema_errors": errors, "edge": edge,
            "next_step": None, "complete": True}


def next_step(match_dir: str):
    for agent, filename in STEPS:
        if read_json(os.path.join(match_dir, filename)) is None:
            return {"agent": agent, "expected_file": filename}
    return None


# --------------------------------------------------------------------------
# init
# --------------------------------------------------------------------------

def cmd_init(args) -> int:
    spec = load_input(args.input)
    request = discovery.normalize_request(spec)
    fixtures = discovery.discover(request)

    day = args.run or (request.get("local_date")
                       or request["window_from_utc"][:10])
    run_dir = os.path.join(ROOT, "runs_hockey", day)
    write_json(os.path.join(run_dir, "request.json"), request)
    write_json(os.path.join(run_dir, "fixtures.json"), fixtures)
    write_json(os.path.join(run_dir, "_input.json"), spec)
    ensure_journal()

    # --- G-1 : la chaîne ne démarre pas sans affiche exploitable ---------
    if fixtures["status"] != "OK":
        reason = {
            "EMPTY": "aucun match dans la fenêtre",
            "AMBIGUOUS": "équipes ambiguës — poser la question avec les candidats",
            "DATA_REQUEST": "calendrier de ligue injoignable",
        }[fixtures["status"]]
        print(json.dumps({"run_dir": os.path.relpath(run_dir, ROOT),
                          "blocking_gate": "G-1", "status": fixtures["status"],
                          "reason": reason,
                          "ambiguous_candidates": fixtures["ambiguous_candidates"],
                          "leagues_unreachable": fixtures["leagues_unreachable"]},
                         ensure_ascii=False, indent=2))
        return 3

    scheduled = [f for f in fixtures["fixtures"] if f["fixture_status"] == "SCHEDULED"]
    for fx in scheduled:
        mdir = os.path.join(run_dir, fx["match_id"])
        os.makedirs(mdir, exist_ok=True)
        write_json(os.path.join(mdir, "_match.json"), fx)
        sources = os.path.join(mdir, "sources.md")
        if not os.path.exists(sources):
            with open(sources, "w", encoding="utf-8") as fh:
                fh.write(f"# Sources — {fx['away']} @ {fx['home']}\n\n"
                         f"| URL | retrieved_at_utc | agent |\n|---|---|---|\n")
                for url in fx["source_urls"]:
                    fh.write(f"| {url} | {fixtures['generated_at_utc']} | AD_DISCOVERY |\n")

    max_matches = int(spec.get("max_matches", 12))
    print(json.dumps({
        "run_dir": os.path.relpath(run_dir, ROOT),
        "mode": request["mode"],
        "window_utc": fixtures["window_utc"],
        "scheduled": len(scheduled),
        "excluded": {s: sum(1 for f in fixtures["fixtures"] if f["fixture_status"] == s)
                     for s in ("POSTPONED", "EXCLUDED_STARTED", "EXCLUDED_TIER_C")},
        "leagues_unreachable": fixtures["leagues_unreachable"],
        "triage_required": len(scheduled) > max_matches,
        "max_matches": max_matches,
    }, ensure_ascii=False, indent=2))
    return 0


# --------------------------------------------------------------------------
# triage
# --------------------------------------------------------------------------

def cmd_triage(args) -> int:
    """Tri sur la QUALITÉ DES DONNÉES, jamais sur un edge supposé.

    Trier sur l'edge réintroduirait le biais que la chaîne doit éliminer :
    on choisirait les matchs qui semblent payants avant de les avoir
    instruits. On ne trie donc que sur DRS, gardiens confirmés et proximité
    du coup d'envoi.
    """
    run_dir = args.run if os.path.isabs(args.run) else os.path.join(ROOT, args.run)
    fixtures = read_json(os.path.join(run_dir, "fixtures.json")) or {}
    spec = read_json(os.path.join(run_dir, "_input.json")) or {}
    max_matches = int(spec.get("max_matches", 12))

    rows = []
    for fx in fixtures.get("fixtures", []):
        if fx["fixture_status"] != "SCHEDULED":
            continue
        mdir = os.path.join(run_dir, fx["match_id"])
        h1 = read_json(os.path.join(mdir, "02_h1_integrity.json")) or {}
        h3 = read_json(os.path.join(mdir, "04_h3_goalies_lineups.json")) or {}
        h7 = read_json(os.path.join(mdir, "08_h7_market.json")) or {}
        drs = as_float(dig(h1.get("payload") or {}, "drs", "data_reliability_score"))
        confirmed, worst = goalies_confirmed(h3) if h3 else (False, "UNKNOWN")
        sharp = bool(dig(h7.get("payload") or {}, "sharp_reference", default=None)) \
            or bool(fx.get("odds_sport_key"))

        reasons = []
        if drs is not None and drs < DRS_FLOOR:
            reasons.append(f"DRS {drs} < {DRS_FLOOR}")
        if fx["tier"] == "TIER_C":
            reasons.append("ligue TIER_C")
        if not sharp:
            reasons.append("aucune cote sharp disponible")

        rows.append({"match_id": fx["match_id"], "league": fx["league"],
                     "tier": fx["tier"], "drs": drs, "goalies": worst,
                     "goalies_confirmed": confirmed,
                     "puck_drop_utc": fx["puck_drop_utc"],
                     "excluded_reasons": reasons})

    kept = [r for r in rows if not r["excluded_reasons"]]
    kept.sort(key=lambda r: (-(r["drs"] if r["drs"] is not None else -1),
                             not r["goalies_confirmed"], r["puck_drop_utc"]))
    selected = kept[:max_matches]
    selected_ids = {r["match_id"] for r in selected}

    for row in rows:
        if row["match_id"] in selected_ids:
            row["triage"] = "SELECTED"
        elif row["excluded_reasons"]:
            row["triage"] = "TRIAGED_OUT"
            row["triage_reason"] = " ; ".join(row["excluded_reasons"])
        else:
            row["triage"] = "TRIAGED_OUT"
            row["triage_reason"] = f"hors des {max_matches} premiers au classement qualité"

    result = {"agent": "AT_TRIAGE", "max_matches": max_matches,
              "criteria": "DRS décroissant, puis gardiens confirmés, puis coup d'envoi le plus proche",
              "selected": [r["match_id"] for r in selected],
              "rows": rows, "generated_at_utc": now_utc()}
    write_json(os.path.join(run_dir, "triage.json"), result)
    print(json.dumps({"selected": len(selected), "triaged_out": len(rows) - len(selected),
                      "selected_ids": result["selected"]}, ensure_ascii=False, indent=2))
    return 0


def cmd_check(args) -> int:
    result = evaluate_gates(args.match_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result["schema_errors"] else 0


# --------------------------------------------------------------------------
# finalize
# --------------------------------------------------------------------------

def ensure_journal() -> None:
    os.makedirs(os.path.dirname(JOURNAL), exist_ok=True)
    if not os.path.exists(JOURNAL):
        with open(JOURNAL, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(JOURNAL_COLUMNS)


def append_journal(rows: list[dict]) -> int:
    ensure_journal()
    with open(JOURNAL, encoding="utf-8") as fh:
        existing = {r["match_id"] for r in csv.DictReader(fh) if r.get("match_id")}
    new = [r for r in rows if r["match_id"] not in existing]
    with open(JOURNAL, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=JOURNAL_COLUMNS)
        for row in new:
            writer.writerow({c: row.get(c, "") for c in JOURNAL_COLUMNS})
    return len(new)


def collect(run_dir: str) -> list[dict]:
    fixtures = read_json(os.path.join(run_dir, "fixtures.json")) or {}
    triage = read_json(os.path.join(run_dir, "triage.json")) or {}
    triage_by_id = {r["match_id"]: r for r in triage.get("rows", [])}

    rows = []
    for fx in fixtures.get("fixtures", []):
        mdir = os.path.join(run_dir, fx["match_id"])
        entry = {"fixture": fx, "match_id": fx["match_id"],
                 "triage": triage_by_id.get(fx["match_id"], {})}

        if fx["fixture_status"] != "SCHEDULED":
            entry.update({"verdict": fx["fixture_status"], "blocking_gate": "",
                          "reason": "exclu par DISCOVERY", "gate_log": [],
                          "schema_errors": [], "decision": {}, "drs": None,
                          "vs": None, "goalies": {}, "edge": recompute_edge(None, None),
                          "confidence": 0.0})
            rows.append(entry)
            continue

        if entry["triage"].get("triage") == "TRIAGED_OUT":
            entry.update({"verdict": "TRIAGED_OUT", "blocking_gate": "",
                          "reason": entry["triage"].get("triage_reason", ""),
                          "gate_log": [], "schema_errors": [], "decision": {},
                          "drs": entry["triage"].get("drs"), "vs": None,
                          "goalies": {}, "edge": recompute_edge(None, None),
                          "confidence": 0.0})
            rows.append(entry)
            continue

        gates = evaluate_gates(mdir)
        h1 = read_json(os.path.join(mdir, "02_h1_integrity.json")) or {}
        h3 = read_json(os.path.join(mdir, "04_h3_goalies_lineups.json")) or {}
        h6 = read_json(os.path.join(mdir, "07_h6_volatility.json")) or {}
        h8 = read_json(os.path.join(mdir, "09_h8_decision.json")) or {}
        h9 = read_json(os.path.join(mdir, "10_h9_council.json")) or {}
        p8 = h8.get("payload") or {}
        _, worst = goalies_confirmed(h3) if h3 else (False, "UNKNOWN")
        entry.update({
            "verdict": gates["verdict"], "blocking_gate": gates["blocking_gate"] or "",
            "reason": gates["reason"] or "", "gate_log": gates["gate_log"],
            "schema_errors": gates["schema_errors"],
            "decision": p8,
            "drs": as_float(dig(h1.get("payload") or {}, "drs", "data_reliability_score")),
            "vs": as_float(dig(h6.get("payload") or {}, "vs", "volatility_score")),
            "goalies": {
                "home": ((h3.get("payload") or {}).get("home_goalie") or {}),
                "away": ((h3.get("payload") or {}).get("away_goalie") or {}),
                "worst_status": worst,
            },
            "council": dig(h9.get("payload") or {}, "council_verdict", "COUNCIL_VERDICT"),
            "edge": gates.get("edge") or recompute_edge(p8.get("p_model"),
                                                        p8.get("market_odds"),
                                                        p8.get("edge_pct")),
            "confidence": as_float(h8.get("confidence")) or 0.0,
        })
        rows.append(entry)
    return rows


def apply_bankroll(rows: list[dict], spec: dict) -> list[str]:
    notes = []
    max_stake = as_float(spec.get("max_stake_per_bet")) or 2.0
    max_daily = as_float(spec.get("max_daily_exposure")) or 5.0
    bets = [r for r in rows if r["verdict"] == "BET"]

    for r in bets:
        stake = as_float(r["decision"].get("stake_units")) or 0.0
        r["stake_final"] = min(stake, max_stake)
        if stake > max_stake:
            notes.append(f"{r['match_id']} : mise {stake}u plafonnée à {max_stake}u")

    # Un seul pari par match : moneyline, puck line et total du même match
    # portent le même résultat sous-jacent et se corrèlent presque totalement.
    by_match: dict[str, dict] = {}
    for r in sorted(bets, key=lambda x: -(x["edge"]["computed_edge_pct"] or -999)):
        if r["match_id"] in by_match:
            r["stake_final"] = 0.0
            r["verdict"] = "NO_BET (corrélé)"
            notes.append(f"{r['match_id']} : second marché écarté (corrélé au premier)")
        else:
            by_match[r["match_id"]] = r

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
            notes.append(f"{r['match_id']} : mise réduite de {cut}u (exposition > {max_daily}u)")
        notes.append(f"Exposition ramenée de {total:.2f}u à "
                     f"{sum(r['stake_final'] for r in kept):.2f}u")
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
        f"# APEX-HOCKEY — Synthèse du run {run_label}", "",
        f"- **Mode d'entrée** : `{request.get('mode','?')}`",
        f"- **Fenêtre UTC couverte** : {fixtures.get('window_utc',{}).get('from','?')} → "
        f"{fixtures.get('window_utc',{}).get('to','?')}",
        f"- **Fuseau de saisie** : {request.get('timezone_input','?')}",
        f"- **Affiches trouvées** : {len(fixtures.get('fixtures', []))}", "",
        "| Match | Ligue | Tier | DRS | VS | Gardiens | Marché | Type | p_model | Fair | Cote | Edge % | Mise (u) | Verdict | Gate |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        fx, d = r["fixture"], r["decision"]
        goalies = r.get("goalies") or {}
        g_label = "—"
        if goalies.get("home") or goalies.get("away"):
            g_label = (f"{(goalies.get('home') or {}).get('name','?')} / "
                       f"{(goalies.get('away') or {}).get('name','?')} "
                       f"({goalies.get('worst_status','?')})")
        lines.append("| {} @ {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            fx["away"], fx["home"], fx["league"], fx["tier"],
            fmt(r.get("drs"), 1), fmt(r.get("vs"), 1), g_label,
            d.get("selection", "—") or "—", d.get("market_type", "—") or "—",
            fmt(as_float(d.get("p_model")), 3), fmt(as_float(d.get("fair_odds"))),
            fmt(as_float(d.get("market_odds"))), fmt(r["edge"]["computed_edge_pct"]),
            fmt(r.get("stake_final")), r["verdict"], r["blocking_gate"] or "—"))

    bets = [r for r in rows if r["verdict"] == "BET" and r.get("stake_final", 0) > 0]
    if bets:
        lines += ["", "## Conditions d'invalidation"]
        for r in bets:
            d, fx = r["decision"], r["fixture"]
            lines += [
                "", f"### {fx['away']} @ {fx['home']} — {d.get('market_type')} "
                    f"/ {d.get('selection')}",
                f"- Cote minimale acceptable : **{fmt(as_float(d.get('min_acceptable_odds')))}** "
                f"({d.get('bookmaker','?')})",
                f"- Prolongation {'INCLUSE' if str(d.get('market_type','')).endswith('INC_OT') or d.get('market_type') == 'PUCKLINE' else 'EXCLUE'} "
                f"dans ce marché",
                f"- Dépend du gardien partant : {'oui' if d.get('depends_on_goalie') else 'non'}",
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
            step = next_step(os.path.join(run_dir, r["match_id"])) or {}
            lines.append(f"- {r['match_id']} — prochain agent : {step.get('agent','?')}")

    if fixtures.get("leagues_unreachable"):
        lines += ["", "## Ligues injoignables"] + [
            f"- {u['league']} : {u['reason']}" for u in fixtures["leagues_unreachable"]]

    errors = [e for r in rows for e in r["schema_errors"]]
    if errors:
        lines += ["", "## Anomalies de schéma"] + [f"- {e}" for e in errors]

    with open(os.path.join(run_dir, "SYNTHESE.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    tg = []
    for r in bets:
        d, fx, g = r["decision"], r["fixture"], r.get("goalies") or {}
        inc_ot = "avec prolongation" if d.get("market_type") in (
            "ML_INC_OT", "TOTAL_INC_OT", "PUCKLINE") else "temps reglementaire"
        tg += [
            f"APEX-HOCKEY {run_label} — {fx['away']} @ {fx['home']}",
            f"Ligue: {fx['league']} ({fx['tier']})",
            f"Gardiens: {(g.get('home') or {}).get('name','?')} / "
            f"{(g.get('away') or {}).get('name','?')} [{g.get('worst_status','?')}]",
            f"Marche: {d.get('market_type')} — {d.get('selection')}"
            + (f" {d.get('line')}" if d.get("line") is not None else ""),
            f"Portee: {inc_ot}",
            f"Cote: {fmt(as_float(d.get('market_odds')))} ({d.get('bookmaker','?')})",
            f"Cote mini: {fmt(as_float(d.get('min_acceptable_odds')))}",
            f"p_model: {fmt(as_float(d.get('p_model')), 3)} | Fair: {fmt(as_float(d.get('fair_odds')))}",
            f"Edge: {fmt(r['edge']['computed_edge_pct'])}%",
            f"Mise: {fmt(r.get('stake_final'))}u",
            "Analyse probabiliste, aucun gain garanti.",
            "",
        ]
    if not bets:
        tg = [f"APEX-HOCKEY {run_label} — 0 pari retenu.",
              "Analyse probabiliste, aucun gain garanti."]
    with open(os.path.join(run_dir, "telegram.txt"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(tg).rstrip() + "\n")

    payload = {"run": run_label, "mode": request.get("mode"),
               "window_utc": fixtures.get("window_utc"),
               "generated_at_utc": now_utc(), "bankroll_notes": notes, "matches": []}
    for r in rows:
        mdir = os.path.join(run_dir, r["match_id"])
        payload["matches"].append({
            "match_id": r["match_id"],
            "fixture_status": r["fixture"]["fixture_status"],
            "a8_envelope": read_json(os.path.join(mdir, "09_h8_decision.json")),
            "council_verdict": r.get("council"),
            "preflight": read_json(os.path.join(mdir, "11_preflight.json")),
            "final_verdict": r["verdict"], "blocking_gate": r["blocking_gate"],
            "stake_final": r.get("stake_final", 0.0),
            "edge_recomputed_pct": r["edge"]["computed_edge_pct"],
            "gate_log": r["gate_log"],
        })
    write_json(os.path.join(run_dir, "synthese.json"), payload)

    journal_rows = []
    for r in [x for x in rows if x["verdict"] != "PENDING"]:
        d, fx, g = r["decision"], r["fixture"], r.get("goalies") or {}
        journal_rows.append({
            "date": fx["puck_drop_utc"][:10], "match_id": r["match_id"],
            "league": fx["league"], "tier": fx["tier"],
            "drs": r.get("drs") if r.get("drs") is not None else "",
            "vs": r.get("vs") if r.get("vs") is not None else "",
            "home_goalie": (g.get("home") or {}).get("name", ""),
            "away_goalie": (g.get("away") or {}).get("name", ""),
            "goalie_status": g.get("worst_status", ""),
            "market_type": d.get("market_type", ""), "line": d.get("line", ""),
            "selection": d.get("selection", ""), "p_model": d.get("p_model", ""),
            "fair_odds": d.get("fair_odds", ""), "market_odds": d.get("market_odds", ""),
            "edge_pct": r["edge"]["computed_edge_pct"] if r["edge"]["computed_edge_pct"] is not None else "",
            "stake_units": r.get("stake_final", 0.0),
            "verdict": r["verdict"], "blocking_gate": r["blocking_gate"],
            "result_reg": "", "result_final": "", "went_ot": "", "pnl_units": "",
        })
    added = append_journal(journal_rows)

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["verdict"].split(" ")[0]] = counts.get(r["verdict"].split(" ")[0], 0) + 1
    missing = sorted({m for r in rows for m in
                      ((read_json(os.path.join(run_dir, r["match_id"],
                                               "02_h1_integrity.json")) or {}
                        ).get("missing_fields") or [])})
    unconfirmed_bets = [r["match_id"] for r in bets
                        if r["decision"].get("depends_on_goalie")
                        and (r.get("goalies") or {}).get("worst_status") != CONFIRMED]

    print(json.dumps({
        "mode": request.get("mode"), "window_utc": fixtures.get("window_utc"),
        "fixtures_total": len(fixtures.get("fixtures", [])),
        "counts": counts,
        "total_exposure_units": round(sum(r.get("stake_final", 0.0) for r in bets), 3),
        "bets_on_unconfirmed_goalie": unconfirmed_bets,
        "journal_rows_added": added,
        "missing_data": missing,
        "schema_errors": errors,
    }, ensure_ascii=False, indent=2))
    return 1 if (errors or unconfirmed_bets) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="APEX-HOCKEY-LEAD")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="Résout la requête et crée le run")
    p.add_argument("--input", required=True)
    p.add_argument("--run", help="Force le nom du run")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("triage", help="Passe légère sur la qualité des données")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_triage)

    p = sub.add_parser("check", help="Évalue G0→G7 pour un match")
    p.add_argument("--match-dir", required=True)
    p.set_defaults(func=cmd_check)

    p = sub.add_parser("finalize", help="Bankroll + livrables + journal")
    p.add_argument("--run", required=True)
    p.set_defaults(func=cmd_finalize)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
