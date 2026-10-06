"""Read-only APEX import. Preserve official selections and every known veto.

This is not a scanner, BSM model, risk ledger, or validation authority.
It never prices a different market from raw EV lists or confidence scores.
"""
from __future__ import annotations

from .contracts import Mission, canonical, digest, utc


def records_from_payload(payload):
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and "records" in payload:
        records = payload["records"]
    elif isinstance(payload, dict):
        records = [payload]
    else:
        raise ValueError("APEX import expects a record, list, or {records: [...]} object")
    if not isinstance(records, list) or any(not isinstance(x, dict) for x in records):
        raise ValueError("Every imported APEX record must be an object")
    return records


def mission_from_apex(record, *, source_uri, imported_at, as_of=None):
    utc(imported_at)
    as_of = as_of or imported_at
    utc(as_of)
    fixture = record.get("fixture", {})
    if not isinstance(fixture, dict):
        fixture = {}
    identifier = record.get("fixture_id", record.get("match_id", fixture.get("id", digest(record)[:16])))
    label = record.get("match", " / ".join(str(record.get(k, "?")) for k in ("home", "away")))
    fc = record.get("forecast", record)
    if not isinstance(fc, dict):
        raise ValueError("Malformed APEX forecast")
    off = fc.get("official_selection")
    vetoes = []
    for item in (record, fc, off if isinstance(off, dict) else {}):
        for flag in ("veto", "suspect", "instable", "integrity_blocked", "bet_blocked"):
            if item.get(flag):
                vetoes.append(flag)
        for name in ("vetoes", "veto_reasons", "blocking_issues"):
            values = item.get(name, [])
            if values:
                if not isinstance(values, list):
                    vetoes.append("malformed_" + name)
                else:
                    vetoes.extend(str(x) for x in values)
        integrity = item.get("asian_integrity")
        if isinstance(integrity, dict) and (integrity.get("suspect") or integrity.get("blocked")):
            vetoes.append("asian_integrity")
    evidence, claims = [], []
    origin = "apex-upstream:" + source_uri
    error = record.get("erreur") or record.get("error") or fc.get("erreur") or fc.get("error")
    if error:
        content = str(error)
        statement = next((line for line in reversed(content.splitlines()) if line.strip()), content)
        evidence.append({"evidence_id": "execution-log", "fact_key": "upstream_execution",
                         "value": "failed", "statement": statement, "content": content,
                         "source_uri": source_uri, "origin_ids": [origin],
                         "available_at": imported_at, "retrieved_at": imported_at,
                         "kind": "observation"})
        claims.append({"claim_id": "execution-failed", "statement": statement,
                       "evidence_ids": ["execution-log"], "kind": "fact"})
        vetoes.append("UPSTREAM_RUNTIME_ERROR")
    forecast = None
    if isinstance(off, dict) and not error:
        market = off.get("marche", off.get("market"))
        if market is not None and off.get("p") is not None:
            # No inferred validation from a status string. Original status remains metadata.
            odds = off.get("cote", off.get("odds"))
            forecast = {
                "event_id": f"{identifier}:{market}", "event": str(market),
                "probability": off["p"], "method": "APEX official_selection import",
                "method_version": str(fc.get("model_version", "unknown")),
                "validation_status": "unvalidated", "odds": odds,
                "odds_at": off.get("odds_at", fc.get("odds_at")),
                "kickoff": fc.get("kickoff_utc", fc.get("kickoff", fixture.get("date"))),
                "settlement": off.get("settlement", "unknown"),
                "synthetic": bool(fc.get("synthetic", False))}
            statement = canonical({"event": market, "probability": off["p"], "odds": odds})
            evidence.append({"evidence_id": "official-selection", "fact_key": "official_selection",
                             "value": {"event": market, "p": off["p"], "odds": odds},
                             "statement": statement, "content": statement, "source_uri": source_uri,
                             "origin_ids": [origin], "available_at": imported_at,
                             "retrieved_at": imported_at, "kind": "observation"})
    elif not error:
        statement = "No structured official_selection was supplied"
        evidence.append({"evidence_id": "selection-missing", "fact_key": "official_selection",
                         "value": None, "statement": statement, "content": statement,
                         "source_uri": source_uri, "origin_ids": [origin],
                         "available_at": imported_at, "retrieved_at": imported_at,
                         "kind": "observation", "critical": False})
        claims.append({"claim_id": "selection-required", "statement": "A structured official_selection is required",
                       "evidence_ids": [], "kind": "unknown", "critical": True})
    if isinstance(off, dict) and not error and forecast is None:
        vetoes.append("MALFORMED_OFFICIAL_SELECTION")
    payload = {"task_id": f"apex:{identifier}:{digest(record)[:12]}",
               "objective": f"Review upstream analytical dossier: {label}",
               "as_of": as_of, "mode": "analysis", "proposed_action": "report",
               "evidence": evidence, "claims": claims, "forecast": forecast,
               "requirements": {e["fact_key"]: 1 for e in evidence if e.get("critical", True)},
               "metadata": {"domain": "APEX", "input_sha256": digest(record),
                            "source_uri": source_uri, "imported_at": imported_at,
                            "upstream_model_status": str(fc.get("statut_modele", "unknown")),
                            "upstream_vetoes": sorted(set(vetoes)), "stake_pct": 0}}
    return Mission.from_dict(payload)
