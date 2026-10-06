"""Pure role handlers. Deterministic checks, no network or external side effects.

LLM profiles are separate host-loaded files. These handlers do not impersonate
independent models and never turn a number of agreeing roles into confidence.
"""
from __future__ import annotations

import hashlib
import math
import random
import re
from dataclasses import asdict

from .contracts import AgentResult, Finding, Mission, canonical, finite_number, utc


def result(role, findings=(), **metrics):
    return AgentResult(role, "complete", tuple(findings), metrics)


def _all_findings(context):
    findings = list(context.get("findings", []))
    for item in context.get("results", {}).values():
        findings.extend(item.findings)
    return list({(x.code, x.message, x.evidence_ids): x for x in findings}.values())


def _sensor_metrics(context):
    item = context.get("results", {}).get("SENSOR")
    return item.metrics if item else {}


def _usable(mission, context):
    ids = set(_sensor_metrics(context).get("admissible_evidence_ids", []))
    return [e for e in mission.evidence if e.evidence_id in ids]


def origin_groups(evidence):
    """Connected origins OR identical content are one conservative evidence family."""
    groups = []
    for e in evidence:
        if not e.origin_ids:
            continue
        origins = set(e.origin_ids)
        hashes = {e.content_hash}
        merged = []
        for group in groups:
            if group[0] & origins or group[1] & hashes:
                origins |= group[0]; hashes |= group[1]
            else:
                merged.append(group)
        # A merge can now connect to an earlier group: close transitively.
        changed = True
        while changed:
            changed = False
            keep = []
            for group in merged:
                if group[0] & origins or group[1] & hashes:
                    origins |= group[0]; hashes |= group[1]; changed = True
                else:
                    keep.append(group)
            merged = keep
        groups = merged + [(origins, hashes)]
    return [sorted(origins) for origins, _ in groups]


def sensor(mission: Mission, context: dict):
    findings, admissible, checks = [], [], []
    as_of = utc(mission.as_of)
    for e in mission.evidence:
        errors = []
        ids = (e.evidence_id,)
        if hashlib.sha256(e.content.encode("utf-8")).hexdigest() != e.content_hash:
            errors.append(Finding("CONTENT_HASH_MISMATCH", "Content hash does not match supplied content", "block", ids))
        if not e.content.strip() or e.statement.casefold() not in e.content.casefold():
            errors.append(Finding("UNSUPPORTED_CLAIM", "Evidence statement is not present in its source content", "block", ids))
        if utc(e.available_at) > as_of or utc(e.retrieved_at) > as_of:
            errors.append(Finding("FUTURE_INFORMATION", "Evidence was not available and captured by decision time", "block", ids))
        if utc(e.available_at) > utc(e.retrieved_at):
            errors.append(Finding("INVALID_TEMPORAL_ORDER", "Availability follows retrieval", "block", ids))
        if e.valid_until and utc(e.valid_until) <= as_of:
            errors.append(Finding("STALE_EVIDENCE", "Evidence has expired for this decision", "collect", ids))
        if e.kind in ("rumor", "inference"):
            errors.append(Finding("UNVERIFIED_EVIDENCE", "Rumor or inference cannot establish a critical fact", "collect", ids))
        if not e.origin_ids:
            errors.append(Finding("UNKNOWN_ORIGIN", "Primary origin is unknown; independence cannot be assumed", "collect", ids))
        if not e.critical:
            errors = [Finding(x.code, x.message, "warn", x.evidence_ids) for x in errors]
        findings.extend(errors)
        accepted = not errors  # Warning-only evidence is still excluded as factual support.
        checks.append({"evidence_id": e.evidence_id, "admissible": accepted,
                       "reason_codes": [x.code for x in errors]})
        if accepted:
            admissible.append(e.evidence_id)
    if not admissible:
        findings.append(Finding("MISSING_EVIDENCE", "No admissible source content supplied", "collect"))
    counts = {}
    for fact_key, minimum in mission.requirements.items():
        sources = [e for e in mission.evidence if e.fact_key == fact_key and e.evidence_id in admissible]
        groups = origin_groups(sources)
        counts[fact_key] = {"origin_groups": groups, "count": len(groups), "required": minimum}
        if len(groups) < minimum:
            code = "CORRELATED_EVIDENCE" if len(sources) >= minimum else "MISSING_EVIDENCE"
            findings.append(Finding(code, f"{fact_key}: {len(groups)} known source families; {minimum} required", "collect",
                                    tuple(e.evidence_id for e in sources)))
    return result("SENSOR", findings, admissible_evidence_ids=admissible,
                  evidence_admissibility=checks, origin_coverage=counts,
                  source_verification="supplied-content-only; transport and semantic truth not verified")


def memory(mission, context):
    records = context.get("memory_records", [])
    return result("MEMORY", visible_entries=len(records), categories=["working", "episodic", "semantic", "failure"],
                  automatic_parameter_updates=False)


def auditor(mission, context):
    findings = []
    admissible = {e.evidence_id: e for e in _usable(mission, context)}
    supplied = {e.evidence_id: e for e in mission.evidence}
    for claim in mission.claims:
        sources = [admissible[x] for x in claim.evidence_ids if x in admissible]
        if claim.kind == "fact" and (not sources or not any(
                claim.statement.casefold() in e.content.casefold() for e in sources)):
            previously_supported = any(x in supplied and claim.statement.casefold() in supplied[x].content.casefold()
                                       for x in claim.evidence_ids)
            code = "UNAVAILABLE_SUPPORT" if previously_supported else "UNSUPPORTED_CLAIM"
            severity = ("collect" if previously_supported else "block") if claim.critical else "warn"
            findings.append(Finding(code, f"{claim.claim_id}: fact lacks admissible supporting source content",
                                    severity, claim.evidence_ids))
        if claim.kind in ("assumption", "unknown") and claim.critical:
            findings.append(Finding("UNRESOLVED_ASSUMPTION", f"{claim.claim_id}: critical assumption requires evidence", "collect", claim.evidence_ids))
    for role, response in context.get("results", {}).items():
        if response.status == "failed":
            findings.append(Finding("AGENT_FAILED", f"Required role failed: {role}"))
    decision = context.get("results", {}).get("DECISION")
    if decision and decision.metrics.get("verdict") == "ACCEPT" and any(
            f.severity in ("block", "collect", "wait") for f in _all_findings(context)):
        findings.append(Finding("AUDIT_REJECTED", "An accepted decision contains unresolved blocking issues"))
    return result("AUDITOR", findings, numerical_input_validation="strict",
                  claim_verification="literal-support-only", external_actions_checked=True)


def pattern(mission, context):
    facts, findings = {}, []
    for e in _usable(mission, context):
        facts.setdefault(e.fact_key, []).append(e)
    contradictions = []
    for key, sources in facts.items():
        if len({canonical(e.value) for e in sources}) > 1:
            contradictions.append(key)
            findings.append(Finding("CONTRADICTORY_EVIDENCE", f"Incompatible values for {key}; resolve before acting", "collect",
                                    tuple(e.evidence_id for e in sources)))
    return result("PATTERN", findings, fact_keys=sorted(facts), contradictions=contradictions,
                  probabilities_invented=False)


def forecast(mission, context):
    f = mission.forecast
    if not f:
        return result("FORECAST", forecast=None, model_status="not_applicable")
    findings = []
    if f.validation_status == "quarantined":
        findings.append(Finding("MODEL_QUARANTINED", "Model is quarantined; quantitative action blocked"))
    elif f.validation_status != "validated":
        findings.append(Finding("MODEL_UNVALIDATED", "Prediction is exploratory; no validated recommendation", "warn"))
    else:
        findings.append(Finding("VALIDATION_NOT_VERIFIED", "A declared validation flag is not a checked validation artifact", "warn"))
    if f.synthetic:
        findings.append(Finding("SYNTHETIC_FORECAST", "Synthetic example, excluded from performance evidence", "warn"))
    if f.kickoff and utc(f.kickoff) <= utc(mission.as_of):
        findings.append(Finding("PREMATCH_CUTOFF_PASSED", "Pre-match forecast is at or after kickoff", "block"))
    ev = None
    if f.odds is not None:
        if not f.odds_at:
            findings.append(Finding("MISSING_ODDS_TIME", "Price has no timestamp", "collect"))
        elif utc(f.odds_at) > utc(mission.as_of):
            findings.append(Finding("FUTURE_INFORMATION", "Price follows decision time", "block"))
        if f.settlement != "binary":
            findings.append(Finding("NON_BINARY_SETTLEMENT", "Binary EV is not applicable to refunds or partial settlement", "warn"))
        else:
            ev = f.probability * f.odds - 1.0
    return result("FORECAST", findings, forecast=asdict(f), probability=f.probability,
                  probability_adjusted=False, arithmetic_ev=ev, ev_validated=False,
                  model_status="quarantined" if f.validation_status == "quarantined" else "validation_not_verified")


def context_agent(mission, context):
    findings = []
    psychological = []
    for c in mission.claims:
        if c.psychology:
            psychological.append(c.claim_id)
            findings.append(Finding("UNVERIFIABLE_PSYCHOLOGY",
                            f"{c.claim_id}: an attributed statement does not establish an internal mental state",
                            "collect" if c.critical else "warn", c.evidence_ids))
    return result("CONTEXT", findings, psychology_claims=psychological,
                  probability_adjustments=[], context_effects="hypotheses unless separately validated")


def skeptic(mission, context):
    unresolved = [f.code for f in _all_findings(context) if f.severity != "warn"]
    hypotheses = ["The candidate follows the supplied facts",
                  "A competing explanation fits the same facts",
                  "A critical observation is missing",
                  "Historical relationships no longer hold"]
    return result("SKEPTIC", unresolved_issues=sorted(set(unresolved)), alternative_hypotheses=hypotheses,
                  competing_probability=None)


def simulator(mission, context):
    f = mission.forecast
    if not f:
        return result("SIMULATOR", simulations=0, scenarios="qualitative alternatives only; no probability supplied")
    if f.settlement != "binary":
        return result("SIMULATOR", simulations=0, reason="Full settlement distribution required")
    # Seeded samples evaluate supplied p numerically, not the quality of that p.
    n = 10000
    seed = int(hashlib.sha256((mission.task_id + f.event_id).encode()).hexdigest()[:16], 16)
    rng = random.Random(seed)
    successes = sum(rng.random() < f.probability for _ in range(n))
    probabilities = f.sensitivity_probabilities
    ev_range = None
    if probabilities and f.odds:
        evs = [p * f.odds - 1 for p in probabilities]
        ev_range = [min(evs), max(evs)]
    return result("SIMULATOR", simulations=n, seed=seed, success_frequency=successes/n,
                  sampling_standard_error=math.sqrt(f.probability*(1-f.probability)/n),
                  sensitivity_probabilities=list(probabilities), sensitivity_ev_range=ev_range,
                  assumptions_validated=False, probability_interval=None,
                  explanation="Sampling error is not predictive uncertainty; simulations do not validate input p")


def red_team(mission, context):
    findings = []
    for e in mission.evidence:
        if re.search(r"(ignore (previous|all)|disable risk|envoie le ticket|désactive risk|send the ticket)", e.content, re.I):
            findings.append(Finding("UNTRUSTED_SOURCE_INSTRUCTION", "Source text contains instructions; treated as data only", "warn", (e.evidence_id,)))
    upstream = mission.metadata.get("upstream_vetoes", [])
    if not isinstance(upstream, (list, tuple)) or any(not isinstance(x, str) for x in upstream):
        findings.append(Finding("INVALID_UPSTREAM_VETOES", "Malformed upstream veto contract"))
    elif upstream:
        findings.append(Finding("UPSTREAM_VETO", "Upstream vetoes remain blocking: " + ", ".join(upstream)))
    return result("RED TEAM", findings, unresolved_critical_objections=[f.code for f in _all_findings(context)
                    if f.severity in ("block", "collect", "wait")], source_text_executed=False)


def risk(mission, context):
    findings = []
    if mission.proposed_action != "report":
        findings.append(Finding("ACTION_NOT_AUTHORIZED", "This runtime permits analytical reports only"))
    stake = mission.metadata.get("stake_pct", 0)
    try:
        stake = finite_number(stake, "stake_pct", 0)
        if stake > 0:
            findings.append(Finding("RISK_LIMIT", "Report runtime has zero financial exposure budget"))
    except ValueError:
        findings.append(Finding("INVALID_NUMBER", "Invalid requested financial exposure"))
    return result("RISK", findings, stake_pct=0.0, financial_execution_allowed=False,
                  proposed_action=mission.proposed_action, allowed_actions=["report"])


def decision(mission, context):
    findings = _all_findings(context)
    if any(f.severity == "block" for f in findings):
        verdict = "REJECT"
    elif any(f.severity in ("collect", "wait") for f in findings):
        verdict = "WAIT" if mission.resume_condition else "COLLECT_MORE"
    else:
        verdict = "ACCEPT"
    quality = "insufficient" if verdict != "ACCEPT" else "adequate"
    return result("DECISION", verdict=verdict, reason_codes=sorted({f.code for f in findings}),
                  reasons=[f.message for f in findings], evidence_quality=quality,
                  recommendation_verdict="WATCH" if mission.forecast else "NOT_APPLICABLE",
                  execution_allowed=verdict == "ACCEPT" and mission.proposed_action == "report",
                  financial_execution_allowed=False, stake_pct=0.0,
                  resume_condition=mission.resume_condition if verdict == "WAIT" else None)


def meta(mission, context):
    usable = _usable(mission, context)
    groups = origin_groups(usable)
    material = [f.code for f in _all_findings(context) if f.severity in ("block", "collect", "wait")]
    return result("META", known_origin_groups=groups, family_count=len(groups),
                  supplied_documents=len(mission.evidence), reuse_detected=len(usable) > len(groups),
                  independence_proven=False, unresolved_issues=sorted(set(material)),
                  role_outputs_are_independent_evidence=False,
                  confidence_percentage=None, automatic_parameter_updates=False)


def executor(mission, context):
    d = context.get("results", {}).get("DECISION")
    allowed = bool(d and d.metrics.get("execution_allowed") and mission.proposed_action == "report")
    return result("EXECUTOR", action="report", produced=allowed, external_side_effects=False,
                  idempotency_key=mission.task_id, financial_execution_allowed=False)


def learner(mission, context):
    return AgentResult("LEARNER", "deferred", (), {
        "resume_condition": "A confirmed outcome is recorded in MemoryStore.settle",
        "automatic_parameter_updates": False})


HANDLERS = {"SENSOR": sensor, "MEMORY": memory, "PATTERN": pattern, "FORECAST": forecast,
            "CONTEXT": context_agent, "SKEPTIC": skeptic, "SIMULATOR": simulator,
            "RED TEAM": red_team, "RISK": risk, "DECISION": decision, "AUDITOR": auditor,
            "META": meta, "EXECUTOR": executor, "LEARNER": learner,
            "ORION-Core": lambda m, c: result("ORION-Core", max_workers=4)}


def get_agent(role):
    return HANDLERS[role]
