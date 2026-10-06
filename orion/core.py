"""Bounded DAG orchestration; role outputs never constitute independent votes."""
from __future__ import annotations

import copy
import datetime as dt
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Callable

from .agents import get_agent
from .contracts import AgentResult, Finding, Mission, ROLES, SCHEMA_VERSION, canonical, digest, finite_number, utc
from .memory import MemoryStore


def _findings(items: list[Finding]) -> list[Finding]:
    return list({(x.code, x.message, x.severity, x.evidence_ids): x for x in items}.values())


def trusted_input_checks(mission: Mission) -> tuple[dict[str, AgentResult], list[Finding]]:
    """Mandatory built-in checks, independent of replaceable role proposals.

    They verify supplied content and its chronology, not remote source truth.
    Python extensions remain trusted executable code, not sandboxed agents.
    """
    checked: dict[str, AgentResult] = {}
    findings: list[Finding] = []
    payload = Mission.from_dict(mission.to_dict()).to_dict()
    for role in ("SENSOR", "PATTERN", "AUDITOR", "CONTEXT"):
        try:
            response = get_agent(role)(Mission.from_dict(copy.deepcopy(payload)), {
                "results": copy.deepcopy(checked), "findings": tuple(copy.deepcopy(findings)),
                "memory_records": [], "mode": "analysis", "stage": "trusted_input_checks",
            })
            if not isinstance(response, AgentResult) or response.role != role or response.status != "complete":
                raise ValueError("Mandatory input check did not complete")
            checked[role] = response
            findings.extend(response.findings)
        except Exception as exc:
            findings.append(Finding("TRUSTED_INPUT_CHECK_FAILED",
                                    f"Mandatory input check failed: {role} ({type(exc).__name__})"))
    return checked, _findings(findings)


class OrionCore:
    """Run immutable input through fifteen logical roles, at most four workers.

    Injected handlers must remain pure and terminate. This v1 has no provider,
    network, autonomous financial execution, or background scheduling.
    """

    def __init__(self, memory: MemoryStore | None = None, max_workers: int = 4,
                 handlers: dict[str, Callable] | None = None) -> None:
        if isinstance(max_workers, bool) or not isinstance(max_workers, int) or not 1 <= max_workers <= 4:
            raise ValueError("max_workers must be an integer from 1 to 4")
        if handlers is not None and (not isinstance(handlers, dict) or any(
                role not in ROLES or not callable(handler) for role, handler in handlers.items())):
            raise ValueError("Handlers must map known role names to callables")
        self.memory = memory
        self.max_workers = max_workers
        self.handlers = dict(handlers or {})

    def run(self, mission: Mission | dict) -> dict:
        # Frozen dataclasses can still be constructed incorrectly or contain
        # mutable nested values. Revalidate and detach even an existing Mission.
        payload = mission.to_dict() if isinstance(mission, Mission) else copy.deepcopy(mission)
        mission = Mission.from_dict(payload)
        payload = json.loads(canonical(mission.to_dict()))
        snapshot_hash = digest(payload)
        run_id = "orion-" + digest({"schema_version": SCHEMA_VERSION, "mission": payload})
        results: dict[str, AgentResult] = {}
        findings: list[Finding] = []
        trace: list[dict] = []
        memory_records: list[dict] = []
        recorded_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        mode = "analysis" if mission.mode == "auto" else mission.mode
        escalation_reasons: list[str] = []
        activity_lock = threading.Lock()
        active, peak_active = 0, 0

        if utc(mission.as_of) > utc(recorded_at):
            findings.append(Finding("AS_OF_IN_FUTURE", "Decision time cannot be after the actual execution time"))
        trusted_results, trusted_findings = trusted_input_checks(mission)
        findings.extend(trusted_findings)
        trace.append({"role": "ORION-Core", "stage": "trusted_input_checks", "status": "complete",
                      "checked_roles": list(trusted_results),
                      "reason_codes": sorted({item.code for item in trusted_findings})})

        def verified_replay(record: dict) -> dict:
            previous_payload = Mission.from_dict(record["mission"]).to_dict()
            saved = record["result"]
            if digest(previous_payload) != snapshot_hash:
                raise ValueError("Saved snapshot does not match run identity")
            if not isinstance(saved, dict) or any(saved.get(key) != expected for key, expected in (
                    ("run_id", run_id), ("schema_version", SCHEMA_VERSION), ("task_id", mission.task_id),
                    ("snapshot_sha256", snapshot_hash))):
                raise ValueError("Saved decision identity is invalid")
            if canonical(saved.get("forecast")) != canonical(payload.get("forecast")):
                raise ValueError("Saved forecast does not match its immutable snapshot")
            if saved.get("financial_execution_allowed") is not False or saved.get("stake_pct") != 0:
                raise ValueError("Saved decision violates the report-only boundary")
            saved_verdict = saved.get("verdict")
            if saved_verdict not in ("ACCEPT", "REJECT", "WAIT", "COLLECT_MORE"):
                raise ValueError("Saved verdict is invalid")
            if saved.get("execution_allowed") is not (saved_verdict == "ACCEPT" and mission.proposed_action == "report"):
                raise ValueError("Saved execution permission is inconsistent")
            saved_findings = saved.get("findings")
            if not isinstance(saved_findings, list) or any(not isinstance(item, dict) for item in saved_findings):
                raise ValueError("Saved findings are invalid")
            if saved_verdict == "ACCEPT" and (mission.proposed_action != "report" or any(
                    item.get("severity") in ("block", "collect", "wait") for item in saved_findings)):
                raise ValueError("Saved decision disregards a mandatory veto")
            if any(item.severity in ("block", "collect", "wait") for item in findings) and saved.get("verdict") == "ACCEPT":
                raise ValueError("Saved decision violates a current mandatory input guard")
            canonical(saved)
            return copy.deepcopy(saved)

        if self.memory:
            try:
                previous = self.memory.load_run(run_id)
                if previous:
                    return verified_replay(previous)
                memory_records = self.memory.recall(mission.as_of, limit=20)
            except Exception as exc:
                findings.append(Finding("MEMORY_UNAVAILABLE",
                                        f"Memory could not be verified ({type(exc).__name__})"))

        # Enforce the report boundary independently of injectable role handlers.
        if mission.proposed_action != "report":
            findings.append(Finding("ACTION_NOT_AUTHORIZED", "This runtime permits analytical reports only"))
        try:
            requested_stake = finite_number(mission.metadata.get("stake_pct", 0), "stake_pct", 0)
            if requested_stake > 0:
                findings.append(Finding("RISK_LIMIT", "Report runtime has zero financial exposure budget"))
        except ValueError:
            findings.append(Finding("INVALID_NUMBER", "Invalid requested financial exposure"))
        upstream_vetoes = mission.metadata.get("upstream_vetoes", [])
        if not isinstance(upstream_vetoes, (list, tuple)) or any(not isinstance(x, str) for x in upstream_vetoes):
            findings.append(Finding("INVALID_UPSTREAM_VETOES", "Malformed upstream veto contract"))
        elif upstream_vetoes:
            findings.append(Finding("UPSTREAM_VETO", "Upstream vetoes remain blocking: " + ", ".join(upstream_vetoes)))

        def context_for(stage: str) -> dict:
            return {
                "results": copy.deepcopy(results), "findings": tuple(copy.deepcopy(_findings(findings))),
                "memory_records": copy.deepcopy(memory_records), "mode": mode, "stage": stage,
                "run_id": run_id, "snapshot_sha256": snapshot_hash,
            }

        def context_hash(context: dict) -> str:
            return digest({**context,
                           "results": {role: item.to_dict() for role, item in context["results"].items()},
                           "findings": [item.to_dict() for item in context["findings"]]})

        def call(role: str, stage: str, context: dict) -> AgentResult:
            nonlocal active, peak_active
            local_mission = Mission.from_dict(copy.deepcopy(payload))
            with activity_lock:
                active += 1
                peak_active = max(peak_active, active)
            try:
                before_context = context_hash(context)
                handler = self.handlers.get(role) or get_agent(role)
                response = handler(local_mission, context)
                if digest(local_mission.to_dict()) != snapshot_hash or context_hash(context) != before_context:
                    return AgentResult(role, "failed", (Finding(
                        "AGENT_MUTATED_INPUT", f"Role attempted to modify its immutable input: {role}"),), {})
                if not isinstance(response, AgentResult) or response.role != role:
                    raise ValueError("Agent returned an invalid role contract")
                if not isinstance(response.metrics, dict) or not isinstance(response.findings, tuple):
                    raise ValueError("Agent metrics and findings have invalid types")
                if response.status not in ("complete", "partial", "failed", "deferred"):
                    raise ValueError("Agent returned an invalid status")
                for item in response.findings:
                    if (not isinstance(item, Finding) or item.severity not in ("block", "collect", "wait", "warn")
                            or not isinstance(item.code, str) or not item.code
                            or not isinstance(item.message, str) or not isinstance(item.evidence_ids, tuple)
                            or any(not isinstance(x, str) for x in item.evidence_ids)):
                        raise ValueError("Agent returned an invalid finding")
                canonical(response.to_dict())  # rejects NaN/Infinity in extensions
                response = copy.deepcopy(response)
                if response.status in ("failed", "partial") or (response.status == "deferred" and role != "LEARNER"):
                    response = AgentResult(role, "failed", response.findings + (Finding(
                        "AGENT_FAILED", f"Required role did not complete: {role}"),), response.metrics)
                return response
            except Exception as exc:
                return AgentResult(role, "failed", (Finding(
                    "AGENT_FAILED", f"Required role failed: {role} ({type(exc).__name__})"),),
                                   {"error_type": type(exc).__name__})
            finally:
                with activity_lock:
                    active -= 1

        def merge(role: str, stage: str, response: AgentResult) -> None:
            results[role] = response
            findings.extend(response.findings)
            trace.append({"role": role, "stage": stage, "status": response.status,
                          "reason_codes": [item.code for item in response.findings]})

        def skip(role: str, stage: str, reason: str) -> None:
            response = AgentResult(role, "skipped", (), {"reason": reason})
            merge(role, stage, response)

        def guarded_decision() -> None:
            # Findings are monotonic: final auditing or a substituted DECISION
            # handler cannot remove earlier vetoes or turn them into warnings.
            all_findings = _findings(findings)
            supplied = results.get("DECISION")
            candidate = supplied.metrics.get("verdict") if supplied else None
            if candidate not in ("ACCEPT", "REJECT", "WAIT", "COLLECT_MORE"):
                findings.append(Finding("INVALID_DECISION", "Decision role did not return a known verdict"))
                all_findings = _findings(findings)
                candidate = "REJECT"
            if any(item.severity == "block" for item in all_findings):
                verdict = "REJECT"
            elif any(item.severity in ("collect", "wait") for item in all_findings):
                verdict = "WAIT" if mission.resume_condition else "COLLECT_MORE"
            else:
                verdict = candidate
            if verdict == "WAIT" and not mission.resume_condition:
                verdict = "COLLECT_MORE"
            metrics = copy.deepcopy(supplied.metrics) if supplied else {}
            metrics.update({
                "verdict": verdict,
                "reason_codes": sorted({item.code for item in all_findings}),
                "reasons": [item.message for item in all_findings],
                "evidence_quality": "adequate" if verdict == "ACCEPT" else "insufficient",
                "recommendation_verdict": "WATCH" if mission.forecast else "NOT_APPLICABLE",
                "execution_allowed": verdict == "ACCEPT" and mission.proposed_action == "report",
                "financial_execution_allowed": False, "stake_pct": 0.0,
                "resume_condition": mission.resume_condition if verdict == "WAIT" else None,
            })
            results["DECISION"] = AgentResult("DECISION", supplied.status if supplied else "failed",
                                                supplied.findings if supplied else (), metrics)

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="orion") as pool:
            def phase(roles: tuple[str, ...], stage: str) -> None:
                # Every branch reads the same preceding phase, with its own copy.
                futures = [(role, pool.submit(call, role, stage, context_for(stage))) for role in roles]
                for role, future in futures:
                    merge(role, stage, future.result())

            phase(("ORION-Core",), "plan")
            phase(("SENSOR", "MEMORY"), "intake")
            phase(("AUDITOR",), "preflight")
            phase(("PATTERN",), "patterns")
            escalation_reasons = sorted({item.code for item in findings if item.severity in ("block", "collect", "wait")})
            if mission.proposed_action != "report" or any(item.psychology and item.critical for item in mission.claims):
                escalation_reasons.append("HIGH_IMPACT_OR_CONTEXT_UNCERTAINTY")
            if escalation_reasons:
                mode = "deep"
            elif mission.mode == "auto" and not mission.forecast and len(mission.evidence) <= 8:
                mode = "reflex"
            phase(("FORECAST", "CONTEXT"), "analysis")
            # Newly discovered uncertainty also escalates an explicit reflex.
            later_reasons = [item.code for item in findings if item.severity in ("block", "collect", "wait")]
            if later_reasons:
                mode = "deep"
                escalation_reasons = sorted(set(escalation_reasons + later_reasons))
            if mode == "reflex":
                skip("SIMULATOR", "challenge", "Reflex mode: no numeric scenario is needed")
                phase(("SKEPTIC",), "challenge")
                skip("RED TEAM", "risk_review", "Reflex mode: material vetoes absent; risk and audits remain mandatory")
                phase(("RISK",), "risk_review")
            else:
                phase(("SKEPTIC", "SIMULATOR"), "challenge")
                phase(("RED TEAM", "RISK"), "risk_review")
            phase(("DECISION",), "arbitration")
            guarded_decision()
            phase(("AUDITOR", "META"), "final_audit")
            guarded_decision()
            phase(("EXECUTOR",), "report")
            # No training or probability updates are permitted before a result.
            merge("LEARNER", "outcome", AgentResult("LEARNER", "deferred", (), {
                "resume_condition": "Record a confirmed outcome with MemoryStore.settle",
                "automatic_parameter_updates": False,
            }))
            guarded_decision()

        results["ORION-Core"] = AgentResult("ORION-Core", results["ORION-Core"].status,
                                               results["ORION-Core"].findings, {
            **results["ORION-Core"].metrics, "max_workers": self.max_workers,
            "peak_active_workers": peak_active, "mode": mode,
            "escalation_reasons": sorted(set(escalation_reasons)),
            "snapshot_sha256": snapshot_hash, "probability_voting": False,
            "trusted_input_checks": {role: {"status": item.status,
                                              "reason_codes": [finding.code for finding in item.findings]}
                                     for role, item in trusted_results.items()},
        })

        def report() -> dict:
            decision_metrics = results["DECISION"].metrics
            forecast_result = results.get("FORECAST")
            return {
                "schema_version": SCHEMA_VERSION, "run_id": run_id, "task_id": mission.task_id,
                "objective": mission.objective, "as_of": mission.as_of, "recorded_at": recorded_at,
                "snapshot_sha256": snapshot_hash, "mode": mode,
                "forecast": asdict(mission.forecast) if mission.forecast else None,
                "model_status": forecast_result.metrics.get("model_status", "unavailable") if forecast_result else "unavailable",
                **{key: decision_metrics[key] for key in (
                    "verdict", "reason_codes", "reasons", "evidence_quality", "recommendation_verdict",
                    "execution_allowed", "financial_execution_allowed", "stake_pct", "resume_condition")},
                "findings": [item.to_dict() for item in _findings(findings)],
                "metrics": {role: copy.deepcopy(response.metrics) for role, response in results.items()},
                "trace": copy.deepcopy(trace), "memory_persisted": self.memory is not None,
                "runtime_scope": "offline_report_only; no background service or financial executor",
            }

        result = report()
        if self.memory:
            try:
                self.memory.save_run(run_id, payload, result)
            except Exception as exc:
                # Concurrent identical runs may already have frozen their result.
                try:
                    saved = self.memory.load_run(run_id)
                    if saved:
                        return verified_replay(saved)
                except Exception:
                    pass
                findings.append(Finding("MEMORY_WRITE_FAILED", f"Decision could not be recorded ({type(exc).__name__})"))
                guarded_decision()
                trace.append({"role": "MEMORY", "stage": "commit", "status": "failed",
                              "reason_codes": ["MEMORY_WRITE_FAILED"]})
                result = report()
                result["memory_persisted"] = False
        canonical(result)
        return result
