#!/usr/bin/env python3
"""ORION CLI: run, replay, and learn from analytical dossiers, offline."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orion.apex_adapter import mission_from_apex, records_from_payload
from orion.contracts import Mission, canonical
from orion.core import OrionCore
from orion.memory import MemoryStore


def now_utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read_json(path):
    def reject(value):
        raise ValueError("INVALID_NUMBER: " + value)
    return json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=reject)


def atomic_write(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=".orion-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def report_markdown(report):
    lines = ["# ORION SUPERBRAIN — rapport", "", f"Mission : {report.get('task_id')}", "",
             f"**Décision analytique : {report.get('verdict')}**", "",
             f"Mode : {report.get('mode')}. Recommandation : {report.get('recommendation_verdict')}. Mise : 0 %.", "",
             "Les contrôles portent sur les contenus fournis. Ils ne prouvent pas la vérité des sources ni une performance prédictive.", ""]
    forecast = report.get("forecast")
    if forecast:
        lines += [f"Événement : {forecast.get('event')}", "",
                  f"Probabilité fournie : {forecast.get('probability')}. Méthode : {forecast.get('method')} ({forecast.get('method_version')}).",
                  "", "La probabilité est conservée ; aucune moyenne des avis ni correction arbitraire n’est appliquée.", ""]
    lines += ["## Contrôles", "", "| Code | Gravité | Motif |", "|---|---|---|"]
    for f in report.get("findings", []):
        message = str(f.get("message", "")).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {f.get('code')} | {f.get('severity')} | {message} |")
    if not report.get("findings"):
        lines.append("| Aucun blocage | — | Dossier recevable pour un rapport analytique |")
    lines += ["", "## Parcours des rôles", "", "| Rôle | Étape | État |", "|---|---|---|"]
    for t in report.get("trace", []):
        lines.append(f"| {t.get('role')} | {t.get('stage', '')} | {t.get('status')} |")
    lines += ["", f"Identifiant : `{report.get('run_id')}`", "",
              f"Empreinte des entrées : `{report.get('snapshot_sha256')}`", "",
              "Ce moteur produit des rapports locaux. Aucun pari, message externe ou service permanent n’est lancé.", ""]
    return "\n".join(lines)


def write_report(report, out):
    target = Path(out) / report["run_id"]
    atomic_write(target / "report.json", json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    atomic_write(target / "report.md", report_markdown(report))
    return target


def demo_payloads():
    as_of = now_utc()
    older = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    def evidence(identifier, origin, content):
        return {"evidence_id": identifier, "fact_key": "delivery_deadline", "value": "Friday",
                "statement": "Delivery is due Friday", "content": content,
                "source_uri": "demo://" + identifier, "origin_ids": [origin],
                "available_at": older, "retrieved_at": older, "kind": "document"}
    positive = {"task_id": "demo:independent-documents", "objective": "Review an illustrative delivery deadline",
                "as_of": as_of, "proposed_action": "report", "mode": "analysis",
                "requirements": {"delivery_deadline": 2},
                "evidence": [evidence("purchase-order", "buyer-record", "Delivery is due Friday. Purchase order."),
                             evidence("supplier-confirmation", "supplier-record", "Delivery is due Friday. Supplier confirmation.")],
                "claims": [{"claim_id": "deadline", "statement": "Delivery is due Friday",
                            "kind": "fact", "evidence_ids": ["purchase-order", "supplier-confirmation"]}]}
    correlated = json.loads(canonical(positive))
    correlated["task_id"] = "demo:copied-source"
    correlated["evidence"] = [evidence("copy-" + str(i), "same-press-release", "Delivery is due Friday. Copied announcement.")
                              for i in range(5)]
    correlated["claims"] = []
    simulated = json.loads(canonical(positive))
    simulated["task_id"] = "demo:synthetic-forecast"
    simulated["forecast"] = {"event_id": "illustrative-success", "event": "A synthetic binary event succeeds",
                             "probability": 0.65, "method": "user-supplied synthetic example",
                             "method_version": "demo-1", "validation_status": "unvalidated",
                             "synthetic": True, "odds": 1.7, "odds_at": older,
                             "sensitivity_probabilities": [0.55, 0.65, 0.7]}
    return [positive, correlated, simulated]


def main(argv=None):
    parser = argparse.ArgumentParser(description="ORION SUPERBRAIN: offline analytical agent orchestration")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("demo", "analyze", "from-apex", "settle", "status"):
        p = sub.add_parser(name)
        p.add_argument("--memory", default="state/orion/memory.sqlite3")
        if name in ("demo", "analyze", "from-apex"):
            p.add_argument("--out", default="reports/orion")
            p.add_argument("--workers", type=int, default=4)
        if name in ("analyze", "from-apex"):
            p.add_argument("--input", required=True)
        if name == "from-apex":
            p.add_argument("--as-of")
        if name == "settle":
            p.add_argument("--run-id", required=True)
            p.add_argument("--outcome", type=int, choices=(0, 1), required=True)
            p.add_argument("--available-at", required=True)
    args = parser.parse_args(argv)
    try:
        memory = MemoryStore(args.memory)
        if args.command == "status":
            print(json.dumps(memory.learning_report(now_utc()), ensure_ascii=False, indent=2, allow_nan=False))
            return 0
        if args.command == "settle":
            outcome = memory.settle(args.run_id, args.outcome, args.available_at)
            print(json.dumps(outcome, ensure_ascii=False, indent=2, allow_nan=False))
            return 0
        core = OrionCore(memory=memory, max_workers=args.workers)
        if args.command == "demo":
            missions = [Mission.from_dict(p) for p in demo_payloads()]
        elif args.command == "analyze":
            missions = [Mission.from_dict(read_json(args.input))]
        else:
            imported_at = now_utc()
            source_uri = Path(args.input).resolve().as_uri()
            missions = [mission_from_apex(r, source_uri=source_uri, imported_at=imported_at, as_of=args.as_of)
                        for r in records_from_payload(read_json(args.input))]
        index = []
        for mission in missions:
            report = core.run(mission)
            directory = write_report(report, args.out)
            item = {"run_id": report["run_id"], "task_id": report["task_id"], "verdict": report["verdict"],
                    "recommendation_verdict": report["recommendation_verdict"], "report": str(directory / "report.md"),
                    "reason_codes": report.get("reason_codes", [])}
            index.append(item)
            print(f"{report['run_id']}: {report['verdict']} | {report['recommendation_verdict']} | mise 0 %")
        atomic_write(Path(args.out)/"index.json", json.dumps(index, ensure_ascii=False, indent=2) + "\n")
        lines = ["# ORION — exécution", "", "| Mission | Décision analytique | Recommandation |", "|---|---|---|"]
        lines += [f"| {r['task_id']} | {r['verdict']} | {r['recommendation_verdict']} |" for r in index]
        atomic_write(Path(args.out)/"index.md", "\n".join(lines) + "\n")
        return 0
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(f"ORION refused input or persistence: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
