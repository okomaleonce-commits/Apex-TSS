#!/usr/bin/env python3
"""APEX-FUSION — moteur unique qui fond les quatre cellules APEX en UN digest et UN email.

Avant ce moteur, quatre passages produisaient quatre sorties :
  • APEX-WORM     — radar d'anomalies sur le CLASSEMENT (Sharp/Blowout/Upset/Convergence).
  • APEX-MI       — bruit de marché et comportemental H-60 (ne price pas, bet_authority=false).
  • APEX-PROTOCOL — simulation calibrée BSM (Dixon-Coles Monte-Carlo), via APEX-SYNC.
  • ORION         — cerveau collectif d'arbitrage (voix indépendantes, désaccord, gel).

APEX-FUSION les enchaîne dans un seul outil et rend, PAR MATCH, un verdict ORION unique
(ACCEPTER / REJETER / ATTENDRE / COLLECTER) obtenu en fondant les couches présentes en voix
SOURCÉES puis en appelant tools/orion_consensus.arbitrate. Le digest final est un seul HTML :
la carte ORION en tête (la fusion), puis le corps WORM/MI/SYNC/CHARACTER/bilan réutilisé tel quel.

Principes NON négociables (hérités de l'audit APEX) :
  1. GEL actif : aucun pari réel émis. Tout « ACCEPTER » d'ORION est rétrogradé en ATTENDRE.
  2. ANTI-INVENTION et SOURCES HONNÊTES : chaque voix porte sa source réelle. La Poisson sur le
     CLASSEMENT (WORM) n'est JAMAIS étiquetée BSM. FORECAST (BSM) n'est une voix que si une
     simulation calibrée existe réellement ; sinon elle est écrite ABSENTE.
  3. META / INDÉPENDANCE : des couches qui lisent la même donnée comptent pour UNE voix.
  4. L'ABSTENTION EST UNE DÉCISION : trop peu de voix indépendantes → COLLECTER.

Usage :
  python3 tools/apex_fusion.py run   [--date J] [--scan] [--no-mi] [--max-calls N]
  python3 tools/apex_fusion.py orion --date J           # table ORION seule (texte)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import apex_worm as W          # noqa: E402  (radar + builder d'email réutilisé)
import orion_consensus as O    # noqa: E402  (cœur d'arbitrage)

# Ligues réellement couvertes par le backtest BSM (backtests/latest_params.json "divs").
# Hors de cette liste, FORECAST (BSM) ne peut pas rendre de voix calibrée : écrite ABSENTE.
def _bsm_scope() -> set:
    try:
        p = ROOT / "backtests" / "latest_params.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        return set(d.get("divs") or [])
    except (OSError, ValueError):
        return set()


def _score(x):
    """Un moteur WORM peut être un dict {'score':..} ou un scalaire ; renvoie un float ou None."""
    if isinstance(x, dict):
        x = x.get("score")
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v


# Tier WORM → probabilité d'EDGE actionnable de la voix structurelle (sa propre confiance).
# Ce n'est pas une proba de résultat : c'est « ce match porte-t-il un bord jouable ? » vu du radar.
_TIER_EDGE = {"JOUER": 0.72, "JOUER_PETIT": 0.62, "SURVEILLER": 0.46,
              "NO BET": 0.32, "SURVEILLER_FORME": 0.46}


def orion_votes(worm_rec: dict, mi_item: dict | None, bsm: dict | None):
    """Assemble les voix ORION SOURCÉES d'un match à partir des couches réellement présentes.

    Renvoie (votes, couches) où couches note ce qui a parlé (W/M/C/S) et ce qui est absent.
    Chaque voix = {agent, p, source}. META fusionnera les sources partagées en aval.
    """
    votes = []
    couches = {"W": False, "M": False, "C": False, "S": False}

    # (W) Couche structurelle WORM — Sharp/Blowout/Upset/Convergence + reco lisent le MÊME
    #     classement : UNE SEULE source "worm_classement" (sinon fausse illusion de consensus).
    reco = (worm_rec.get("reco") or {})
    tier = (reco.get("decision") or {}).get("tier") or reco.get("primary_market")
    if tier is not None:
        p = _TIER_EDGE.get(tier, 0.40)
        ev = worm_rec.get("ev_best")
        try:                       # un EV franchement positif pousse un peu la voix (plafonné)
            if ev is not None:
                p = max(0.0, min(1.0, p + min(0.08, max(-0.08, float(ev) / 2))))
        except (TypeError, ValueError):
            pass
        votes.append({"agent": "SENSOR/structure", "p": p, "source": "worm_classement"})
        couches["W"] = True

    # (M) Couche marché — sharp move / RLM / MI watch. Source "marche" (indépendante du classement).
    market_ps = []
    sharp = _score(worm_rec.get("sharp"))
    if sharp is not None and sharp >= 50:
        market_ps.append(0.58 + min(0.12, (sharp - 50) / 200))   # 50→0.58 … 100→0.70
    if worm_rec.get("rlm"):
        market_ps.append(0.62)
    if mi_item:
        st = mi_item.get("status")
        bst = mi_item.get("blowout_status")
        if st == "LIVE_UPSET_WATCH" or bst == "LIVE_BLOWOUT_WATCH":
            market_ps.append(0.66)                                # anomalie structurelle ET argent
        msc = mi_item.get("mi_move_signal_score")
        try:
            if msc is not None and float(msc) >= 50:
                market_ps.append(0.57)
        except (TypeError, ValueError):
            pass
    if market_ps:
        votes.append({"agent": "SENSOR/marche", "p": max(market_ps), "source": "marche"})
        couches["M"] = True

    # (C) Couche comportementale — seulement si un vrai indice apex-bi-* a été scoré pour ce match.
    #     Le worm-hook MI est orienté marché : pas d'indice comportemental → voix ABSENTE (honnête).
    if mi_item and mi_item.get("behavioral_net_edge") is not None:
        try:
            votes.append({"agent": "CONTEXT/comportement",
                          "p": max(0.0, min(1.0, 0.5 + float(mi_item["behavioral_net_edge"]))),
                          "source": "comportement"})
            couches["C"] = True
        except (TypeError, ValueError):
            pass

    # (S) Couche FORECAST (BSM) — UNIQUEMENT si une simulation calibrée existe réellement pour ce
    #     match. On n'étiquette JAMAIS la Poisson-classement WORM en BSM (défaut D1 corrigé).
    if bsm and bsm.get("p") is not None:
        try:
            votes.append({"agent": "FORECAST/bsm", "p": max(0.0, min(1.0, float(bsm["p"]))),
                          "source": "stats_bsm"})
            couches["S"] = True
        except (TypeError, ValueError):
            pass

    return votes, couches


def _load_mi(day: str) -> dict:
    """Artefact MI worm-hook du jour, indexé par fixture_id. Absent = {} (écrit absent en aval)."""
    p = ROOT / "data" / "worm" / "mi" / f"{day}.json"
    out = {}
    try:
        art = json.loads(p.read_text(encoding="utf-8"))
        for it in art.get("items", []):
            out[it.get("fixture_id")] = it
    except (OSError, ValueError):
        pass
    return out


def _load_bsm_cache(day: str) -> dict:
    """Résultats BSM calibrés mis en cache par fixture_id, s'il en existe. Sinon {} (FORECAST absent).

    Format attendu : data/fusion/bsm/<day>.json = {"<fixture_id>": {"p": <edge 0..1>, "statut": ...}}.
    Jamais fabriqué : si le fichier n'existe pas, aucune voix BSM n'est inventée.
    """
    p = ROOT / "data" / "fusion" / "bsm" / f"{day}.json"
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return {int(k): v for k, v in raw.items()}
    except (OSError, ValueError):
        return {}


def arbitrate_day(day: str):
    """Rend la liste des verdicts ORION par match actif, triés par pertinence WORM décroissante."""
    d = dt.date.fromisoformat(day)
    rows = W.latest_by_fixture(d)
    rows.sort(key=W.relevance, reverse=True)
    mi = _load_mi(day)
    bsm = _load_bsm_cache(day)
    scope = _bsm_scope()
    out = []
    for r in rows:
        if r.get("phase") in ("DONE", "DEAD"):
            continue
        fid = r.get("fixture_id")
        div = r.get("division") or r.get("div")
        bsm_rec = bsm.get(fid)
        # Marqueur de périmètre BSM : une voix FORECAST n'est légitime QUE pour une ligue backtestée.
        in_scope = bool(div and div in scope)
        votes, couches = orion_votes(r, mi.get(fid), bsm_rec if in_scope else None)
        verdict = O.arbitrate(votes)
        verdict.update({
            "fixture_id": fid,
            "match": f"{r.get('home')} – {r.get('away')}",
            "league": r.get("league"),
            "country": r.get("country"),
            "kickoff": r.get("kickoff"),
            "phase": r.get("phase"),
            "tier_worm": (r.get("reco", {}).get("decision") or {}).get("tier"),
            "couches": couches,
            "bsm_in_scope": in_scope,
            "relevance": round(W.relevance(r), 3),
        })
        out.append(verdict)
    return out


def _orion_card_html(day: str, verdicts: list) -> str:
    """Carte HTML de la couche ORION, à greffer en tête du digest WORM."""
    def esc(x):
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # On met en avant les matchs où ORION a quelque chose à dire (ACCEPTER/REJETER/ATTENDRE),
    # puis les COLLECTER. Jamais plus de 20 lignes pour rester lisible en email.
    order = {"ACCEPTER": 0, "REJETER": 1, "ATTENDRE": 2, "COLLECTER": 3}
    vv = sorted(verdicts, key=lambda v: (order.get(v["decision"], 9), -v.get("relevance", 0)))
    from collections import Counter
    c = Counter(v["decision"] for v in verdicts)
    n_act = sum(1 for v in verdicts if v["decision"] in ("ACCEPTER", "REJETER", "ATTENDRE"))
    repartition = " · ".join(f"{n} {lbl}" for lbl, n in
                             sorted(c.items(), key=lambda kv: -kv[1]))

    badge = {
        "ACCEPTER": "background:#e7f6ec;color:#137333",
        "REJETER": "background:#fde8e8;color:#b02a2a",
        "ATTENDRE": "background:#fef7e0;color:#8a6d00",
        "COLLECTER": "background:#eef0f4;color:#555",
    }
    H = ["<div class='card' style='border:2px solid #4338ca'>",
         f"<h1>ORION — arbitrage unifié · {esc(day)}</h1>",
         f"<div class='muted'>Fusion WORM + MI + PROTOCOL/BSM en UN verdict par match. "
         f"{len(verdicts)} matchs actifs · {repartition} · {n_act} avec signal d'arbitrage. "
         f"<b>GEL actif</b> : aucune mise réelle — tout « ACCEPTER » devient ATTENDRE.</div>",
         "<table><tr><th>Match</th><th>Compét.</th><th>KO</th><th>Couches</th>"
         "<th class='r'>Voix&nbsp;ind.</th><th class='r'>Désac.</th><th class='r'>Conf.</th>"
         "<th>WORM</th><th>ORION</th></tr>"]
    for v in vv[:20]:
        cch = "".join(k for k, on in v["couches"].items() if on) or "—"
        ko = (v.get("kickoff") or "")[11:16]
        conf = v.get("confiance")
        des = v.get("desaccord")
        st = badge.get(v["decision"], "")
        H.append(
            f"<tr><td><b>{esc(v['match'])}</b></td>"
            f"<td>{esc((v.get('country') or '')[:3])} {esc((v.get('league') or '')[:16])}</td>"
            f"<td>{esc(ko)}</td><td class='tag'>{esc(cch)}</td>"
            f"<td class='r'>{v.get('n_independantes')}</td>"
            f"<td class='r'>{des if des is not None else '—'}</td>"
            f"<td class='r'>{conf if conf is not None else '—'}</td>"
            f"<td>{esc(v.get('tier_worm') or '—')}</td>"
            f"<td><span style='font-weight:700;border-radius:6px;padding:1px 7px;{st}'>{esc(v['decision'])}</span></td></tr>")
    H += ["</table>",
          "<div class='muted'>Couches : <b>W</b>=structure WORM (classement) · <b>M</b>=marché/MI · "
          "<b>C</b>=comportemental · <b>S</b>=FORECAST BSM (seulement si ligue backtestée ET sim calibrée). "
          "Verdict via tools/orion_consensus.py : META fond les sources partagées, le désaccord pénalise "
          "la confiance, &lt;3 voix indépendantes → COLLECTER. L'abstention est une décision pleine.</div>",
          "</div>"]
    return "\n".join(H)


def build_fusion_digest(day: str):
    """(sujet, html, verdicts). Réutilise le corps WORM et greffe la carte ORION en tête."""
    d = dt.date.fromisoformat(day)
    worm_subject, worm_html = W.build_email_html(d)
    verdicts = arbitrate_day(day)
    card = _orion_card_html(day, verdicts)
    # Greffe : la carte ORION devient la première carte, juste après <body>.
    if "<body>" in worm_html:
        html = worm_html.replace("<body>", "<body>\n" + card, 1)
    else:
        html = card + worm_html
    from collections import Counter
    c = Counter(v["decision"] for v in verdicts)
    n_act = sum(1 for v in verdicts if v["decision"] in ("ACCEPTER", "REJETER", "ATTENDRE"))
    subject = (f"APEX-FUSION {day} · ORION {n_act}/{len(verdicts)} arbitrages · "
               f"{c.get('ATTENDRE', 0)} ATTENDRE · {c.get('COLLECTER', 0)} COLLECTER · gel")
    return subject, html, verdicts


def _plain_text(day: str, verdicts: list) -> str:
    lines = [f"APEX-FUSION {day} — arbitrage ORION unifié (GEL actif, aucune mise réelle)", ""]
    order = {"ACCEPTER": 0, "REJETER": 1, "ATTENDRE": 2, "COLLECTER": 3}
    for v in sorted(verdicts, key=lambda v: (order.get(v["decision"], 9), -v.get("relevance", 0))):
        cch = "".join(k for k, on in v["couches"].items() if on) or "—"
        lines.append(f"[{v['decision']:9}] {v['match'][:38]:38} voix={v.get('n_independantes')} "
                     f"conf={v.get('confiance')} couches={cch} WORM={v.get('tier_worm')}")
    return "\n".join(lines)


def cmd_run(a):
    day = a.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    if a.scan:
        print("→ APEX-WORM scan (radar + MI) …")
        ns = argparse.Namespace(date=day, money=True, email=False, max_calls=a.max_calls,
                                mi=not a.no_mi, no_mi=a.no_mi)
        # cmd_scan lit ses attributs ; on complète les manquants prudemment.
        for k, dv in (("leagues", None), ("window", 60.0), ("within", 60.0), ("dry_run", False)):
            if not hasattr(ns, k):
                setattr(ns, k, dv)
        try:
            W.cmd_scan(ns)
        except Exception as e:                      # le scan peut échouer (quota) : on continue sur le snapshot existant
            print(f"⚠ scan interrompu ({str(e)[:100]}) — fusion sur le dernier snapshot disponible.")

    subject, html, verdicts = build_fusion_digest(day)
    outdir = ROOT / "reports" / "fusion"
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / f"{day}.email.html").write_text(html, encoding="utf-8")
    (outdir / f"{day}.email.subject.txt").write_text(subject, encoding="utf-8")
    (outdir / f"{day}.email.txt").write_text(_plain_text(day, verdicts), encoding="utf-8")

    print(_plain_text(day, verdicts))
    print("\n" + "=" * 72)
    print("⚠ ENVOI EMAIL OBLIGATOIRE — un seul digest, un seul email (règle APEX).")
    print(f"  to      = okoma.leonce@gmail.com")
    print(f"  subject = {subject}")
    print(f"  htmlBody = reports/fusion/{day}.email.html")
    print(f"  body     = reports/fusion/{day}.email.txt")
    print("  Envoi via mcp__Gmail__send_message depuis la session (connecteur, aucun secret).")
    return 0


def cmd_orion(a):
    day = a.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    verdicts = arbitrate_day(day)
    print(_plain_text(day, verdicts))
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    r = sp.add_parser("run", help="chaîne complète → digest + email uniques")
    r.add_argument("--date")
    r.add_argument("--scan", action="store_true", help="relancer le scan WORM (sinon lit le dernier snapshot)")
    r.add_argument("--no-mi", action="store_true")
    r.add_argument("--max-calls", type=int, default=300)
    r.set_defaults(func=cmd_run)
    o = sp.add_parser("orion", help="table ORION seule (texte)")
    o.add_argument("--date")
    o.set_defaults(func=cmd_orion)
    a = p.parse_args()
    return a.func(a)


if __name__ == "__main__":
    raise SystemExit(main())
