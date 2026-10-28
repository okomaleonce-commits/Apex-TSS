#!/usr/bin/env python3
"""APEX-DIGEST — interface unifiée : UNE requête → UN digest, en passant par tous les moteurs du dépôt.

Accepte une requête souple — une date, une période, un match, une ligue, une liste de matchs — et
route vers les moteurs selon le sport :

  • FOOTBALL (mécanisé de bout en bout) : WORM radar → MI bruit marché/comportement → BSM simulation
    calibrée → FUSION/ORION arbitrage → sources sharp, via tools/apex_fusion.py. Un jour entier produit
    le digest FUSION canonique ; un match / une ligue / une liste / une période produisent un digest
    ORION focalisé (carte ORION + contrat de sortie par verdict), et un match nommé reçoit en plus le
    détail BSM (simulation Monte-Carlo cachée) de ce match.
  • TURF : digest APEX-TURF-WORM du jour (tools/apex_turf_worm.build_email_html), lu depuis le snapshot.
  • HOCKEY : la chaîne hockey est pilotée par apex_hockey_lead (init→triage→check→finalize) depuis un
    fichier d'entrée ; le dispatcher RÉSOUT la requête en un scaffold d'input prêt à lancer, sans
    fabriquer d'analyse.

Produit UN digest unifié (reports/digest/<slug>.html + .txt + .subject.txt). Gel actif : aucune mise
réelle. Anti-invention : un sport sans données du jour est écrit « non disponible », jamais simulé.
À la fin : rappel d'envoi email (connecteur Gmail, règle CLAUDE.md) — un passage sans email est incomplet.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))
OUT = ROOT / "reports" / "digest"


# ───────────────────────────── normalisation de la requête ─────────────────────────────

def _days(date, periode):
    if periode:
        a, b = periode.split(":")
        da, db = dt.date.fromisoformat(a), dt.date.fromisoformat(b)
        if db < da:
            da, db = db, da
        return [(da + dt.timedelta(days=i)).isoformat() for i in range((db - da).days + 1)]
    return [date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")]


def _fixture_set(s):
    if not s:
        return None
    out = set()
    for x in s.split(","):
        x = x.strip()
        if x.isdigit():
            out.add(int(x))
    return out or None


def _make_filter(fixtures, ligue, match):
    """Construit un prédicat sur les verdicts ORION. None = aucun filtre (jour entier)."""
    if not (fixtures or ligue or match):
        return None, "jour entier"
    toks = [t for t in (match or "").lower().replace("-", " ").split() if len(t) > 2]

    def f(v):
        if fixtures and v.get("fixture_id") not in fixtures:
            return False
        if ligue and ligue.lower() not in (v.get("league") or "").lower():
            return False
        if toks:
            hay = (v.get("match") or "").lower()
            if not all(t in hay for t in toks):
                return False
        return True
    label = []
    if fixtures:
        label.append(f"fixtures {sorted(fixtures)}")
    if ligue:
        label.append(f"ligue « {ligue} »")
    if match:
        label.append(f"match « {match} »")
    return f, " · ".join(label)


def _esc(x):
    return html.escape(str(x), quote=False)


# ───────────────────────────── FOOTBALL ─────────────────────────────

def _bsm_detail(day, fixture_id):
    """Détail BSM d'un match (simulation Monte-Carlo cachée) depuis les λ + cotes du snapshot WORM.
    Lecture seule (pas de --record) : le digest est de la recherche, pas une écriture au journal.
    Renvoie un bloc texte, ou None si λ/cotes absents (anti-invention)."""
    try:
        import apex_clv as CLV, apex_apifootball as AF
    except Exception:
        return None
    passes = CLV._passes_by_fixture(day).get(fixture_id)
    if not passes:
        return None
    last = [p for p in passes if p.get("phase") == "PREMATCH"] or passes
    r = last[-1]
    lam = r.get("lambdas")
    if not (lam and len(lam) == 2 and all(lam)):
        return None
    div = AF.LEAGUE_TO_DIV.get(r.get("league_id")) or ""
    _, o1 = AF.pick_book(r.get("odds") or {}, "1X2")
    _, ou = AF.pick_book(r.get("odds") or {}, "OU")
    _, bt = AF.pick_book(r.get("odds") or {}, "BTTS")
    argv = ["python3", str(TOOLS / "apex_bsm.py"), "simulate", "--div", div,
            "--home", r.get("home", ""), "--away", r.get("away", ""),
            "--lh", f"{lam[0]:.3f}", "--la", f"{lam[1]:.3f}",
            "--lambda-source", "APEX-WORM λ structurels (digest)",
            "--status-note", "NON VALIDÉ — λ structurels WORM",
            "--fixture-id", str(fixture_id),
            "--odds-source", "APEX-WORM snapshot", "--odds-time", r.get("scan_time_utc", "")]
    if o1:
        argv += ["--odds-1x2", ",".join(f"{x:.2f}" for x in o1)]
    if ou and ou.get("2.5") and all(ou["2.5"]):
        argv += ["--odds-ou25", ",".join(f"{x:.2f}" for x in ou["2.5"])]
    if bt and all(bt):
        argv += ["--odds-btts", ",".join(f"{x:.2f}" for x in bt)]
    try:
        p = subprocess.run(argv, cwd=str(ROOT), capture_output=True, text=True, timeout=180)
    except (subprocess.TimeoutExpired, OSError) as e:
        return f"(BSM indisponible : {e})"
    return p.stdout.strip() if p.returncode == 0 else f"(BSM erreur : {(p.stderr or '')[:200]})"


def football_sections(days, flt, flt_label, want_bsm):
    """Renvoie (html_parts, txt_parts). Football mécanisé via apex_fusion."""
    try:
        import apex_fusion as F
    except Exception as e:  # noqa: BLE001
        return ([f"<p>Football indisponible : {_esc(e)}</p>"], [f"Football indisponible : {e}"])
    H, T = [], []
    for day in days:
        try:
            verds = F.arbitrate_day(day)
        except Exception as e:  # noqa: BLE001
            H.append(f"<h3>Football — {day}</h3><p>Snapshot absent ou illisible : {_esc(e)} "
                     f"(lancer apex_fusion/apex_worm scan). Rien inventé.</p>")
            T.append(f"Football {day} : snapshot absent ({e}).")
            continue
        sel = [v for v in verds if (flt is None or flt(v))]
        # jour entier + 1 seul jour + football seul : digest FUSION canonique (corps complet)
        if flt is None and len(days) == 1:
            subj, full_html, _v, did = F.build_fusion_digest(day)
            H.append(f"<h2>FOOTBALL — digest FUSION {day} (canonique, digest_id {did})</h2>" + full_html)
            T.append(F._plain_text(day, verds))
            continue
        if not sel:
            H.append(f"<h3>Football — {day} · {_esc(flt_label)}</h3>"
                     f"<p>Aucun match ne correspond dans le snapshot (non inventé).</p>")
            T.append(f"Football {day} · {flt_label} : aucun match correspondant.")
            continue
        card = F._orion_card_html(day, sel)
        H.append(f"<h3>Football — {day} · {_esc(flt_label)} ({len(sel)} match(s))</h3>" + card)
        T.append(f"Football {day} · {flt_label} ({len(sel)} match) :\n" + F._plain_text(day, sel))
        # détail BSM pour un match unique explicitement ciblé
        if want_bsm and len(sel) == 1:
            det = _bsm_detail(day, sel[0].get("fixture_id"))
            if det:
                H.append(f"<h4>Détail BSM — {_esc(sel[0].get('match'))}</h4><pre>{_esc(det)}</pre>")
                T.append("Détail BSM :\n" + det)
    return H, T


# ───────────────────────────── TURF ─────────────────────────────

def turf_sections(days):
    try:
        import apex_turf_worm as TW
    except Exception as e:  # noqa: BLE001
        return ([f"<p>Turf indisponible : {_esc(e)}</p>"], [f"Turf indisponible : {e}"])
    H, T = [], []
    for day in days:
        try:
            subj, thtml = TW.build_email_html(day)
            H.append(f"<h2>TURF — APEX-TURF-WORM {day}</h2><p><i>{_esc(subj)}</i></p>" + thtml)
            T.append(f"Turf {day} : {subj}")
        except Exception as e:  # noqa: BLE001
            H.append(f"<h2>TURF — {day}</h2><p>Pas de passage turf-worm exploitable pour ce jour "
                     f"({_esc(e)}). Lancer : <code>python3 tools/apex_turf_worm.py scan --lonaci --date {day}</code>. "
                     f"Palier maximal turf : SURVEILLER (gates de pari fermées par le backtest).</p>")
            T.append(f"Turf {day} : aucun passage exploitable — lancer apex_turf_worm scan --lonaci.")
    return H, T


# ───────────────────────────── HOCKEY ─────────────────────────────

def hockey_sections(days, ligue, match, slug):
    """La chaîne hockey est pilotée par un fichier d'entrée (apex_hockey_lead). On RÉSOUT la requête en
    un scaffold d'input prêt à lancer — jamais d'analyse fabriquée."""
    if match:
        parts = [p.strip() for p in match.replace("–", "-").split("-")]
        home, away = (parts + ["", ""])[:2]
        req = (f"request:\n  mode: match\n  match:\n    home: \"{home}\"\n    away: \"{away}\"\n"
               f"    league: \"{ligue or 'NHL'}\"\n    date: {days[0]}\n")
        human = f"match {home} – {away} ({ligue or 'ligue à préciser'}, {days[0]})"
    elif ligue:
        req = (f"request:\n  mode: league\n  league: \"{ligue}\"\n  window:\n"
               f"    from: \"{days[0]}\"\n    to: \"{days[-1]}\"\n")
        human = f"ligue {ligue} du {days[0]} au {days[-1]}"
    else:
        req = f"request:\n  mode: date\n  date: \"{days[0]}\"\n"
        human = f"date {days[0]}"
    inp = OUT / f"{slug}.hockey.input.yaml"
    inp.parent.mkdir(parents=True, exist_ok=True)
    inp.write_text("# APEX-HOCKEY — input généré par apex_digest (à lancer via apex_hockey_lead)\n" + req,
                   encoding="utf-8")
    cmds = (f"python3 tools/apex_hockey_lead.py init --input {inp.relative_to(ROOT)}\n"
            f"# puis triage / check (par match) / finalize — voir le skill apex-hockey-team")
    H = [f"<h2>HOCKEY — {_esc(human)}</h2>"
         f"<p>La chaîne hockey (DISCOVERY→G0..G7) est pilotée par <code>apex_hockey_lead</code> depuis un "
         f"fichier d'entrée : aucune analyse n'est fabriquée ici. Input résolu écrit dans "
         f"<code>{_esc(inp.relative_to(ROOT))}</code>. Lancer :</p><pre>{_esc(cmds)}</pre>"]
    T = [f"Hockey — {human} : input scaffold {inp.relative_to(ROOT)} ; lancer apex_hockey_lead init puis check/finalize."]
    return H, T


# ───────────────────────────── assemblage ─────────────────────────────

def _slug(sports, days, flt_label):
    base = "+".join(sports) + "_" + days[0] + ("_" + days[-1] if len(days) > 1 else "")
    extra = flt_label.replace(" ", "").replace("·", "-").replace("«", "").replace("»", "")[:40]
    return (base + ("_" + extra if extra and extra != "jourentier" else "")).replace("/", "-")


def cmd_run(a):
    days = _days(a.date, a.periode)
    fixtures = _fixture_set(a.fixtures)
    flt, flt_label = _make_filter(fixtures, a.ligue, a.match)
    sports = [s.strip() for s in (a.sport or "football").split(",")]
    if "auto" in sports:
        sports = ["football"]
    slug = _slug(sports, days, flt_label)

    H = [f"<!doctype html><html><head><meta charset='utf-8'><title>APEX-DIGEST {days[0]}</title></head><body>",
         f"<h1>APEX-DIGEST — {_esc(', '.join(sports))} · {_esc(days[0] + ('→' + days[-1] if len(days)>1 else ''))} · {_esc(flt_label)}</h1>",
         "<p><i>Gel actif : aucune mise réelle. Chaîne complète par sport ; données absentes écrites "
         "comme absentes (jamais simulées).</i></p>"]
    T = [f"APEX-DIGEST — {', '.join(sports)} · {days[0]}{('→'+days[-1]) if len(days)>1 else ''} · {flt_label}",
         "Gel actif : aucune mise réelle.", ""]

    if "football" in sports:
        fh, ft = football_sections(days, flt, flt_label, want_bsm=bool(a.match or fixtures))
        H += fh
        T += ft + [""]
    if "turf" in sports:
        th, tt = turf_sections(days)
        H += th
        T += tt + [""]
    if "hockey" in sports:
        hh, ht = hockey_sections(days, a.ligue, a.match, slug)
        H += hh
        T += ht + [""]

    H.append("<hr><p style='color:#888;font-size:11px'>APEX-DIGEST · interface unifiée · gel actif.</p></body></html>")
    subject = f"APEX-DIGEST {', '.join(sports)} {days[0]}{('→'+days[-1]) if len(days)>1 else ''} · {flt_label}"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{slug}.html").write_text("\n".join(H), encoding="utf-8")
    (OUT / f"{slug}.txt").write_text("\n".join(T), encoding="utf-8")
    (OUT / f"{slug}.subject.txt").write_text(subject, encoding="utf-8")

    print("\n".join(T))
    print("\n" + "=" * 72)
    print("⚠ ENVOI EMAIL OBLIGATOIRE — un passage APEX sans email est INCOMPLET (règle CLAUDE.md).")
    print(f"  to      = okoma.leonce@gmail.com")
    print(f"  subject = {subject}")
    print(f"  htmlBody = reports/digest/{slug}.html")
    print(f"  body     = reports/digest/{slug}.txt")
    print("  Enchaîner mcp__Gmail__send_message depuis la session (connecteur, aucun secret).")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", help="AAAA-MM-JJ (défaut : aujourd'hui UTC)")
    p.add_argument("--periode", help="AAAA-MM-JJ:AAAA-MM-JJ (plage inclusive)")
    p.add_argument("--match", help="\"Domicile - Extérieur\" (texte libre, filtré sur les verdicts)")
    p.add_argument("--fixtures", help="liste d'identifiants de match séparés par des virgules")
    p.add_argument("--ligue", help="nom/code de ligue (sous-chaîne)")
    p.add_argument("--sport", default="football",
                   help="football,turf,hockey (liste) ou auto (=football). Défaut : football")
    p.add_argument("--scan", action="store_true", help="(réservé) relancer un scan live avant le digest")
    a = p.parse_args(argv)
    return cmd_run(a)


if __name__ == "__main__":
    raise SystemExit(main())
