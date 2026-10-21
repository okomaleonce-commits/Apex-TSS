#!/usr/bin/env python3
"""APEX-COMPOS — absences → ajustement λ d'avant-match pour apex_bsm simulate (NON CALIBRÉ, sourcé).

Idée : quand une équipe perd des joueurs qui pèsent une PART RÉELLE de sa production offensive, son λ
d'attaque baisse. Ce module transforme des absences RÉELLES (blessures/suspensions API-Football, et la
composition publiée si elle l'est) en un multiplicateur λ BORNÉ, à passer à :

    apex_bsm.py simulate ... --scenario «proba:mult_dom:mult_ext»   (correction subjective, déjà testée
    en sensibilité par BSM), et --missing-lineup si une absence est déterminante (pose un veto BSM).

HONNÊTETÉ (règles APEX) :
  • Le coefficient qui relie « part offensive absente » → « baisse de λ » est DÉCLARÉ et NON CALIBRÉ :
    aucun backtest ne l'a validé. La sortie est donc étiquetée NON CALIBRÉ et emprunte le chemin
    « scénario subjectif » de BSM — jamais présentée comme un bord validé. Le gel reste entier.
  • Chaque poids vient d'une donnée SOURCÉE (buts+passes décisives de la saison, API-Football). Un
    absent dont on ne peut pas sourcer l'importance n'est PAS chiffré : il est signalé, pas inventé.
  • On ne modélise que le MALUS d'attaque de l'équipe diminuée (direction prudente). L'effet d'un
    défenseur/gardien absent sur le λ adverse n'est pas modélisé (écrit comme non modélisé).
  • Journalisé dans ledger/compos.jsonl (append-only) pour que l'effet soit jugeable en avant par le CLV.

Usage :
  python3 tools/apex_compos.py plan --fixture <id> [--div E0] [--k 0.6] [--record]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

# Constantes DÉCLARÉES et NON CALIBRÉES (aucun backtest ne les valide ; bornes prudentes).
K_ABS = 0.6                 # pente part_offensive_absente → malus de λ attaque
LO_MULT = 0.80              # plancher du multiplicateur (−20 % de λ max, même si absences massives)
QUESTIONABLE_WEIGHT = 0.5   # un « douteux » compte moitié d'un « forfait » confirmé
VETO_SHARE = 0.20           # absence(s) ≥ 20 % de la production → --missing-lineup (veto BSM)
COMPOS_LEDGER = ROOT / "ledger" / "compos.jsonl"


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return " ".join(s.replace(".", " ").replace("-", " ").split())


# ───────────────────────────── logique pure (testable sans réseau) ─────────────────────────────

def involvement(players: list[dict]) -> tuple[dict, float]:
    """players = [{name, goals, assists, minutes}]. Renvoie (part_par_joueur, total_équipe).
    Part = (buts+passes) du joueur / total équipe. Total planché à 1 pour éviter la division par ~0."""
    contrib = {}
    for p in players:
        g = float(p.get("goals") or 0) + float(p.get("assists") or 0)
        contrib[_norm(p["name"])] = contrib.get(_norm(p["name"]), 0.0) + g
    total = sum(contrib.values())
    total = max(total, 1.0)
    return {k: v / total for k, v in contrib.items()}, total


def absent_players(blessures_equipe: list[dict], titulaires: list[str] | None) -> list[dict]:
    """Absents d'une équipe à partir des blessures API-Football, affinés par la compo si publiée.
    Renvoie [{name, weight, statut}] : weight=1 forfait confirmé, QUESTIONABLE_WEIGHT si douteux.
    Si la compo est publiée, un « douteux » présent dans le XI est retiré (il joue)."""
    xi = {_norm(t) for t in (titulaires or [])}
    # Déduplication par joueur : l'API injuries peut répéter un même joueur (plusieurs lignes). On garde
    # UNE entrée par joueur, avec le statut le plus sévère (forfait > absent du XI > douteux).
    best: dict = {}
    for b in blessures_equipe:
        nom = b.get("joueur") or b.get("player") or ""
        if not nom:
            continue
        key = _norm(nom)
        if titulaires is not None and key in xi:
            continue                       # annoncé dans le XI → il joue, pas une absence
        typ = (b.get("type") or "").lower()
        confirmed_out = "missing" in typ or "suspend" in typ or "out" in typ
        if confirmed_out:
            cand = {"name": nom, "weight": 1.0, "statut": b.get("type") or "forfait"}
        elif titulaires is not None:
            cand = {"name": nom, "weight": 1.0, "statut": "absent du XI publié"}
        else:
            cand = {"name": nom, "weight": QUESTIONABLE_WEIGHT, "statut": b.get("type") or "douteux"}
        prev = best.get(key)
        if prev is None or cand["weight"] > prev["weight"]:
            best[key] = cand
    return list(best.values())


def team_malus(absents: list[dict], shares: dict, k: float = K_ABS, lo: float = LO_MULT):
    """Multiplicateur de λ attaque ∈ [lo, 1.0] + détail. part_absente = Σ poids·part offensive.
    Un absent sans part sourcée (absent des stats) compte 0 mais est signalé (non inventé)."""
    detail, part, non_source = [], 0.0, []
    for a in absents:
        s = shares.get(_norm(a["name"]))
        if s is None:
            non_source.append(a["name"])
            detail.append({"joueur": a["name"], "statut": a["statut"], "part": None, "poids": a["weight"]})
            continue
        part += a["weight"] * s
        detail.append({"joueur": a["name"], "statut": a["statut"], "part": round(s, 4), "poids": a["weight"]})
    mult = max(lo, 1.0 - k * part)
    return {"mult": round(mult, 4), "part_absente": round(part, 4),
            "detail": detail, "importance_non_sourcee": non_source}


def scenario_string(mult_home: float, mult_away: float) -> str:
    """Scénario CERTAIN pour apex_bsm simulate : proba 1.0, multiplicateurs λ dom/ext."""
    return f"1.0:{mult_home:.3f}:{mult_away:.3f}"


# ───────────────────────────── récupération réseau (API-Football) ─────────────────────────────

def _team_involvement(tid: int, season, AF) -> dict:
    """Part offensive (buts+passes saison) par joueur d'une équipe. {} si indisponible (jamais inventé)."""
    try:
        resp = AF.api_all("players", team=tid, season=season)
    except Exception:
        return {}
    players = []
    for it in resp:
        name = (it.get("player") or {}).get("name")
        if not name:
            continue
        g = a = mins = 0
        for st in it.get("statistics", []):
            gg = st.get("goals") or {}
            g += gg.get("total") or 0
            a += gg.get("assists") or 0
            mins += (st.get("games") or {}).get("minutes") or 0
        players.append({"name": name, "goals": g, "assists": a, "minutes": mins})
    shares, _ = involvement(players)
    return shares


def plan(fixture_id: int, div: str = "", k: float = K_ABS, record: bool = False) -> dict:
    import apex_apifootball as AF
    fx = AF.api("fixtures", id=fixture_id)["response"]
    if not fx:
        sys.exit(f"Fixture {fixture_id} introuvable.")
    f = fx[0]
    home, away = f["teams"]["home"]["name"], f["teams"]["away"]["name"]
    hid, aid = f["teams"]["home"]["id"], f["teams"]["away"]["id"]
    season = f["league"]["season"]
    league = f["league"]["name"]

    blessures = [{"equipe": i["team"]["name"], "joueur": i["player"]["name"],
                  "type": i["player"].get("type")} for i in AF.api_all("injuries", fixture=fixture_id)]
    try:
        lu = AF.api("fixtures/lineups", fixture=fixture_id)["response"]
    except Exception:
        lu = []
    xi = {t["team"]["name"]: [p["player"]["name"] for p in t.get("startXI", [])] for t in lu}

    sh_home = _team_involvement(hid, season, AF)
    sh_away = _team_involvement(aid, season, AF)

    def side(team_name, shares):
        bl = [b for b in blessures if b.get("equipe") == team_name]
        tit = xi.get(team_name)                      # None si compo non publiée
        absents = absent_players(bl, tit)
        m = team_malus(absents, shares, k=k)
        m["compo_publiee"] = tit is not None
        m["stats_dispo"] = bool(shares)
        return m

    mh = side(home, sh_home)
    ma = side(away, sh_away)
    veto = mh["part_absente"] >= VETO_SHARE or ma["part_absente"] >= VETO_SHARE
    scen = scenario_string(mh["mult"], ma["mult"])
    out = {
        "fixture_id": fixture_id, "home": home, "away": away, "league": league, "season": season,
        "div": div, "k_abs": k, "calibration": "NON CALIBRÉ",
        "home_malus": mh, "away_malus": ma, "scenario": scen, "missing_lineup_veto": veto,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if record:
        COMPOS_LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with open(COMPOS_LEDGER, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(out, ensure_ascii=False) + "\n")
        out["recorded"] = str(COMPOS_LEDGER.relative_to(ROOT))
    return out


# ───────────────────────────── rendu ─────────────────────────────

def render(o: dict) -> str:
    L = [f"APEX-COMPOS · {o['home']} – {o['away']} · {o['league']} {o['season']} · fixture {o['fixture_id']}",
         "⚠ NON CALIBRÉ : malus λ subjectif, sourcé mais non backtesté. Alimente le scénario de "
         "sensibilité de BSM, jamais un bord validé. Gel entier.", ""]
    for tag, m, team in (("DOMICILE", o["home_malus"], o["home"]), ("EXTÉRIEUR", o["away_malus"], o["away"])):
        compo = "compo publiée" if m["compo_publiee"] else "compo NON publiée (douteux à moitié)"
        stats = "stats OK" if m["stats_dispo"] else "stats joueurs INDISPONIBLES → part non chiffrée"
        L.append(f"[{tag}] {team} — multiplicateur λ attaque = {m['mult']} "
                 f"(part offensive absente {m['part_absente']:.0%}) · {compo} · {stats}")
        for d in m["detail"]:
            part = "part non sourcée" if d["part"] is None else f"{d['part']:.0%} de la prod."
            L.append(f"    · {d['joueur']} [{d['statut']}] — {part} (poids {d['poids']})")
        if m["importance_non_sourcee"]:
            L.append(f"    (importance non sourcée, non chiffrée : {', '.join(m['importance_non_sourcee'])})")
    L.append("")
    L.append("Malus d'attaque de l'équipe diminuée uniquement — effet d'un défenseur/gardien absent "
             "sur le λ adverse NON modélisé.")
    L.append("")
    L.append("À injecter dans la simulation calibrée (le scénario est testé en sensibilité par BSM) :")
    veto = " --missing-lineup" if o["missing_lineup_veto"] else ""
    divarg = f" --div {o['div']}" if o["div"] else ""
    L.append(f"  python3 tools/apex_bsm.py simulate --home \"{o['home']}\" --away \"{o['away']}\"{divarg} "
             f"--scenario \"{o['scenario']}\"{veto}")
    if o["missing_lineup_veto"]:
        L.append("  (--missing-lineup : une absence ≥ 20 % de la production → veto BSM)")
    return "\n".join(L)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    pl = sp.add_parser("plan", help="absences → scénario λ pour simulate")
    pl.add_argument("--fixture", type=int, required=True)
    pl.add_argument("--div", default="")
    pl.add_argument("--k", type=float, default=K_ABS)
    pl.add_argument("--record", action="store_true", help="journalise dans ledger/compos.jsonl")
    a = p.parse_args()
    o = plan(a.fixture, a.div, a.k, a.record)
    print(render(o))
    if o.get("recorded"):
        print(f"\nJournalisé → {o['recorded']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
