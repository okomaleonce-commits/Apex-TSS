"""Journée du 29/09/2026 — chaque match passe par apex_bsm.py simulate --record.

* National League (EC) : forces estimées par le modèle backtesté (--div EC).
* Sélections (Ligue des Nations, qualifs CAN, CONCACAF NL) : hors périmètre du backtest.
  λ issus du classement Elo (eloratings.net, +100 à domicile) via une conversion NON VALIDÉE ;
  statut « NON VALIDÉ — λ externes ».
Cotes : dernier relevé API-Football (Pinnacle en priorité), avec source et heure.
"""
import json
import math
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
import apex_apifootball as AF  # noqa: E402
import apex_bsm as B  # noqa: E402
import apex_intl as I  # noqa: E402

INTL = "--h0" not in sys.argv
ONLY_NATIONS = "--nations" in sys.argv
import datetime as _dt
NOW = _dt.datetime.now(_dt.timezone.utc).isoformat()   # H1 par défaut ; --h0 reproduit l'ancienne heuristique

SNAP = ROOT / "data/apifootball/snapshots/2026-09-29.jsonl"
ELO = json.load(open(Path(__file__).with_name("elo.json")))


def elo_lambdas(h, a, base_T):
    """Espérance Elo (+100 domicile) → λ ; total croissant avec le déséquilibre. Heuristique non validée."""
    we = 1 / (10 ** (-(ELO[h] - ELO[a] + 100) / 400) + 1)
    T = base_T + 4.2 * (we - 0.5) ** 2
    best = None
    for s in range(-240, 241):
        sup = s / 40
        lh, la = (T + sup) / 2, (T - sup) / 2
        if lh <= 0.05 or la <= 0.05:
            continue
        p = B.probs_from_matrix(B.dc_matrix(lh, la, -0.05))["1X2"]
        e = abs(p[0] + 0.5 * p[1] - we)
        if best is None or e < best[0]:
            best = (e, lh, la)
    return round(best[1], 3), round(best[2], 3), we


def odds_args(odds):
    args, srcs = [], set()
    bk, o = AF.pick_book(odds, "1X2")
    if o:
        args += ["--odds-1x2", f"{o[0]},{o[1]},{o[2]}"]; srcs.add(bk)
    bk, ou = AF.pick_book(odds, "OU")
    if ou and ou.get("2.5") and all(ou["2.5"]):
        args += ["--odds-ou25", f"{ou['2.5'][0]},{ou['2.5'][1]}"]; srcs.add(bk)
    bk, bt = AF.pick_book(odds, "BTTS")
    if bt:
        args += ["--odds-btts", f"{bt[0]},{bt[1]}"]; srcs.add(bk)
    bk, ah = AF.pick_book(odds, "AH")
    if ah:
        for side in ("dom", "ext"):
            for line, c in (ah.get(side) or {}).items():
                if abs(float(line)) <= 2:
                    args += ["--odds-ah", f"{side}:{float(line) + 0.0:+.2f}:{c}"]
        srcs.add(bk)
    return args, "+".join(sorted(s for s in srcs if s))


def main():
    recs = {}
    for line in open(SNAP, encoding="utf-8"):
        r = json.loads(line)
        recs[r["fixture_id"]] = r          # dernier relevé par match
    meta = []
    for r in sorted(recs.values(), key=lambda r: r["kickoff_utc"]):
        cmd = ["python3", "tools/apex_bsm.py", "simulate", "--kickoff", r["kickoff_utc"], "--record"]
        if ONLY_NATIONS and r["league_id"] == 43:
            continue
        if r["kickoff_utc"] <= NOW:
            continue                                   # match déjà commencé : aucune prévision a posteriori
        if r["league_id"] == 43:
            hn, hs = AF.fd_name("EC", r["home"]); an, as_ = AF.fd_name("EC", r["away"])
            cmd += ["--div", "EC", "--home", hn, "--away", an, "--asof", r["kickoff_utc"][:10],
                    "--seasons", "2425,2526,2627"]
            info = {"mapping": [[r["home"], hn, round(hs, 2)], [r["away"], an, round(as_, 2)]]}
        else:
            if INTL:   # conversion H1 backtestée (tools/apex_intl.py)
                lh, la, P = I.lambdas_for(r["home"], r["away"])
                cmd += ["--home", r["home"], "--away", r["away"], "--lh", f"{lh:.3f}", "--la", f"{la:.3f}",
                        "--rho", str(P["rho"]), "--lambda-source", f"APEX-INTL H1 ({P['run_id']}, Elo au {P['elo_au']})",
                        "--status-note", "NON COMPARÉ AU MARCHÉ — conversion Elo→λ validée contre H0 et référence simple"]
                info = {"elo": [P["elo"][I.canon(r["home"])], P["elo"][I.canon(r["away"])]], "modele": "H1"}
            else:
                base_T = 2.45 if r["league_id"] == 5 else 2.2
                lh, la, we = elo_lambdas(r["home"], r["away"], base_T)
                cmd += ["--home", r["home"], "--away", r["away"], "--lh", str(lh), "--la", str(la)]
                info = {"elo": [ELO[r["home"]], ELO[r["away"]]], "we": round(we, 3), "modele": "H0"}
        oa, src = odds_args(r.get("odds", {}))
        cmd += oa + ["--odds-source", f"API-Football/{src}" if src else "aucune", "--odds-time", r["retrieved_at_utc"]]
        res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        if res.returncode:
            print("ÉCHEC", r["home"], r["away"], res.stderr.strip()[-300:])
            meta.append({"fixture_id": r["fixture_id"], "erreur": res.stderr.strip()[-300:], **info})
            continue
        fid = [l for l in res.stdout.splitlines() if l.startswith("forecast_id")][0].split()[2]
        meta.append({"fixture_id": r["fixture_id"], "forecast_id": fid, "league": r["league"], "cmd": shlex.join(cmd), **info})
        print(res.stdout.splitlines()[0], "→", [l for l in res.stdout.splitlines() if l.startswith("DÉCISION")][0])
    json.dump(meta, open(Path(__file__).with_name("runs_h1.json" if (INTL and ONLY_NATIONS) else "runs.json"), "w"), ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
