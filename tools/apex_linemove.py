#!/usr/bin/env python3
"""APEX-LINEMOVE — le mouvement de cote est-il prévisible, et le modèle l'anticipe-t-il ?

Proxy historique du « mouvement » : cote Pinnacle AVANT-MATCH → cote de CLÔTURE Pinnacle (football-data).
Trois questions, sur le test final 2025-26 (aucune info future : tout est connu avant chaque match) :

  Q1  La clôture est-elle plus précise que l'avant-match ? (log-loss)
  Q2  « Suivre le steam » — parier l'issue vers laquelle la ligne s'est déplacée — rapporte-t-il,
      à la cote d'avant-match ? Et « fader » le mouvement ?
  Q3  ALPHA visé : le désaccord du modèle avec l'avant-match prédit-il le sens du mouvement ?
      Si oui, parier tôt là où le modèle voit ce que la ligne corrigera ensuite = CLV positif.

Le mouvement historique (avant-match→clôture) ne remplace pas le mouvement OUVERTURE→avant-match, non
disponible dans football-data. Ce dernier ne peut être testé qu'EN AVANT (commande `forward`, snapshots
API-Football horodatés). Ici on mesure ce qui est backtestable proprement aujourd'hui.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_bsm as B  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
BT = ROOT / "backtests"


def ll(p, k):
    return -math.log(max(p[k], 1e-12))


def cmd_backtest(a):
    P = json.loads((BT / "latest_params.json").read_text())
    divs = a.divs.split(","); seasons = a.seasons.split(","); test = a.test.split(",")
    data = {d: sorted((m for s in seasons for m in B.fetch_matches(d, s)), key=lambda m: m["date"]) for d in divs}
    recs = []
    for d in divs:
        recs += B.walk_forward(data[d], test, P["xi"], P["K"], [P["rho"]], [P["sigma"]])[(P["rho"], P["sigma"])]
    rows = []
    for r in recs:
        m = r["m"]
        if not m["o1x2"] or not m["close1x2"]:
            continue
        pre = B.demargin(m["o1x2"]); clo = B.demargin(m["close1x2"])
        rows.append({"pre": pre, "clo": clo, "model": np.array(r["P"]["1X2"]), "k": B.outcome_idx(m),
                     "o_pre": m["o1x2"], "mid": f"{m['div']}{m['date']}{m['home']}"})
    n = len(rows)

    # Q1 — précision
    q1 = {"logloss_avant_match": round(float(np.mean([ll(x["pre"], x["k"]) for x in rows])), 4),
          "logloss_cloture": round(float(np.mean([ll(x["clo"], x["k"]) for x in rows])), 4)}

    # Q2 — suivre / fader le steam. Le "mouvement" d'une issue = clo - pre. On mise l'issue au plus grand
    # mouvement positif (steam), à la cote d'avant-match. Fader = l'issue au mouvement le plus négatif.
    def bet_move(direction, min_move):
        bets = []
        for x in rows:
            mv = x["clo"] - x["pre"]
            j = int(mv.argmax()) if direction == "suivre" else int(mv.argmin())
            if abs(mv[j]) < min_move:
                continue
            o = x["o_pre"][j]
            bets.append({"mid": x["mid"], "pnl": o - 1 if x["k"] == j else -1.0})
        if not bets:
            return {"n": 0}
        pnl = np.array([b["pnl"] for b in bets])
        return {"n": len(bets), "rendement": round(float(pnl.mean()), 4), "IC95": B.boot_ci(pnl)}
    q2 = {f"suivre_min{int(mm*100)}pct": bet_move("suivre", mm) for mm in (0.0, 0.02, 0.05)}
    q2.update({f"fader_min{int(mm*100)}pct": bet_move("fader", mm) for mm in (0.02, 0.05)})

    # Q3 — le désaccord modèle vs avant-match prédit-il le mouvement ?
    # signal = model - pre (par issue) ; move = clo - pre. Corrélation, et rendement des paris où
    # le modèle voit une issue sous-cotée d'au moins s, joués à l'avant-match (+ CLV mesuré).
    sig = np.concatenate([x["model"] - x["pre"] for x in rows])
    mov = np.concatenate([x["clo"] - x["pre"] for x in rows])
    corr = float(np.corrcoef(sig, mov)[0, 1])

    def bet_model(s):
        bets = []
        for x in rows:
            d = x["model"] - x["pre"]
            j = int(d.argmax())
            if d[j] < s:
                continue
            o = x["o_pre"][j]
            clv = o * x["clo"][j] - 1                       # >0 = mieux que la clôture
            bets.append({"mid": x["mid"], "pnl": o - 1 if x["k"] == j else -1.0, "clv": clv})
        if not bets:
            return {"n": 0}
        pnl = np.array([b["pnl"] for b in bets]); clv = np.array([b["clv"] for b in bets])
        return {"n": len(bets), "rendement": round(float(pnl.mean()), 4), "IC95": B.boot_ci(pnl),
                "clv_moyen": round(float(clv.mean()), 4), "part_clv_positif": round(float((clv > 0).mean()), 3)}
    q3 = {"correlation_signal_mouvement": round(corr, 4),
          "paris_modele": {f"seuil_{int(s*100)}pct": bet_model(s) for s in (0.03, 0.05, 0.08)}}

    verdict = []
    verdict.append("Q1 : la clôture est plus précise que l'avant-match" if q1["logloss_cloture"] < q1["logloss_avant_match"]
                   else "Q1 : l'avant-match est aussi précis que la clôture")
    st = q2["suivre_min5pct"]
    verdict.append(f"Q2 : suivre le steam (≥5%) rapporte {st.get('rendement')} IC95 {st.get('IC95')} — "
                   + ("edge" if st.get("n") and st["IC95"][0] > 0 else "pas d'edge net"))
    verdict.append(f"Q3 : corrélation signal↔mouvement {corr:+.3f} — "
                   + ("le modèle anticipe le mouvement" if corr > 0.05 else "le modèle n'anticipe pas le mouvement"))
    rep = {"n_matchs": n, "divs": divs, "test": test, "Q1_precision": q1, "Q2_steam": q2, "Q3_anticipation": q3,
           "verdict": verdict,
           "limite": "mouvement avant-match→clôture uniquement ; ouverture→avant-match seulement testable en avant (forward)"}
    out = BT / "linemove.json"; out.write_text(json.dumps(rep, ensure_ascii=False, indent=1))
    print(render(rep))


def render(R):
    L = [f"# Mouvement de cote : prévisible ? — {R['n_matchs']} matchs, test {', '.join(R['test'])}",
         f"Ligues {', '.join(R['divs'])}. Proxy : Pinnacle avant-match → clôture.", "",
         "## Q1 — précision avant-match vs clôture (log-loss, plus bas = mieux)",
         f"- avant-match : {R['Q1_precision']['logloss_avant_match']}",
         f"- clôture : {R['Q1_precision']['logloss_cloture']}", "",
         "## Q2 — suivre / fader le mouvement (mise à la cote d'avant-match)", "",
         "| stratégie | n | rendement/u | IC95 |", "|---|---|---|---|"]
    for k, v in R["Q2_steam"].items():
        if v.get("n"):
            L.append(f"| {k} | {v['n']} | {v['rendement']:+.3f} | {v['IC95']} |")
    L += ["", "## Q3 — le modèle anticipe-t-il le mouvement ?",
          f"Corrélation (signal modèle−avant-match) ↔ (mouvement clôture−avant-match) : **{R['Q3_anticipation']['correlation_signal_mouvement']:+.3f}**",
          "", "| seuil désaccord modèle | n | rendement/u | IC95 | CLV moyen | % CLV>0 |", "|---|---|---|---|---|---|"]
    for k, v in R["Q3_anticipation"]["paris_modele"].items():
        if v.get("n"):
            L.append(f"| {k} | {v['n']} | {v['rendement']:+.3f} | {v['IC95']} | {v['clv_moyen']:+.4f} | {v['part_clv_positif']} |")
    L += ["", "## Verdict", ""] + [f"- {v}" for v in R["verdict"]]
    L += ["", f"*Limite : {R['limite']}.*"]
    return "\n".join(L) + "\n"


def cmd_forward(a):
    """Mouvement OUVERTURE→avant-match sur les snapshots API-Football horodatés (se remplit en avant)."""
    import apex_apifootball as AF  # noqa
    snaps = sorted((ROOT / "data/apifootball/snapshots").glob("*.jsonl"))
    byfix = {}
    for p in snaps:
        for line in open(p, encoding="utf-8"):
            r = json.loads(line)
            o = AF.pick_book(r.get("odds", {}), "1X2")[1]
            if not o:
                continue
            byfix.setdefault(r["fixture_id"], []).append((r["retrieved_at_utc"], o, r["home"], r["away"]))
    moves = [v for v in byfix.values() if len(v) >= 2]
    print(f"{len(byfix)} matchs relevés, {len(moves)} avec ≥2 relevés (ouverture + avant-match).")
    if not moves:
        print("Le mouvement ouverture→avant-match se mesurera quand plusieurs relevés par match seront accumulés "
              "(le pipeline quotidien + un second passage tardif s'en chargent).")
        return
    tot = 0.0
    for v in sorted(moves, key=lambda v: v[0][0]):
        v.sort()
        (_, o0, h, aw), (_, o1, _, _) = v[0], v[-1]
        d0 = B.demargin(o0); d1 = B.demargin(o1)
        mv = d1 - d0
        tot += abs(mv).sum()
        print(f"  {h} – {aw} : 1X2 {[round(x,3) for x in d0]} → {[round(x,3) for x in d1]} (Δmax {abs(mv).max()*100:+.1f} pts)")
    print(f"Amplitude moyenne du mouvement : {tot/len(moves)*100:.1f} pts. Trop peu de matchs pour conclure.")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("backtest")
    b.add_argument("--divs", default="E0,E1,E2,E3,SP1,I1,D1,F1")
    b.add_argument("--seasons", default="2122,2223,2324,2425,2526"); b.add_argument("--test", default="2526")
    sp.add_parser("forward")
    a = p.parse_args()
    {"backtest": cmd_backtest, "forward": cmd_forward}[a.cmd](a)


if __name__ == "__main__":
    main()
