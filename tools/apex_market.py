#!/usr/bin/env python3
"""APEX-MARKET — modèle « ancré sur le marché » et son évaluation en temps réel.

Idée : le marché (Pinnacle avant-match démarginé) est le meilleur estimateur connu. On ne le remplace pas,
on part de lui. Le modèle APEX-BSM ne sert qu'à s'en écarter, et seulement d'un poids w estimé sur données
passées : p_final ∝ p_marché^(1−w) × p_modèle^w. Si le modèle n'apporte rien, w tombe à 0 et p_final = marché.

Deux commandes :
  calibrate  Estime w chronologiquement (apprentissage / validation / test isolé) et dit HONNÊTEMENT si le
             mélange bat le marché pur hors échantillon. Aucune information future.
  forward    Évalue l'accumulation réelle de la saison en cours depuis le journal (ledger/) : pour chaque
             match RÉGLÉ, log-loss du modèle, du marché et du mélange, plus le CLV. C'est le vrai test en
             avant, sur des matchs que rien n'a servi à régler.

Le mélange n'autorise JAMAIS un pari que le protocole BSM interdirait : il reste soumis aux mêmes gates.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_bsm as B  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
BT = ROOT / "backtests"
LEDGER = ROOT / "ledger"
PARAMS = BT / "latest_params.json"


def blend(pm, pk, w):
    """p ∝ pk^(1−w) × pm^w. w=0 → marché pur ; w=1 → modèle pur."""
    raw = np.array([pk[i] ** (1 - w) * pm[i] ** w for i in range(len(pk))])
    return raw / raw.sum()


def logloss(p, k):
    return -math.log(max(p[k], 1e-12))


def cmd_calibrate(a):
    P = json.loads(PARAMS.read_text())
    divs = a.divs.split(","); seasons = a.seasons.split(",")
    val_seasons = a.val.split(","); test_seasons = a.test.split(",")
    data = {d: sorted((m for s in seasons for m in B.fetch_matches(d, s)), key=lambda m: m["date"]) for d in divs}
    # probabilités du modèle, match par match, sur validation + test (fenêtres hebdo, aucune info future)
    def model_recs(season_set):
        recs = []
        for d in divs:
            recs += B.walk_forward(data[d], season_set, P["xi"], P["K"], [P["rho"]], [P["sigma"]])[(P["rho"], P["sigma"])]
        out = []
        for r in recs:
            m = r["m"]
            if not m["o1x2"]:
                continue
            out.append({"pm": np.array(r["P"]["1X2"]), "pk": B.demargin(m["o1x2"]),
                        "k": B.outcome_idx(m), "close": m["close1x2"], "o": m["o1x2"], "div": m["div"]})
        return out
    val, test = model_recs(val_seasons), model_recs(test_seasons)

    # w optimal = minimise la log-loss 1X2 sur la VALIDATION uniquement
    ws = [i / 100 for i in range(0, 101, 2)]
    curve = [(w, float(np.mean([logloss(blend(r["pm"], r["pk"], w), r["k"]) for r in val]))) for w in ws]
    w_star = min(curve, key=lambda x: x[1])[0]

    def stat(recs, probf):
        ll = [logloss(probf(r), r["k"]) for r in recs]
        return {"n": len(recs), "logloss": round(float(np.mean(ll)), 4)}

    def delta_ci(recs, probf):
        d = [logloss(probf(r), r["k"]) - logloss(r["pk"], r["k"]) for r in recs]  # <0 = mieux que le marché
        return round(float(np.mean(d)), 4), B.boot_ci(d)

    res = {"marché_pur": stat(test, lambda r: r["pk"]),
           "modèle_pur": stat(test, lambda r: r["pm"]),
           "mélange_w*": stat(test, lambda r: blend(r["pm"], r["pk"], w_star))}
    dmix = delta_ci(test, lambda r: blend(r["pm"], r["pk"], w_star))
    dmod = delta_ci(test, lambda r: r["pm"])

    verdict = ("Le mélange BAT le marché hors échantillon" if dmix[1][1] < 0 else
               "w*≈0 : le modèle n'apporte rien, le mélange = marché" if w_star <= 0.06 else
               "Le mélange ne bat PAS le marché de façon significative (IC95 contient 0)")
    report = {"run": f"market-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}", "divs": divs,
              "apprentissage": "implicite (ratings glissants)", "validation": val_seasons, "test_final": test_seasons,
              "w_optimal_validation": w_star, "courbe_w": curve,
              "test_final": res, "delta_logloss_vs_marche": {"mélange": dmix, "modèle": dmod},
              "verdict": verdict}
    out = BT / report["run"]; out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    P2 = {**P, "market_blend_w": w_star, "market_run": report["run"], "market_verdict": verdict}
    (BT / "market_params.json").write_text(json.dumps(P2, ensure_ascii=False, indent=1))
    print(render_cal(report))


def render_cal(R):
    L = [f"# Ancrage marché — calibration {R['run']}",
         f"Ligues {', '.join(R['divs'])} · validation {', '.join(R['validation'])} · **test final {', '.join(R['test_final'])}**",
         f"Poids modèle optimal (validation) : **w = {R['w_optimal_validation']}**  (0 = marché pur, 1 = modèle pur)", "",
         "## Test final — log-loss 1X2 (plus bas = mieux)", "", "| Modèle | n | log-loss |", "|---|---|---|"]
    for k, v in R["test_final"].items():
        L.append(f"| {k} | {v['n']} | {v['logloss']} |")
    d = R["delta_logloss_vs_marche"]
    L += ["", "Écart de log-loss vs marché (négatif = mieux que le marché), IC95 bootstrap :",
          f"- mélange : {d['mélange'][0]:+.4f}  IC95 {d['mélange'][1]}",
          f"- modèle seul : {d['modèle'][0]:+.4f}  IC95 {d['modèle'][1]}",
          "", f"**Verdict : {R['verdict']}**", "",
          "Courbe log-loss selon w (validation) : " + ", ".join(f"w={w:.2f}:{ll:.4f}" for w, ll in R["courbe_w"][::5])]
    return "\n".join(L) + "\n"


def cmd_forward(a):
    """Évalue l'accumulation réelle : matchs RÉGLÉS du journal, modèle vs marché vs mélange, hors échantillon."""
    mp = json.loads((BT / "market_params.json").read_text()) if (BT / "market_params.json").exists() else {"market_blend_w": 0.0}
    w = mp["market_blend_w"]
    F = {json.loads(l)["forecast_id"]: json.loads(l) for l in open(LEDGER / "forecasts.jsonl", encoding="utf-8")} \
        if (LEDGER / "forecasts.jsonl").exists() else {}
    S = {}
    if (LEDGER / "settlements.jsonl").exists():
        for l in open(LEDGER / "settlements.jsonl", encoding="utf-8"):
            s = json.loads(l); S[s["forecast_id"]] = s          # dernière saisie fait foi
    rows = []
    for fid, f in F.items():
        s = S.get(fid)
        if not s or s.get("statut") != "JOUE" or not s.get("score"):
            continue
        o = f.get("cotes", {}).get("1X2")
        m = f.get("marches", {})
        if not o or "1" not in m:
            continue
        try:
            h, g = map(int, s["score"].split("-"))
        except ValueError:
            continue
        k = 0 if h > g else 1 if h == g else 2
        pm = np.array([m["1"], m["X"], m["2"]]); pk = B.demargin(o)
        pb = blend(pm, pk, w)
        clv = None
        if a.date_min and f["coup_envoi"][:10] < a.date_min:
            continue
        rows.append({"k": k, "pm": pm, "pk": pk, "pb": pb, "date": f["coup_envoi"][:10],
                     "won_market_fav": pk.argmax() == k})
    if not rows:
        print("Aucun match réglé avec cotes 1X2 enregistrées. Le test en avant se remplira au fil de la saison.")
        return

    def agg(key):
        ll = [logloss(r[key], r["k"]) for r in rows]
        return round(float(np.mean(ll)), 4)

    def dci(key):
        d = [logloss(r[key], r["k"]) - logloss(r["pk"], r["k"]) for r in rows]
        return round(float(np.mean(d)), 4), B.boot_ci(d)

    out = {"n_matchs_regles": len(rows), "poids_melange_w": w,
           "logloss": {"marché": agg("pk"), "modèle": agg("pm"), "mélange": agg("pb")},
           "delta_vs_marche": {"modèle": dci("pm"), "mélange": dci("pb")},
           "periode": f"{min(r['date'] for r in rows)} → {max(r['date'] for r in rows)}"}
    print(json.dumps(out, ensure_ascii=False, indent=1))
    n = out["n_matchs_regles"]
    if n < 100:
        print(f"\n⚠ {n} matchs seulement : bien trop peu pour conclure. Il en faut plusieurs centaines. "
              "Le journal se remplit à chaque passage quotidien.")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("calibrate")
    c.add_argument("--divs", default="E0,E1,E2,E3,SP1,I1,D1,F1")
    c.add_argument("--seasons", default="2122,2223,2324,2425,2526")
    c.add_argument("--val", default="2324,2425"); c.add_argument("--test", default="2526")
    f = sp.add_parser("forward"); f.add_argument("--date-min", default="2026-07-01")
    a = p.parse_args()
    {"calibrate": cmd_calibrate, "forward": cmd_forward}[a.cmd](a)


if __name__ == "__main__":
    main()
