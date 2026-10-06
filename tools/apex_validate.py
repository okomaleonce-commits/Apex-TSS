#!/usr/bin/env python3
"""APEX-VALIDATE — validation prospective multi-jours (audit 2026-10-05, étape 4).

Agrège les bilans journaliers append-only (`data/worm/bilans/<jour>.json`) en un verdict cumulé,
HONNÊTE sur deux points que le bilan d'une seule journée ne montre pas :

  1. INCERTITUDE : un taux de réussite sur 30-50 paris a un intervalle de confiance large. On calcule
     l'intervalle de Wilson 95 % ; si sa borne basse ne dépasse pas le seuil de rentabilité, il n'y a
     AUCUN bord démontré, quel que soit le taux ponctuel.
  2. RENTABILITÉ ≠ TAUX : gagner 66 % à cote 1,50 ne rapporte RIEN (break-even = 1/1,50 = 66,7 %).
     Pour chaque famille de marché on affiche le seuil de rentabilité à une cote de référence
     PRUDENTE, et on compare la borne basse du taux à ce seuil.

Ce module NE price pas et N'autorise aucune mise. Il mesure, et le plus souvent il doit conclure
« bord NON démontré » — c'est le résultat attendu tant que le gel est en place. Les cotes réellement
obtenues ne sont pas verrouillées par décision : un vrai ROI/CLV reste non calculable ici, et c'est
dit explicitement.
"""
from __future__ import annotations

import glob
import json
import math
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BILANS = ROOT / "data" / "worm" / "bilans"

# Cote de référence PRUDENTE par famille de marché (pour le seuil de rentabilité). Volontairement
# basse (donc exigeante) : si le système ne bat même pas ce seuil prudent, il ne bat rien. Ce ne sont
# PAS les cotes obtenues (non verrouillées) — juste un repère pour transformer un taux en rentabilité.
REF_ODDS = {
    "Under 2.5": 1.55,
    "Over 2.5": 1.75,
    "Handicap asiatique -0.5/-1": 1.90,
    "Double chance 1X / +0.5 AH": 1.35,
    "Double chance X2 / +0.5 AH": 1.40,
}
DEFAULT_REF_ODD = 1.80


def wilson(k, n, z=1.96):
    """Intervalle de Wilson 95 % pour une proportion k/n. Renvoie (bas, centre, haut)."""
    if n <= 0:
        return (0.0, 0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    marge = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / d
    return (max(0.0, centre - marge), p, min(1.0, centre + marge))


def _accumulate(key):
    """Cumule {gagné, demi, perdu, n} par clé (ex. 'par_marche') sur tous les bilans disponibles."""
    agg = {}
    files = sorted(glob.glob(str(BILANS / "*.json")))
    jours = []
    for fp in files:
        try:
            b = json.loads(Path(fp).read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        jours.append(b.get("jour", Path(fp).stem))
        for name, s in (b.get(key) or {}).items():
            a = agg.setdefault(name, {"gagné": 0, "demi": 0, "perdu": 0})
            a["gagné"] += int(s.get("gagné", 0))
            a["demi"] += int(s.get("demi", 0))
            a["perdu"] += int(s.get("perdu", 0))
    return agg, jours


def _ref_odd(market_name):
    for k, v in REF_ODDS.items():
        if market_name.startswith(k):
            return v
    return DEFAULT_REF_ODD


def evaluate(key="par_marche"):
    agg, jours = _accumulate(key)
    rows = []
    for name, a in sorted(agg.items(), key=lambda kv: -(kv[1]["gagné"] + kv[1]["demi"] + kv[1]["perdu"])):
        wins = a["gagné"] + 0.5 * a["demi"]
        n = a["gagné"] + a["demi"] + a["perdu"]          # push exclus en amont (bilan)
        if n == 0:
            continue
        lo, mid, hi = wilson(wins, n)
        odd = _ref_odd(name)
        breakeven = 1.0 / odd
        # Bord démontré UNIQUEMENT si la borne BASSE de l'intervalle dépasse le seuil de rentabilité.
        edge = "BORD DÉMONTRÉ" if lo > breakeven else ("insuffisant" if mid > breakeven else "sous le seuil")
        rows.append({"nom": name, "n": n, "taux": round(mid, 3), "ic_bas": round(lo, 3),
                     "ic_haut": round(hi, 3), "cote_ref": odd, "breakeven": round(breakeven, 3),
                     "verdict": edge})
    return {"jours": jours, "key": key, "rows": rows}


def verdict_global():
    """Verdict binaire honnête : existe-t-il AU MOINS une famille de marché dont la borne basse de
    l'IC95 dépasse son seuil de rentabilité, sur un échantillon décent (n ≥ 30) ? Sinon : NON démontré."""
    ev = evaluate("par_marche")
    demontres = [r for r in ev["rows"] if r["verdict"] == "BORD DÉMONTRÉ" and r["n"] >= 30]
    # Un « bord conditionnel » n'autorise JAMAIS à lui seul la levée du gel : il reste suspendu à des
    # conditions non satisfaites (cotes non verrouillées, jours corrélés, contradiction du backtest
    # calibré). `gel_levable` est donc toujours False tant que ces conditions ne sont pas levées.
    return {"jours": ev["jours"], "n_jours": len(ev["jours"]),
            "bord_conditionnel": bool(demontres), "familles_avec_bord": [r["nom"] for r in demontres],
            "gel_levable": False, "detail": ev["rows"]}


def _fmt(ev):
    L = [f"APEX-VALIDATE · {ev['key']} · {len(ev['jours'])} jours : {', '.join(ev['jours'])}",
         f"{'Marché/famille':<34} {'N':>4} {'taux':>6} {'IC95 bas':>9} {'break-even':>11}  verdict"]
    for r in ev["rows"]:
        L.append(f"{r['nom'][:34]:<34} {r['n']:>4} {r['taux']*100:>5.1f}% {r['ic_bas']*100:>8.1f}% "
                 f"{r['breakeven']*100:>10.1f}% (@{r['cote_ref']})  {r['verdict']}")
    return "\n".join(L)


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Validation prospective multi-jours APEX (honnête).")
    ap.add_argument("--by", choices=["par_marche", "par_signal", "par_palier"], default="par_marche")
    a = ap.parse_args(argv)
    ev = evaluate(a.by)
    print(_fmt(ev))
    v = verdict_global()
    print()
    if v["bord_conditionnel"]:
        print(f"VERDICT : bord CONDITIONNEL sur {v['familles_avec_bord']} — la borne basse de l'IC95 "
              f"(n ≥ 30) dépasse le seuil de rentabilité À COTE PRUDENTE SUPPOSÉE. Ce n'est PAS un bord "
              "constaté : il tient seulement si ces cotes sont réellement obtenues.")
    else:
        print("VERDICT : AUCUN bord même conditionnel. Le gel reste justifié.")
    print("\nLe gel N'EST PAS levable sur cette base. Conditions non satisfaites :")
    print("  1. Cotes obtenues NON verrouillées par décision → ROI/CLV réel non calculable (le bord")
    print("     s'évapore si les cotes réelles sont sous la référence).")
    print("  2. 6 jours seulement, fortement corrélés (mêmes ligues récurrentes) → sur-ajustement probable.")
    print("  3. Contredit le backtest CALIBRÉ (BSM : ROI −12,79 %, log-loss > marché), test plus rigoureux.")
    print("  → Étapes requises avant toute levée : verrouiller la cote par décision (CLV), accumuler")
    print("     en AVANT sur ≥ 4-6 semaines indépendantes, et réconcilier avec le backtest calibré.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
