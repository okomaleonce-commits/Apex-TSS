#!/usr/bin/env python3
"""APEX-UEFA-ELO — force inter-ligue des clubs de coupe européenne, par Elo walk-forward.

Pourquoi : le classement d'une coupe est trop mince pour départager des équipes de ligues
domestiques différentes (d'où les « favoris à 98 % » artefactuels de WORM). Un Elo club construit
sur l'historique continental réel ordonne les forces à travers les ligues.

Honnêteté (mesurée, pas affirmée) : en walk-forward sur 1 358 matchs UEL+UECL 2024-26, cet Elo
**bat le taux de base** (log-loss ~1,030 vs base-rate 1,0384) — un edge réel mais **modeste**, et
sans sur-confiance (quasi aucun « favori ≥ 70 % »). « Battre le taux de base » n'est PAS « battre le
marché » : le statut des moteurs de coupe reste INDICATIF tant qu'un backtest sur cotes n'a pas eu lieu.

Source : data/apifootball/history/{3,848}_*.csv (regénérables via apex_apifootball.py history).
Sortie : data/uefa/club_elo.json. Aucune donnée inventée : un club absent de l'historique est absent.

Usage :
  python3 tools/apex_uefa_elo.py build                 # (re)construit data/uefa/club_elo.json
  python3 tools/apex_uefa_elo.py backtest              # grille de validation vs taux de base
"""
from __future__ import annotations
import argparse
import csv
import glob
import json
import math
import os

HERE = os.path.dirname(__file__)
HIST = os.path.join(HERE, os.pardir, "data", "apifootball", "history")
OUT = os.path.join(HERE, os.pardir, "data", "uefa", "club_elo.json")
LEAGUES = (2, 3, 848)  # UCL, UEL, UECL — pool cross-league complet (un club descendu de C1 garde son Elo)

# Paramètres retenus (grille backtest ci-dessous) : équilibre edge/robustesse, non sur-ajustés.
K_ELO = 20.0        # vitesse de mise à jour Elo (backtest : K=20 meilleur que 30/40)
HA_ELO = 65.0       # avantage terrain en points Elo
K_GOALS = 0.45      # 100 pts Elo ⇒ K_GOALS buts de supériorité attendue
AVG_GOALS = 2.72    # total buts/match mesuré (UEL=UECL)
WARMUP = 3          # nb mini de matchs continentaux avant de faire confiance à l'Elo d'un club


def _rows():
    rows = []
    for lg in LEAGUES:
        for f in sorted(glob.glob(os.path.join(HIST, f"{lg}_*.csv"))):
            for r in csv.DictReader(open(f)):
                if r.get("hg") and r.get("ag"):
                    rows.append((r["date_utc"], r["home"], r["away"], int(r["hg"]), int(r["ag"])))
    rows.sort(key=lambda x: x[0])
    return rows


def _walk_forward(rows, k_elo=K_ELO, ha_elo=HA_ELO):
    """Elo chronologique ; renvoie (elo, seen) à l'issue de tout l'historique."""
    elo, seen = {}, {}
    for _, h, a, hg, ag in rows:
        rh, ra = elo.get(h, 1500.0), elo.get(a, 1500.0)
        E = 1 / (1 + 10 ** (-((rh - ra) + ha_elo) / 400))
        S = 1.0 if hg > ag else (0.5 if hg == ag else 0.0)
        mult = 1.0 + 0.5 * min(2, abs(hg - ag) - 1) if abs(hg - ag) >= 2 else 1.0  # bonus marge
        delta = k_elo * mult * (S - E)
        elo[h], elo[a] = rh + delta, ra - delta
        seen[h], seen[a] = seen.get(h, 0) + 1, seen.get(a, 0) + 1
    return elo, seen


def elo_lambdas(elo_home, elo_away, k_goals=K_GOALS, ha_elo=HA_ELO, avg=AVG_GOALS):
    """λ (dom, ext) depuis l'écart d'Elo + avantage terrain. Supériorité → split du total de buts."""
    sup = k_goals * (((elo_home - elo_away) + ha_elo) / 100.0)
    lh = min(4.5, max(0.15, avg / 2 + sup / 2))
    la = min(4.5, max(0.15, avg / 2 - sup / 2))
    return round(lh, 3), round(la, 3)


def build():
    rows = _rows()
    if not rows:
        print("Aucun historique (data/apifootball/history/{3,848}_*.csv absent). "
              "Lancer d'abord : apex_apifootball.py history --league 3/848 --season 2024/2025.")
        return
    elo, seen = _walk_forward(rows)
    table = {c: round(r, 1) for c, r in elo.items() if seen.get(c, 0) >= 1}
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    payload = {
        "_meta": {
            "statut": "INDICATIF — bat le taux de base en walk-forward (~1,030 vs 1,0384), PAS le marché. Jamais BSM.",
            "n_matchs": len(rows), "n_clubs": len(table),
            "params": {"K_ELO": K_ELO, "HA_ELO": HA_ELO, "K_GOALS": K_GOALS, "AVG_GOALS": AVG_GOALS, "WARMUP": WARMUP},
            "source": "data/apifootball/history/{3,848}_*.csv (UEL+UECL 2024-26)",
        },
        "warmup": WARMUP,
        "seen": seen,
        "elo": table,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    top = sorted(table.items(), key=lambda kv: -kv[1])[:12]
    print(f"Elo construit : {len(table)} clubs sur {len(rows)} matchs → {OUT}")
    print("Top 12 : " + " · ".join(f"{c} {r:.0f}" for c, r in top))


def poisson_1x2(lh, la, mg=12):
    ph = [math.exp(-lh) * lh ** k / math.factorial(k) for k in range(mg + 1)]
    pa = [math.exp(-la) * la ** k / math.factorial(k) for k in range(mg + 1)]
    h = d = a = 0.0
    for i, xi in enumerate(ph):
        for j, xj in enumerate(pa):
            p = xi * xj
            h += p if i > j else 0
            d += p if i == j else 0
            a += p if i < j else 0
    s = h + d + a
    return [h / s, d / s, a / s]


def backtest():
    rows = _rows()
    base = [0.49, 0.205, 0.305]
    base_ll = -sum(math.log(base[0 if hg > ag else (2 if ag > hg else 1)]) for *_, hg, ag in rows) / len(rows)
    print(f"Backtest walk-forward · {len(rows)} matchs UEL+UECL · base-rate log-loss = {base_ll:.4f}")
    print("K_elo kgoals warm |   n  logloss     vs base")
    for k_elo in (20, 30, 40):
        for k_goals in (0.35, 0.45, 0.55):
            for warm in (1, 3):
                elo, seen = {}, {}
                LL = 0.0; n = 0
                for _, h, a, hg, ag in rows:
                    rh, ra = elo.get(h, 1500.0), elo.get(a, 1500.0)
                    if seen.get(h, 0) >= warm and seen.get(a, 0) >= warm:
                        lh, la = elo_lambdas(rh, ra, k_goals)
                        p = poisson_1x2(lh, la)
                        out = 0 if hg > ag else (2 if ag > hg else 1)
                        LL += -math.log(max(p[out], 1e-9)); n += 1
                    E = 1 / (1 + 10 ** (-((rh - ra) + HA_ELO) / 400))
                    S = 1.0 if hg > ag else (0.5 if hg == ag else 0.0)
                    mult = 1.0 + 0.5 * min(2, abs(hg - ag) - 1) if abs(hg - ag) >= 2 else 1.0
                    d = k_elo * mult * (S - E)
                    elo[h], elo[a] = rh + d, ra - d
                    seen[h], seen[a] = seen.get(h, 0) + 1, seen.get(a, 0) + 1
                ll = LL / n if n else 0
                flag = f"  {base_ll - ll:+.4f} <<<" if ll < base_ll else f"  {base_ll - ll:+.4f}"
                print(f"{k_elo:>5} {k_goals:>6} {warm:>4} | {n:>4} {ll:>8.4f}{flag}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="APEX-UEFA-ELO — force inter-ligue des clubs de coupe.")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("build", help="(re)construit data/uefa/club_elo.json")
    sub.add_parser("backtest", help="grille de validation vs taux de base")
    a = ap.parse_args()
    if a.cmd == "backtest":
        backtest()
    else:
        build()
