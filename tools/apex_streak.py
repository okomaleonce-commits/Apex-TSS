#!/usr/bin/env python3
"""APEX-STREAK — backtest du signal CONTRADICTION-DE-SÉRIE (J26).

Hypothèse à MESURER, pas à supposer (garde-fou J26, anti-sophisme du joueur) :
après une série d'un même côté de marché (ex. unders 2.5), le retournement
(over) n'est un signal QUE si les quatre conditions tiennent, puis SEULEMENT si
le backtest walk-forward le valide (ROI > 0 stable). Sinon → WATCH, jamais JOUER.

Les quatre conditions (évaluées avec les seuls matchs ANTÉRIEURS, zéro fuite) :
  1. SÉRIE      — N occurrences consécutives du côté « froid » pour l'équipe.
  2. CONTRADICTION — pendant la série, le marché attendait le côté « chaud »
                  (proba implicite dé-viggée moyenne ≥ C) : l'issue a contredit
                  l'attente la plus sharp disponible.
  3. PLAFOND    — la longueur courante atteint le quantile haut (Q) de la
                  distribution empirique des longueurs de série de l'équipe.
  4. FORME      — la forme récente (F derniers matchs de l'équipe) s'aligne sur
                  le retournement (cadence de buts en hausse vs base de l'équipe).

Données : football-data.co.uk via apex_bsm.fetch_matches (résultats + cotes
Pinnacle pré-match O/U 2.5 et 1X2). Marchés : over25 (ROI réel sur ou25) et
btts (football-data n'a PAS de cote BTTS → hit-rate seulement, ROI NON CALCULABLE,
écrit absent — J25). Walk-forward : apprentissage+amorçage, validation (réglage
des seuils, gelés), test final isolé.

Aucun seuil n'est en dur : ils sont cherchés en validation puis gelés.
Usage : python3 tools/apex_streak.py backtest [--divs ...] [--market over25|btts]
"""
from __future__ import annotations
import argparse, datetime as dt, itertools, json, pathlib, sys, time
import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import apex_bsm as B  # réutilise fetch_matches (football-data.co.uk)

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "backtests"
DIVS = ["E0", "E1", "E2", "E3", "SP1", "I1", "D1", "F1"]
SEASONS = ["2122", "2223", "2324", "2425", "2526"]
TRAIN, VAL, TEST = {"2122", "2223"}, {"2324", "2425"}, {"2526"}


def devig_two_way(odds: list[float] | None) -> float | None:
    """Proba implicite dé-viggée du 1er côté d'un marché à deux issues."""
    if not odds or len(odds) != 2 or min(odds) <= 1.0:
        return None
    inv = [1.0 / o for o in odds]
    return inv[0] / sum(inv)


def hot_cold(m: dict, market: str):
    """(issue chaude réalisée bool, proba chaude attendue par le marché|None,
    cote du côté chaud|None). Côté « chaud » = ce qu'on parie au retournement.
    over25 : chaud=over. btts : chaud=BTTS oui."""
    tot = m["hg"] + m["ag"]
    if market == "over25":
        realized = tot > 2.5
        p_exp = devig_two_way(m.get("ou25"))        # [over, under]
        odds = m["ou25"][0] if m.get("ou25") else None
        return realized, p_exp, odds
    if market == "btts":
        realized = m["hg"] > 0 and m["ag"] > 0
        return realized, None, None                  # pas de cote BTTS dans football-data
    raise ValueError(market)


def team_timeline(matches: list[dict], market: str) -> dict[str, list[dict]]:
    """Par équipe, la suite chronologique de ses matchs avec issue chaude et attente."""
    tl: dict[str, list[dict]] = {}
    for m in sorted(matches, key=lambda x: x["date"]):
        realized, p_exp, odds = hot_cold(m, market)
        for team, isown in ((m["home"], True), (m["away"], False)):
            tl.setdefault(team, []).append({
                "date": m["date"], "hot": realized, "p_exp": p_exp,
                "tot": m["hg"] + m["ag"], "m": m, "odds_hot": odds,
            })
    return tl


def cold_run_lengths(hist: list[dict]) -> list[int]:
    """Longueurs de toutes les séries « froides » (hot=False) terminées dans l'historique."""
    runs, cur = [], 0
    for h in hist:
        if not h["hot"]:
            cur += 1
        else:
            if cur:
                runs.append(cur)
            cur = 0
    if cur:
        runs.append(cur)
    return runs


def signal_before(hist_prior: list[dict], N: int, C: float, Q: float, F: int,
                  market: str) -> bool:
    """Les 4 conditions tiennent-elles juste avant le prochain match ? hist_prior =
    matchs ANTÉRIEURS de l'équipe (ordre chrono)."""
    if len(hist_prior) < max(N, F) + 3:
        return False
    tail = hist_prior[-N:]
    # 1. série de N froids
    if any(h["hot"] for h in tail):
        return False
    # 2. contradiction : le marché attendait le chaud (sur les marchés avec cote)
    if market == "over25":
        ps = [h["p_exp"] for h in tail if h["p_exp"] is not None]
        if len(ps) < N or float(np.mean(ps)) < C:
            return False
    else:  # btts : attente = base empirique antérieure de l'équipe
        base = float(np.mean([1.0 if h["hot"] else 0.0 for h in hist_prior[:-N]])) if len(hist_prior) > N else 0.0
        if base < C:
            return False
    # 3. plafond historique : série courante ≥ quantile Q des séries passées
    runs = cold_run_lengths(hist_prior[:-N]) or cold_run_lengths(hist_prior)
    if not runs:
        return False
    cap = float(np.quantile(runs, Q))
    if N < cap:
        return False
    # 4. forme : cadence de buts récente (F derniers) > base de l'équipe → vers le chaud
    recent = float(np.mean([h["tot"] for h in hist_prior[-F:]]))
    baseline = float(np.mean([h["tot"] for h in hist_prior]))
    return recent > baseline


def run_market(data: dict, seasons: set[str], N, C, Q, F, market):
    """Place le pari de retournement sur les matchs des saisons `seasons`.
    Retourne (n_paris, hits, roi|None, pnl_list)."""
    hits, n, pnl = 0, 0, []
    for div, matches in data.items():
        tl = team_timeline(matches, market)
        for team, hist in tl.items():
            for i in range(len(hist)):
                h = hist[i]
                if h["m"]["season"] not in seasons:
                    continue
                prior = hist[:i]
                if not signal_before(prior, N, C, Q, F, market):
                    continue
                n += 1
                win = h["hot"]
                hits += int(win)
                if market == "over25" and h["odds_hot"]:
                    pnl.append(h["odds_hot"] - 1.0 if win else -1.0)
    roi = float(np.mean(pnl)) if pnl else None
    return n, hits, roi, pnl


def boot_ci(x: list[float], B_=2000, seed=7):
    if not x:
        return None
    rng = np.random.default_rng(seed)
    a = np.array(x)
    bs = [float(a[rng.integers(0, len(a), len(a))].mean()) for _ in range(B_)]
    return [round(float(np.quantile(bs, 0.025)), 4), round(float(np.quantile(bs, 0.975)), 4)]


def backtest(args):
    divs = args.divs.split(",")
    market = args.market
    print(f"# APEX-STREAK backtest — marché={market} ligues={','.join(divs)}", flush=True)
    data = {}
    for d in divs:
        rows = []
        for s in SEASONS:
            try:
                rows += B.fetch_matches(d, s)
            except Exception as e:
                print(f"  ! {d} {s} injoignable : {str(e)[:60]}", flush=True)
        data[d] = rows
        print(f"  {d}: {len(rows)} matchs", flush=True)

    # grille de seuils (apprise en VALIDATION, puis gelée)
    grid = list(itertools.product(
        args.N or [3, 4, 5],          # longueur de série
        args.C or [0.50, 0.55, 0.60], # contradiction (attente chaude moyenne)
        [0.80, 0.90],                 # quantile de plafond
        [4, 6],                       # fenêtre de forme
    ))
    best, best_key = None, None
    for N, C, Q, F in grid:
        n, hits, roi, pnl = run_market(data, VAL, N, C, Q, F, market)
        if n < args.min_bets:
            continue
        key = roi if market == "over25" else (hits / n)  # objectif : ROI (over) / hit-rate (btts)
        if best is None or (key is not None and key > best):
            best, best_key = key, (N, C, Q, F, n, hits, roi)
    if best_key is None:
        print(f"\nVALIDATION : aucune combinaison n'atteint min_bets={args.min_bets}. "
              f"Signal non exploitable sur cet échantillon.", flush=True)
        verdict = "NON VALIDÉ — trop peu de signaux en validation"
        frozen = None
        test = None
    else:
        N, C, Q, F, vn, vh, vroi = best_key
        frozen = {"N": N, "C": C, "Q": Q, "F": F}
        tn, th, troi, tpnl = run_market(data, TEST, N, C, Q, F, market)
        test = {"n": tn, "hits": th, "hit_rate": round(th / tn, 4) if tn else None,
                "roi": round(troi, 4) if troi is not None else None,
                "roi_IC95": boot_ci(tpnl) if tpnl else None}
        # verdict : activer SEULEMENT si ROI>0 (over) stable ; btts = hit-rate seul, jamais ROI
        if market == "over25":
            ok = test["roi"] is not None and test["roi"] > 0 and test["roi_IC95"] and test["roi_IC95"][0] > 0
            verdict = ("VALIDÉ — ROI test > 0 (IC95 > 0)" if ok
                       else "NON VALIDÉ — ROI test ≤ 0 ou IC95 franchit 0 → WATCH, jamais JOUER (J26)")
        else:
            verdict = ("NON VALIDÉ — BTTS : football-data ne fournit pas de cote, ROI NON CALCULABLE ; "
                       "hit-rate seul, insuffisant pour parier (J25)")
        print(f"\nVALIDATION meilleure : N={N} C={C} Q={Q} F={F} (n={vn}, hits={vh}, roi={vroi})", flush=True)
        print(f"TEST FINAL {sorted(TEST)} : {json.dumps(test, ensure_ascii=False)}", flush=True)

    print(f"\nVERDICT : {verdict}", flush=True)

    run_id = f"streak-{market}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    rundir = OUT / run_id
    rundir.mkdir(parents=True, exist_ok=True)
    report = {
        "run_id": run_id, "market": market, "divs": divs, "seasons": SEASONS,
        "split": {"apprentissage_amorcage": sorted(TRAIN), "validation": sorted(VAL), "test_final": sorted(TEST)},
        "expectation_source": ("Pinnacle dé-viggé (football-data ou25)" if market == "over25"
                               else "base empirique antérieure de l'équipe (pas de cote BTTS)"),
        "seuils_geles": frozen, "test": test, "verdict": verdict,
        "note": "J26 : une série n'est un pari qu'après ROI test > 0 stable. Aucun seuil inventé.",
    }
    (rundir / "params.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    _write_report_md(rundir, report)
    print(f"\nÉcrit : {rundir}/REPORT.md", flush=True)
    return report


def _write_report_md(rundir, r):
    L = [f"# Backtest APEX-STREAK — {r['run_id']}", "",
         f"- Marché : **{r['market']}**", f"- Ligues : {', '.join(r['divs'])}",
         f"- Attente (« stat contredite ») : {r['expectation_source']}",
         f"- Split : amorçage {r['split']['apprentissage_amorcage']} · "
         f"validation {r['split']['validation']} · **test {r['split']['test_final']}**",
         f"- Seuils gelés (appris en validation) : `{r['seuils_geles']}`", "",
         f"## Verdict", "", f"**{r['verdict']}**", ""]
    if r["test"]:
        t = r["test"]
        L += ["## Test final (isolé)", "",
              "| n paris | hits | hit-rate | ROI | ROI IC95 |", "|---|---|---|---|---|",
              f"| {t['n']} | {t['hits']} | {t['hit_rate']} | {t['roi']} | {t['roi_IC95']} |", ""]
    L += ["> J26 : signal WATCH tant que ROI test n'est pas > 0 de façon stable. "
          "Les facteurs latents (prématch→postmatch) restent observationnels "
          "(`bet_authority=false`). Aucun seuil inventé : grille apprise en validation, gelée."]
    (rundir / "REPORT.md").write_text("\n".join(L))


def main():
    ap = argparse.ArgumentParser(description="APEX-STREAK — backtest signal contradiction-de-série (J26)")
    sp = ap.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("backtest")
    b.add_argument("--divs", default=",".join(DIVS))
    b.add_argument("--market", default="over25", choices=["over25", "btts"])
    b.add_argument("--min-bets", type=int, default=30, dest="min_bets")
    b.add_argument("--N", type=lambda s: [int(x) for x in s.split(",")], default=None)
    b.add_argument("--C", type=lambda s: [float(x) for x in s.split(",")], default=None)
    b.set_defaults(func=backtest)
    a = ap.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
