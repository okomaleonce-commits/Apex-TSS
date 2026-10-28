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


# ───────────────────────── FORWARD-TEST (J26 prospectif) ─────────────────────────
# Le backtest BTTS est impossible (pas de cote BTTS historique). La seule façon
# honnête de valider le signal est prospective : enregistrer la cote live AVANT le
# coup d'envoi, régler après, cumuler ROI + CLV. Les cotes live viennent du snapshot
# WORM (API-Football), qui porte déjà BTTS et O/U 2.5.
FWD_LEAGUES = {39: "E0", 40: "E1", 41: "E2", 42: "E3", 140: "SP1", 135: "I1", 78: "D1", 61: "F1"}
FROZEN = {"over25": {"N": 3, "C": 0.50, "Q": 0.80, "F": 6},
          "btts":   {"N": 4, "C": 0.50, "Q": 0.80, "F": 6}}
FWD_LEDGER = ROOT / "ledger" / "forward_streak.jsonl"
FWD_SETTLED = ROOT / "ledger" / "forward_streak_settled.jsonl"


def _norm(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return "".join(c for c in s.lower() if c.isalnum())


def _match_team(name, candidates):
    """Meilleur nom football-data pour un libellé API-Football (exact normalisé, sinon
    chevauchement de tokens). None si rien de convaincant."""
    n = _norm(name)
    norm = {c: _norm(c) for c in candidates}
    for c, cn in norm.items():
        if cn == n:
            return c
    best, score = None, 0.0
    for c, cn in norm.items():
        if n and (n in cn or cn in n):
            s = min(len(n), len(cn)) / max(len(n), len(cn))
            if s > score:
                best, score = c, s
    return best if score >= 0.6 else None


def _hot_odd_in(fixture_odds, market, prefer_book=None):
    """(cote du côté chaud, book) depuis un bloc odds multi-books. Si prefer_book est fourni et porte
    le marché, on le prend EN PRIORITÉ (CLV comparable book à book), sinon le premier book disponible."""
    if not isinstance(fixture_odds, dict):
        return None, None
    order = ([prefer_book] if prefer_book else []) + \
            [b for b in fixture_odds if b != "_update" and b != prefer_book]
    for book in order:
        v = fixture_odds.get(book)
        if not isinstance(v, dict):
            continue
        if market == "btts" and v.get("BTTS"):
            return v["BTTS"][0], book           # BTTS oui
        if market == "over25" and v.get("OU", {}).get("2.5"):
            return v["OU"]["2.5"][0], book       # Over 2.5
    return None, None


def _live_odds(fixture_odds, market):
    """(cote du côté chaud, source) depuis le bloc odds multi-books du snapshot (1er book qui l'a)."""
    return _hot_odd_in(fixture_odds, market)


def _closing_odds(day, fixture_id, market, prefer_book=None):
    """Cote de CLÔTURE du côté chaud = cote au DERNIER passage PRÉMATCH du snapshot du jour (même book
    que l'entrée si possible). (cote, book) ou (None, None) si introuvable — JAMAIS estimée (J05)."""
    try:
        import apex_clv as CLV
    except Exception:
        return None, None
    passes = CLV._passes_by_fixture(day).get(fixture_id)
    if not passes:
        return None, None
    prematch = [p for p in passes if p.get("phase") == "PREMATCH"]
    if not prematch:
        return None, None
    return _hot_odd_in(prematch[-1].get("odds"), market, prefer_book)


def _season_code(d: dt.date) -> str:
    start = d.year if d.month >= 7 else d.year - 1
    return f"{start % 100:02d}{(start + 1) % 100:02d}"


def cmd_forward(a):
    market = a.market
    day = a.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    snap = ROOT / "data" / "worm" / "snapshots" / f"{day}.jsonl"
    if not snap.exists():
        print(f"Snapshot WORM absent : {snap} — lancer d'abord apex_worm/apex_fusion scan. "
              f"Rien enregistré (anti-invention).", flush=True)
        return
    rows = [json.loads(l) for l in open(snap, encoding="utf-8") if l.strip()]
    inscope = [r for r in rows if r.get("league_id") in FWD_LEAGUES]
    th = FROZEN[market]
    # entrées déjà ouvertes (dédup)
    seen = set()
    if FWD_LEDGER.exists():
        for l in open(FWD_LEDGER, encoding="utf-8"):
            try:
                e = json.loads(l); seen.add((e["fixture_id"], e["market"], e["team_trigger"]))
            except Exception:
                pass
    hist_cache = {}
    recorded = 0
    print(f"# APEX-STREAK forward — {day} · marché={market} · seuils gelés {th}", flush=True)
    for r in inscope:
        div = FWD_LEAGUES[r["league_id"]]
        try:
            fix_date = dt.date.fromisoformat((r.get("kickoff") or "")[:10])
        except Exception:
            continue
        if div not in hist_cache:
            seasons = sorted({_season_code(fix_date), _season_code(fix_date - dt.timedelta(days=120))})
            rowsh = []
            for s in seasons:
                try:
                    rowsh += B.fetch_matches(div, s)
                except Exception:
                    pass
            hist_cache[div] = team_timeline(rowsh, market)
        tl = hist_cache[div]
        odds_hot, src = _live_odds(r.get("odds"), market)
        for api_name in (r.get("home"), r.get("away")):
            team = _match_team(api_name, tl.keys())
            if not team:
                continue
            prior = [h for h in tl[team] if h["date"] < fix_date]
            if not signal_before(prior, th["N"], th["C"], th["Q"], th["F"], market):
                continue
            key = (r["fixture_id"], market, team)
            if key in seen:
                continue
            if not odds_hot:
                print(f"  ⚠ signal {team} ({r['home']}–{r['away']}) mais cote {market} absente du snapshot → non enregistré (J05)", flush=True)
                continue
            entry = {
                "ts_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                "day": day, "div": div, "fixture_id": r["fixture_id"],
                "home": r["home"], "away": r["away"], "kickoff": r.get("kickoff"),
                "market": market, "side": "oui" if market == "btts" else "over",
                "team_trigger": team, "serie_longueur": th["N"],
                "cote_entree": odds_hot, "cote_source": src,
                "seuils": th, "statut": "OUVERT",
                "note": "forward-test J26 — WATCH, pas une mise réelle (gel) ; sert à mesurer ROI/CLV prospectif",
            }
            FWD_LEDGER.parent.mkdir(parents=True, exist_ok=True)
            with open(FWD_LEDGER, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            seen.add(key); recorded += 1
            print(f"  ✓ {div} {r['home']}–{r['away']} | série {market} sur {team} | cote {odds_hot} ({src})", flush=True)
    print(f"\n{recorded} entrée(s) forward enregistrée(s) dans {FWD_LEDGER}", flush=True)
    print("Rappel : forward-test = WATCH prospectif (gel actif). Régler après match : "
          "python3 tools/apex_streak.py forward-settle", flush=True)


def cmd_forward_settle(a):
    if not FWD_LEDGER.exists():
        print("Aucune entrée forward.", flush=True)
        return
    entries = [json.loads(l) for l in open(FWD_LEDGER, encoding="utf-8") if l.strip()]
    already = set()
    if FWD_SETTLED.exists():
        for l in open(FWD_SETTLED, encoding="utf-8"):
            try:
                already.add(json.loads(l)["ref_ts"])
            except Exception:
                pass
    res_cache = {}
    pnl, settled = [], 0
    for e in entries:
        if e["ts_utc"] in already:
            # déjà réglé → réintégrer son P&L au cumul
            continue
        div = e["div"]
        try:
            fdate = dt.date.fromisoformat((e.get("kickoff") or e["day"])[:10])
        except Exception:
            continue
        season = _season_code(fdate)
        if (div, season) not in res_cache:
            try:
                res_cache[(div, season)] = B.fetch_matches(div, season)
            except Exception:
                res_cache[(div, season)] = []
        m = next((x for x in res_cache[(div, season)]
                  if x["date"] == fdate
                  and _match_team(e["home"], [x["home"]]) and _match_team(e["away"], [x["away"]])), None)
        if not m:
            continue  # pas encore joué / introuvable
        tot = m["hg"] + m["ag"]
        hot = (tot > 2.5) if e["market"] == "over25" else (m["hg"] > 0 and m["ag"] > 0)
        win = bool(hot)  # on parie toujours le côté chaud (retournement)
        gain = (e["cote_entree"] - 1.0) if win else -1.0
        # CLV câblé (tâche ouverte) : cote de clôture = dernier passage prématch du snapshot, même
        # book que l'entrée si possible. CLV = cote_entrée / cote_clôture − 1 (positif = on a pris
        # mieux que la clôture). Cote de clôture absente → CLV null, jamais estimé (J05/anti-invention).
        close_odd, close_book = _closing_odds(e.get("day"), e["fixture_id"], e["market"], e.get("cote_source"))
        if close_odd and close_odd > 1.0:
            clv = round(e["cote_entree"] / close_odd - 1.0, 4)
            clv_note = f"cote clôture {close_odd} ({close_book}) au dernier passage prématch du snapshot"
        else:
            clv, clv_note = None, "cote de clôture absente du snapshot (CLV non calculable, non fabriqué)"
        rec = {"ref_ts": e["ts_utc"], "fixture_id": e["fixture_id"], "div": div,
               "match": f"{e['home']}–{e['away']}", "market": e["market"],
               "score": f"{m['hg']}-{m['ag']}", "resultat": "GAGNÉ" if win else "PERDU",
               "cote_entree": e["cote_entree"], "cote_cloture": close_odd, "pnl_unites": round(gain, 3),
               "clv": clv, "clv_note": clv_note,
               "regle_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
        with open(FWD_SETTLED, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        pnl.append(gain); settled += 1
        clv_txt = (f" · CLV {clv:+.1%}" if clv is not None else " · CLV n/d")
        print(f"  {rec['resultat']:6} {rec['match']} {rec['score']} | {e['market']} @ {e['cote_entree']}"
              f" → {gain:+.2f} u{clv_txt}", flush=True)
    # cumul global (toutes lignes réglées)
    allp, allclv = [], []
    if FWD_SETTLED.exists():
        for l in open(FWD_SETTLED, encoding="utf-8"):
            try:
                rr = json.loads(l)
                allp.append(rr["pnl_unites"])
                if rr.get("clv") is not None:
                    allclv.append(rr["clv"])
            except Exception:
                pass
    beat = sum(1 for c in allclv if c > 0)
    print(f"\n{settled} nouvelle(s) ligne(s) réglée(s). Cumul forward-test : {len(allp)} paris · "
          f"ROI {(np.mean(allp) if allp else 0):+.1%} · P&L {sum(allp):+.1f} u"
          + (f" · IC95 {boot_ci(allp)}" if len(allp) >= 10 else " · (IC95 à ≥10 paris)"), flush=True)
    if allclv:
        print(f"CLV : moyen {np.mean(allclv):+.1%} sur {len(allclv)} paris cotés · bat la clôture "
              f"{100*beat/len(allclv):.0f} % (le CLV est le juge, J26/J01).", flush=True)
    else:
        print("CLV : aucun pari avec cote de clôture relevée (CLV non calculable).", flush=True)
    print("Statut : forward-test prospectif (gel). Activation du signal (J26) seulement si ROI ET CLV "
          "positifs stables sur échantillon suffisant.", flush=True)


def main():
    ap = argparse.ArgumentParser(description="APEX-STREAK — signal contradiction-de-série (J26) : backtest + forward-test")
    sp = ap.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("backtest")
    b.add_argument("--divs", default=",".join(DIVS))
    b.add_argument("--market", default="over25", choices=["over25", "btts"])
    b.add_argument("--min-bets", type=int, default=30, dest="min_bets")
    b.add_argument("--N", type=lambda s: [int(x) for x in s.split(",")], default=None)
    b.add_argument("--C", type=lambda s: [float(x) for x in s.split(",")], default=None)
    b.set_defaults(func=backtest)
    fw = sp.add_parser("forward", help="enregistrer les signaux du jour avec cote live (avant coup d'envoi)")
    fw.add_argument("--date", help="YYYY-MM-DD (défaut aujourd'hui UTC) ; lit data/worm/snapshots/<date>.jsonl")
    fw.add_argument("--market", default="btts", choices=["over25", "btts"])
    fw.set_defaults(func=cmd_forward)
    fs = sp.add_parser("forward-settle", help="régler les entrées forward jouées et cumuler le ROI")
    fs.set_defaults(func=cmd_forward_settle)
    a = ap.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
