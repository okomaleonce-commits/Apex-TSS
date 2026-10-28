#!/usr/bin/env python3
"""APEX-POSTMATCH — boucle d'apprentissage post-match (Lot 3, mod #6).

Ferme la boucle que le reste du système laisse ouverte : après les matchs, relier chaque prévision
BSM à son résultat réel, mesurer la calibration et le CLV, puis RECOMMANDER (sans l'appliquer) le
rafraîchissement des cartes et l'ajout d'une règle de jurisprudence.

    settle/observe → CLV → calibration → (recommandation) refresh carte → (recommandation) jurisprudence

Trois garde-fous repris du reste du dépôt :

  • ANTI-INVENTION : un résultat absent du snapshot WORM est écrit « non résolu », jamais fabriqué.
  • APPEND-ONLY : les observations vont dans ledger/postmatch_observations.jsonl (dédup par
    forecast_id) ; rien n'est réécrit.
  • GEL / NON-MUTATION : les model cards ne naissent QUE d'un vrai backtest (jamais éditées depuis des
    résultats live) et la jurisprudence est append-only « jamais inventée ». Ce module ne touche donc
    NI tools/params/model_cards/*.json NI tools/params/jurisprudence.json : il émet des recommandations
    qu'un humain applique. Il n'émet aucune mise réelle.

Comble aussi un trou réel : apex_bsm.py audit ne calcule la calibration que sur les forecasts
`statut == JOUE` — or sous gel rien n'est JOUE, donc les prévisions SHADOW_ONLY ne nourrissaient
jamais la calibration. Ici, TOUTE prévision dont le score final est résolu nourrit la mesure.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "ledger"
FORECASTS = LEDGER / "forecasts.jsonl"
OBS = LEDGER / "postmatch_observations.jsonl"
CARDS = ROOT / "tools" / "params" / "model_cards"
JURIS = ROOT / "tools" / "params" / "jurisprudence.json"
REPORTS = ROOT / "reports" / "postmatch"


# ───────────────────────────── résolution du résultat ─────────────────────────────

def _final_for_fixture(day: str, fixture_id):
    """Score final (hg, ag) d'un fixture depuis le snapshot WORM du jour, sinon None (anti-invention)."""
    try:
        import apex_clv as CLV
    except Exception:
        return None
    passes = CLV._passes_by_fixture(day).get(fixture_id)
    if not passes:
        return None
    return CLV._final_score(passes)


def _realized_1x2(hg: int, ag: int) -> int:
    return 0 if hg > ag else (1 if hg == ag else 2)


def _grade(market: str, hg: int, ag: int):
    """Règle un marché simple contre le score final. Renvoie 'gagné'/'perdu'/'push' ou None si le
    marché n'est pas interprétable sans ambiguïté (jamais deviné)."""
    m = (market or "").strip().lower()
    tot = hg + ag
    if m in ("1x2 1", "1"):
        return "gagné" if hg > ag else "perdu"
    if m in ("1x2 x", "x"):
        return "gagné" if hg == ag else "perdu"
    if m in ("1x2 2", "2"):
        return "gagné" if ag > hg else "perdu"
    if m.startswith("btts"):
        oui = (hg > 0 and ag > 0)
        want_oui = "oui" in m or "yes" in m
        return "gagné" if (oui == want_oui) else "perdu"
    # Over/Under X.5 (lignes entières → pas de push possible)
    import re
    mo = re.search(r"(over|under)\s*([0-9]+\.5)", m)
    if mo:
        line = float(mo.group(2))
        over = tot > line
        return "gagné" if (over == (mo.group(1) == "over")) else "perdu"
    return None   # AH, DNB, lignes asiatiques : non réglés ici (cote/règlement partiel non reconstruits)


def _brier3(p, k) -> float:
    return sum((p[i] - (1.0 if i == k else 0.0)) ** 2 for i in range(3))


def _logloss3(p, k) -> float:
    return -math.log(max(p[k], 1e-12))


# ───────────────────────────── observation (append-only) ─────────────────────────────

def _load_observed_ids() -> set:
    if not OBS.exists():
        return set()
    out = set()
    for line in OBS.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                out.add(json.loads(line)["forecast_id"])
            except (json.JSONDecodeError, KeyError):
                continue
    return out


def _forecasts_for_day(day: str) -> list:
    """Prévisions dont le coup d'envoi tombe le jour `day` (comparaison sur la date ISO)."""
    if not FORECASTS.exists():
        return []
    out = []
    for line in FORECASTS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            f = json.loads(line)
        except json.JSONDecodeError:
            continue
        ko = (f.get("coup_envoi") or "")[:10]
        if ko == day:
            out.append(f)
    return out


def observe_day(day: str, apply_write: bool = True) -> dict:
    """Relie chaque prévision du jour à son résultat réel (snapshot WORM) et calcule Brier/log-loss.
    Renvoie {observed, pending, rows}. N'écrit QUE des entrées nouvelles (dédup par forecast_id)."""
    done = _load_observed_ids()
    rows, pending = [], []
    for f in _forecasts_for_day(day):
        fid_fc = f.get("forecast_id")
        fin = _final_for_fixture(day, f.get("fixture_id"))
        if fin is None:
            pending.append({"forecast_id": fid_fc, "fixture_id": f.get("fixture_id"),
                            "match": f"{f.get('home')} – {f.get('away')}",
                            "raison": "résultat absent du snapshot WORM (non résolu, non fabriqué)"})
            continue
        hg, ag = fin
        k = _realized_1x2(hg, ag)
        M = f.get("marches") or {}
        p = [M.get("1"), M.get("X"), M.get("2")]
        brier = logloss = None
        if all(isinstance(x, (int, float)) for x in p):
            brier = round(_brier3(p, k), 4)
            logloss = round(_logloss3(p, k), 4)
        off = f.get("official_selection") or None
        sel_result = _grade(off.get("marche"), hg, ag) if (off and off.get("marche")) else None
        row = {"forecast_id": fid_fc, "observe_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
               "day": day, "fixture_id": f.get("fixture_id"), "div": f.get("div") or None,
               "match": f"{f.get('home')} – {f.get('away')}", "score": f"{hg}-{ag}",
               "realized_1x2": ("1", "X", "2")[k],
               "predicted_1x2": [round(x, 4) for x in p] if brier is not None else None,
               "brier_1x2": brier, "logloss_1x2": logloss,
               "selection": (off or {}).get("marche"), "selection_result": sel_result,
               "shadow": f.get("shadow"), "statut_execution": f.get("statut_execution"),
               "model_version": f.get("model_version")}
        rows.append(row)
    # append-only : seules les nouvelles observations sont écrites
    new = [r for r in rows if r["forecast_id"] not in done]
    if apply_write and new:
        LEDGER.mkdir(exist_ok=True)
        with open(OBS, "a", encoding="utf-8") as fh:
            for r in new:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    return {"observed": rows, "new": new, "pending": pending}


# ───────────────────────────── calibration (cumulée) ─────────────────────────────

def _all_observations() -> list:
    if not OBS.exists():
        return []
    out = []
    for line in OBS.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def calibration(rows: list) -> dict:
    """Brier/log-loss moyens (1X2) sur les observations fournies, globalement et par ligue. Baseline
    naïve = prédire la base empirique 1X2 de l'échantillon (référence honnête : si le modèle ne bat
    pas ça, il n'informe pas). Échantillon court → mesure indicative, ne valide rien (J01/J10)."""
    scored = [r for r in rows if r.get("brier_1x2") is not None]
    n = len(scored)
    if n == 0:
        return {"n": 0, "brier": None, "logloss": None, "baseline_brier": None, "par_ligue": {}}
    brier = sum(r["brier_1x2"] for r in scored) / n
    logloss = sum(r["logloss_1x2"] for r in scored) / n
    # base empirique observée
    freq = {"1": 0, "X": 0, "2": 0}
    for r in scored:
        freq[r["realized_1x2"]] += 1
    base = [freq["1"] / n, freq["X"] / n, freq["2"] / n]
    base_brier = 0.0
    for r in scored:
        k = ("1", "X", "2").index(r["realized_1x2"])
        base_brier += _brier3(base, k)
    base_brier /= n
    par_ligue = {}
    for r in scored:
        d = r.get("div") or "?"
        g = par_ligue.setdefault(d, {"n": 0, "brier": 0.0})
        g["n"] += 1
        g["brier"] += r["brier_1x2"]
    for d, g in par_ligue.items():
        g["brier"] = round(g["brier"] / g["n"], 4)
    return {"n": n, "brier": round(brier, 4), "logloss": round(logloss, 4),
            "baseline_brier": round(base_brier, 4),
            "bat_baseline": brier < base_brier, "par_ligue": par_ligue}


# ───────────────────────────── recommandations (NON appliquées) ─────────────────────────────

def _load_card(div: str):
    p = CARDS / f"{div}.json"
    try:
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None
    except (OSError, ValueError):
        return None


def recommendations(calib: dict, clv: dict) -> list:
    """Recommandations à APPLIQUER PAR UN HUMAIN (jamais auto) : refresh de carte, règle de
    jurisprudence. On recommande, on n'édite pas (cartes = backtest only ; jurisprudence = append-only
    jamais inventée). Chaque reco cite la mesure qui la motive."""
    recos = []
    for div, g in (calib.get("par_ligue") or {}).items():
        if g["n"] < 20:          # J10 : trop peu pour conclure
            continue
        card = _load_card(div)
        if not card:
            continue
        recos.append({
            "type": "REFRESH_CARTE",
            "ligue": div,
            "mesure": f"Brier observé {g['brier']} sur {g['n']} matchs (vs baseline globale {calib.get('baseline_brier')}).",
            "action": f"RELANCER un backtest réel : `python3 tools/apex_bsm.py backtest --divs {div}` "
                      f"puis vérifier la carte régénérée. NE PAS éditer {div}.json à la main.",
        })
    # Règle de jurisprudence : seulement si un échec SYSTÉMATIQUE et mesuré apparaît (CLV franchement
    # négatif sur un échantillon non trivial). Sinon, rien (pas de règle inventée).
    if clv and clv.get("clv_n", 0) >= 20 and clv.get("clv_moyen") is not None and clv["clv_moyen"] < -0.02:
        recos.append({
            "type": "JURISPRUDENCE",
            "mesure": f"CLV moyen {clv['clv_moyen']:+.4f} sur {clv['clv_n']} paris (bat la clôture "
                      f"{clv.get('clv_beat_close_pct')} %).",
            "action": "PROPOSER (append-only) une règle Jxx dans JURISPRUDENCE.md + jurisprudence.json "
                      "actant que le marché recommandé détruit la CLV sur cet échantillon. "
                      f"Version suggérée : {_next_version()}. Décision humaine requise.",
        })
    return recos


def _next_version() -> str:
    try:
        v = json.loads(JURIS.read_text(encoding="utf-8")).get("version", "1.0.0")
        a, b, c = (list(map(int, v.split("."))) + [0, 0, 0])[:3]
        return f"{a}.{b + 1}.0"
    except Exception:
        return "?"


# ───────────────────────────── rapport ─────────────────────────────

def build_report(day: str, obs: dict, calib: dict, clv: dict, recos: list) -> str:
    L = [f"# APEX-POSTMATCH — {day}", "",
         "*Boucle d'apprentissage post-match (lecture + append-only). Aucune mise réelle (gel actif). "
         "Les cartes et la jurisprudence ne sont PAS modifiées ici : voir les recommandations.*", "",
         "## 1. Observation des prévisions",
         f"- Prévisions du jour résolues : **{len(obs['observed'])}** "
         f"(nouvelles écrites : {len(obs['new'])}) · non résolues : **{len(obs['pending'])}**"]
    for r in obs["observed"][:30]:
        sel = f" · sélection {r['selection']} → {r['selection_result']}" if r.get("selection") else ""
        bl = f"Brier {r['brier_1x2']}" if r["brier_1x2"] is not None else "Brier n/d"
        L.append(f"  - {r['match']} : {r['score']} ({r['realized_1x2']}) · {bl}{sel}")
    for pz in obs["pending"][:30]:
        L.append(f"  - ⏳ {pz['match']} : {pz['raison']}")
    L += ["", "## 2. CLV (APEX-CLV, marchés Over/Under 2.5 à cote stockée)"]
    if clv and clv.get("n_bets_roi"):
        L.append(f"- Paris avec ROI calculable : {clv['n_bets_roi']} · P&L {clv['pnl_units']:+.2f} u · "
                 f"ROI {clv['roi']*100:+.2f} %" if clv.get("roi") is not None else
                 f"- Paris avec ROI calculable : {clv['n_bets_roi']}")
        if clv.get("clv_n"):
            L.append(f"- CLV moyen {clv['clv_moyen']*100:+.2f} % · bat la clôture {clv['clv_beat_close_pct']} % "
                     f"des {clv['clv_n']} paris")
    else:
        L.append("- Aucun pari OU 2.5 à cote stockée ce jour (CLV non calculable — non fabriqué).")
    L += ["", "## 3. Calibration 1X2 (SHADOW incluse — ce que l'audit JOUE-only manquait)"]
    if calib["n"]:
        verdict = "bat" if calib.get("bat_baseline") else "NE bat PAS"
        L.append(f"- n={calib['n']} · Brier {calib['brier']} · log-loss {calib['logloss']} · "
                 f"baseline empirique {calib['baseline_brier']} → le modèle {verdict} la baseline.")
        L.append(f"- Par ligue : " + ", ".join(f"{d} (n={g['n']}, Brier {g['brier']})"
                                                for d, g in calib["par_ligue"].items()))
        L.append("- *Échantillon court ⇒ mesure indicative : ne valide ni n'invalide le modèle (J01/J10).*")
    else:
        L.append("- Aucune prévision résolue avec 1X2 prévu → calibration non calculable (non fabriquée).")
    L += ["", "## 4. Recommandations (à appliquer par un humain — jamais auto)"]
    if recos:
        for r in recos:
            L.append(f"- **{r['type']}**" + (f" · {r['ligue']}" if r.get("ligue") else "")
                     + f" — {r['mesure']} {r['action']}")
    else:
        L.append("- Aucune : échantillon insuffisant ou aucune dérive mesurée. Rien n'est inventé.")
    L += ["", "---", "*Gel actif : aucune mise réelle. Observations append-only "
          "(ledger/postmatch_observations.jsonl). Cartes = backtest only ; jurisprudence = append-only.*"]
    return "\n".join(L)


def cmd_run(a):
    day = a.date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    obs = observe_day(day, apply_write=not a.dry_run)
    all_obs = _all_observations()
    if not a.dry_run:
        # inclure les observations du jour qui viennent d'être écrites
        pass
    calib = calibration(all_obs if all_obs else obs["observed"])
    try:
        import apex_clv as CLV
        clv = CLV.summary([day])
    except Exception as e:  # noqa: BLE001
        clv = {"n_bets_roi": 0, "clv_n": 0, "error": str(e)[:120]}
    recos = recommendations(calib, clv)
    report = build_report(day, obs, calib, clv, recos)
    if not a.dry_run:
        REPORTS.mkdir(parents=True, exist_ok=True)
        (REPORTS / f"{day}.md").write_text(report, encoding="utf-8")
    print(report)
    if not a.dry_run:
        print(f"\nRapport : reports/postmatch/{day}.md · observations : ledger/postmatch_observations.jsonl (append-only)")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    r = sp.add_parser("run", help="observe le jour → CLV → calibration → recommandations")
    r.add_argument("--date", help="AAAA-MM-JJ (défaut : aujourd'hui UTC)")
    r.add_argument("--dry-run", action="store_true", help="n'écrit ni observations ni rapport")
    a = p.parse_args(argv)
    return {"run": cmd_run}[a.cmd](a)


if __name__ == "__main__":
    raise SystemExit(main())
