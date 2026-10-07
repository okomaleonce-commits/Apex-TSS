#!/usr/bin/env python3
"""APEX-CHARACTER — caractère & schéma d'un match, INDÉPENDANT des cotes et du pricing statistique.

Idée : un match a un « caractère » (sa forme réelle sur le terrain) qui existe en dehors du
marché et des modèles de value. Ce module :

  1. CLASSIFIE chaque match déjà backtesté (historique football-data.co.uk, data/history/*.csv)
     en un PROFIL primaire (partition en 6 classes à partir du score final) + des TAGS de
     dynamique (schéma MT→FT : renversement, égalisation tardive, seconde période folle, BTTS…).
  2. BACKTESTE en walk-forward : pour chaque match, prédit la DISTRIBUTION de profils AVANT match
     à partir des seules forces d'équipe (buts marqués/encaissés glissants) → λ structurels →
     matrice de scores Poisson. Aucune cote, aucun xG marché. Puis compare au profil APRÈS match :
     exactitude top-1, log-loss, calibration, taux de base par ligue.
  3. EXPOSE profile(lh, la) → schéma AVANT match (probas des 6 profils) réutilisable partout
     (WORM, SYNC, PROTOCOL) car tous disposent déjà de λ structurels.

« Avant / après » : le schéma AVANT = distribution de profils prédite (odds-free) ; le schéma
APRÈS = profil réellement observé + tags de dynamique. On n'invente rien : si une donnée manque
(score, mi-temps), le match est ignoré ou le tag est omis.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HIST_DIR = ROOT / "data" / "history"
OUT_DIR = ROOT / "data" / "character"
BT_DIR = ROOT / "backtests"

# ───────────────────────── taxonomie des profils (partition du score final) ─────────────────────────
# 6 profils primaires, MUTUELLEMENT EXCLUSIFS et EXHAUSTIFS sur tout score (hg, ag).
PROFILS = ["VERROU", "MATCH_FERME", "EQUILIBRE", "BATAILLE_OUVERTE", "VICTOIRE_NETTE", "DEMONSTRATION"]
PROFIL_LABEL = {
    "VERROU": "🔒 Verrou (0-0)",
    "MATCH_FERME": "🧱 Match fermé (1 but)",
    "EQUILIBRE": "⚖️ Équilibré serré (2-3 buts, écart ≤1)",
    "BATAILLE_OUVERTE": "🔥 Bataille ouverte (≥4 buts, écart ≤1)",
    "VICTOIRE_NETTE": "✔️ Victoire nette (écart 2)",
    "DEMONSTRATION": "💥 Démonstration (écart ≥3)",
}


def profil_of(hg: int, ag: int) -> str:
    """Profil primaire du score final — partition exhaustive, sans cote ni stat marché."""
    tot = hg + ag
    diff = abs(hg - ag)
    if diff >= 3:
        return "DEMONSTRATION"
    if diff == 2:
        return "VICTOIRE_NETTE"
    # écart ≤ 1 à partir d'ici
    if tot == 0:
        return "VERROU"
    if tot >= 4:
        return "BATAILLE_OUVERTE"
    if tot == 1:
        return "MATCH_FERME"
    return "EQUILIBRE"  # écart ≤1, 2 ou 3 buts


def _sign(x: int) -> int:
    return (x > 0) - (x < 0)


def dynamique_tags(hg, ag, hthg, htag) -> list[str]:
    """Schéma MT→FT (tags indépendants). hthg/htag peuvent être None (mi-temps absente)."""
    tags = []
    if hg > 0 and ag > 0:
        tags.append("BTTS")
    if hg + ag >= 3:
        tags.append("OVER25")
    if hg + ag <= 1:
        tags.append("FERME")
    if hthg is None or htag is None:
        return tags
    ht = _sign(hthg - htag)
    ft = _sign(hg - ag)
    if ht != 0 and ft == -ht:
        tags.append("RENVERSEMENT")        # l'équipe menée à la MT gagne
    elif ht != 0 and ft == 0:
        tags.append("EGALISATION_TARDIVE")  # la MT a un leader, le FT est nul
    elif ht == 0 and ft != 0:
        tags.append("DECISION_2E_PERIODE")  # 0 écart à la MT, vainqueur au FT
    sh_home = hg - hthg
    sh_away = ag - htag
    if sh_home + sh_away >= 3:
        tags.append("SECONDE_PERIODE_FOLLE")
    if ag == 0:
        tags.append("CLEAN_SHEET_DOM")
    if hg == 0:
        tags.append("CLEAN_SHEET_EXT")
    return tags


# ───────────────────────── Poisson : schéma AVANT match (odds-free) ─────────────────────────

def _pois_pmf(lam: float, k: int) -> float:
    return math.exp(-lam) * lam ** k / math.factorial(k)


def profil_probs(lh: float, la: float, max_goals: int = 10) -> dict:
    """Distribution des 6 profils à partir des seuls λ structurels (aucune cote).

    C'est le SCHÉMA ATTENDU AVANT match : purement structurel (forces d'équipe)."""
    lh = max(0.05, min(6.0, float(lh)))
    la = max(0.05, min(6.0, float(la)))
    ph = [_pois_pmf(lh, i) for i in range(max_goals + 1)]
    pa = [_pois_pmf(la, j) for j in range(max_goals + 1)]
    probs = {p: 0.0 for p in PROFILS}
    s = 0.0
    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            pij = ph[i] * pa[j]
            probs[profil_of(i, j)] += pij
            s += pij
    if s > 0:
        for p in probs:
            probs[p] /= s
    return probs


def expected_profile(lh: float, la: float) -> dict:
    """Schéma AVANT match prêt à l'emploi : profil dominant + distribution + buts attendus."""
    probs = profil_probs(lh, la)
    top = max(probs, key=probs.get)
    return {
        "profil_attendu": top,
        "label": PROFIL_LABEL[top],
        "p_top": round(probs[top], 4),
        "distribution": {p: round(v, 4) for p, v in probs.items()},
        "buts_attendus": round(lh + la, 2),
        "lambda": [round(lh, 3), round(la, 3)],
        "provenance": "CALCULATED (Poisson structurel, sans cote)",
    }


# ───────────────────────── lecture historique ─────────────────────────

def _date(s: str):
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _int(v):
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def load_division(path: Path) -> list[dict]:
    text = path.read_bytes().decode("utf-8-sig", errors="replace")
    div = path.stem.split("_")[0]
    out = []
    for r in csv.DictReader(io.StringIO(text)):
        d = _date(r.get("Date", ""))
        hg, ag = _int(r.get("FTHG")), _int(r.get("FTAG"))
        if not d or not r.get("HomeTeam") or hg is None or ag is None:
            continue
        out.append({
            "div": div, "date": d, "home": r["HomeTeam"].strip(), "away": r["AwayTeam"].strip(),
            "hg": hg, "ag": ag, "hthg": _int(r.get("HTHG")), "htag": _int(r.get("HTAG")),
        })
    out.sort(key=lambda m: m["date"])
    return out


def load_all() -> dict:
    """{div: [matchs triés par date]} pour toutes les saisons d'une division fusionnées."""
    leagues: dict[str, list] = {}
    for path in sorted(HIST_DIR.glob("*.csv")):
        if "_" not in path.stem:
            continue
        rows = load_division(path)
        leagues.setdefault(rows[0]["div"] if rows else path.stem.split("_")[0], []).extend(rows)
    for div in leagues:
        leagues[div].sort(key=lambda m: m["date"])
    return leagues


# ───────────────────────── forces d'équipe glissantes (walk-forward, odds-free) ─────────────────────────

class LeagueState:
    """Attaque/défense glissantes par équipe (moyenne exponentielle), + moyennes de ligue.

    Tout est construit UNIQUEMENT à partir des buts passés : aucune cote, aucun xG marché."""

    def __init__(self, half_life: int = 20, prior: float = 1.3):
        self.alpha = 1.0 - 0.5 ** (1.0 / max(1, half_life))
        self.prior = prior
        self.atk: dict[str, float] = {}   # buts marqués / match (glissant)
        self.dfn: dict[str, float] = {}   # buts encaissés / match (glissant)
        self.n: dict[str, int] = {}
        self.lg_home = prior              # moyenne buts domicile de ligue
        self.lg_away = prior * 0.82
        self._hw = prior
        self._aw = prior * 0.82
        self._seen = 0

    def lambdas(self, home: str, away: str):
        ha = self.atk.get(home, self.prior)
        hd = self.dfn.get(home, self.prior)
        aa = self.atk.get(away, self.prior)
        ad = self.dfn.get(away, self.prior)
        base_h = max(0.3, self.lg_home)
        base_a = max(0.3, self.lg_away)
        # force relative attaque × faiblesse défensive adverse, ancrée sur la moyenne de ligue
        lh = (ha / self.prior) * (ad / self.prior) * base_h
        la = (aa / self.prior) * (hd / self.prior) * base_a
        return max(0.1, min(5.0, lh)), max(0.1, min(5.0, la))

    def ready(self, home: str, away: str) -> bool:
        return self.n.get(home, 0) >= 4 and self.n.get(away, 0) >= 4

    def update(self, home, away, hg, ag):
        a = self.alpha
        self.atk[home] = (1 - a) * self.atk.get(home, self.prior) + a * hg
        self.dfn[home] = (1 - a) * self.dfn.get(home, self.prior) + a * ag
        self.atk[away] = (1 - a) * self.atk.get(away, self.prior) + a * ag
        self.dfn[away] = (1 - a) * self.dfn.get(away, self.prior) + a * hg
        self.n[home] = self.n.get(home, 0) + 1
        self.n[away] = self.n.get(away, 0) + 1
        self._hw = (1 - a) * self._hw + a * hg
        self._aw = (1 - a) * self._aw + a * ag
        self._seen += 1
        if self._seen >= 10:
            self.lg_home, self.lg_away = self._hw, self._aw


# ───────────────────────── backtest ─────────────────────────

def backtest(leagues: dict, half_life: int = 20) -> dict:
    per_league = {}
    glob = {"n": 0, "top1": 0, "logloss": 0.0,
            "profil_obs": {p: 0 for p in PROFILS}, "profil_pred": {p: 0 for p in PROFILS},
            "tags": {}}
    # calibration : bins de proba prédite du profil réalisé
    cal_bins = [{"lo": i / 10, "hi": (i + 1) / 10, "sum_p": 0.0, "hit": 0, "n": 0} for i in range(10)]
    for div, matches in leagues.items():
        st = LeagueState(half_life=half_life)
        L = {"n": 0, "top1": 0, "logloss": 0.0,
             "profil_obs": {p: 0 for p in PROFILS}, "tags": {}, "n_total": len(matches)}
        for m in matches:
            obs = profil_of(m["hg"], m["ag"])
            if st.ready(m["home"], m["away"]):
                lh, la = st.lambdas(m["home"], m["away"])
                probs = profil_probs(lh, la)
                pred = max(probs, key=probs.get)
                p_obs = max(1e-9, probs[obs])
                L["n"] += 1
                L["top1"] += int(pred == obs)
                L["logloss"] += -math.log(p_obs)
                glob["n"] += 1
                glob["top1"] += int(pred == obs)
                glob["logloss"] += -math.log(p_obs)
                glob["profil_pred"][pred] += 1
                b = min(9, int(p_obs * 10))
                cal_bins[b]["sum_p"] += p_obs
                cal_bins[b]["hit"] += 1  # p_obs est la proba du profil réalisé → "hit" par construction pondéré
                cal_bins[b]["n"] += 1
            L["profil_obs"][obs] += 1
            glob["profil_obs"][obs] += 1
            for t in dynamique_tags(m["hg"], m["ag"], m["hthg"], m["htag"]):
                L["tags"][t] = L["tags"].get(t, 0) + 1
                glob["tags"][t] = glob["tags"].get(t, 0) + 1
            st.update(m["home"], m["away"], m["hg"], m["ag"])
        if L["n"]:
            L["acc_top1"] = round(L["top1"] / L["n"], 4)
            L["logloss_moy"] = round(L["logloss"] / L["n"], 4)
        L["taux_profil"] = {p: round(L["profil_obs"][p] / max(1, len(matches)), 4) for p in PROFILS}
        L["taux_tags"] = {t: round(c / max(1, len(matches)), 4) for t, c in sorted(L["tags"].items())}
        per_league[div] = L
    summary = {
        "genere_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "n_matchs_total": sum(len(v) for v in leagues.values()),
        "n_predits": glob["n"],
        "acc_top1": round(glob["top1"] / max(1, glob["n"]), 4),
        "logloss_moy": round(glob["logloss"] / max(1, glob["n"]), 4),
        "baseline_top1": round(max(glob["profil_obs"].values()) / max(1, sum(glob["profil_obs"].values())), 4),
        "taux_profil_global": {p: round(glob["profil_obs"][p] / max(1, sum(glob["profil_obs"].values())), 4)
                               for p in PROFILS},
        "taux_tags_global": {t: round(c / max(1, sum(len(v) for v in leagues.values())), 4)
                             for t, c in sorted(glob["tags"].items(), key=lambda kv: -kv[1])},
        "calibration": [{"bin": f"{b['lo']:.1f}-{b['hi']:.1f}",
                         "p_moy_prevue": round(b["sum_p"] / b["n"], 4) if b["n"] else None,
                         "n": b["n"]} for b in cal_bins],
        "par_ligue": {d: {"n_total": L["n_total"], "n_predits": L["n"],
                          "acc_top1": L.get("acc_top1"), "logloss_moy": L.get("logloss_moy"),
                          "taux_profil": L["taux_profil"], "taux_tags": L["taux_tags"]}
                      for d, L in per_league.items()},
    }
    return summary


# ───────────────────────── rapport ─────────────────────────

def write_report(summary: dict, run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    L = []
    L.append("# APEX-CHARACTER — backtest du caractère de match")
    L.append("")
    L.append(f"- Généré : {summary['genere_utc']}")
    L.append(f"- Matchs classés : **{summary['n_matchs_total']}** · prédits en walk-forward : "
             f"**{summary['n_predits']}**")
    L.append(f"- Exactitude profil top-1 : **{summary['acc_top1']:.1%}** "
             f"(baseline « toujours le plus fréquent » : {summary['baseline_top1']:.1%})")
    L.append(f"- Log-loss moyen (profil réalisé) : **{summary['logloss_moy']:.3f}**")
    L.append("")
    L.append("> Caractère = forme réelle du match, **indépendante des cotes et du pricing statistique**. "
             "Le schéma AVANT match est la distribution de profils prédite par les seules forces d'équipe "
             "(λ structurels Poisson) ; le schéma APRÈS match est le profil réellement observé + les tags "
             "de dynamique MT→FT. Walk-forward : chaque prédiction n'utilise que le passé.")
    L.append("")
    L.append("## Répartition des profils (observée, toutes ligues)")
    L.append("| Profil | Part |")
    L.append("|---|---|")
    for p in PROFILS:
        L.append(f"| {PROFIL_LABEL[p]} | {summary['taux_profil_global'][p]:.1%} |")
    L.append("")
    L.append("## Schémas de dynamique (tags MT→FT, part des matchs)")
    L.append("| Tag | Part |")
    L.append("|---|---|")
    for t, v in summary["taux_tags_global"].items():
        L.append(f"| {t} | {v:.1%} |")
    L.append("")
    L.append("## Calibration (proba prévue du profil réalisé)")
    L.append("| Bin | p moyenne prévue | n |")
    L.append("|---|---|---|")
    for c in summary["calibration"]:
        if c["n"]:
            L.append(f"| {c['bin']} | {c['p_moy_prevue']} | {c['n']} |")
    L.append("")
    L.append("## Par ligue")
    L.append("| Ligue | n | top-1 | log-loss | profil dominant |")
    L.append("|---|---|---|---|---|")
    for d, x in summary["par_ligue"].items():
        dom = max(x["taux_profil"], key=x["taux_profil"].get)
        L.append(f"| {d} | {x['n_total']} | {x['acc_top1'] or '—'} | {x['logloss_moy'] or '—'} | "
                 f"{dom} {x['taux_profil'][dom]:.0%} |")
    L.append("")
    L.append("_Taxonomie : " + " · ".join(PROFIL_LABEL[p] for p in PROFILS) + "._")
    out = run_dir / "REPORT.md"
    out.write_text("\n".join(L), encoding="utf-8")
    return out


# ───────────────────────── CLI ─────────────────────────

def cmd_backtest(a):
    leagues = load_all()
    if not leagues:
        print("Aucun historique dans data/history/*.csv — rien à backtester.")
        return
    summary = backtest(leagues, half_life=a.half_life)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = BT_DIR / f"character-{ts}"
    report = write_report(summary, run_dir)
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    params = {
        "run_id": run_dir.name, "genere_utc": summary["genere_utc"],
        "model_version": "apex-character-1.0.0",
        "acc_top1": summary["acc_top1"], "baseline_top1": summary["baseline_top1"],
        "logloss_moy": summary["logloss_moy"],
        "taux_profil_global": summary["taux_profil_global"],
        "taux_tags_global": summary["taux_tags_global"],
        "taux_profil_par_ligue": {d: x["taux_profil"] for d, x in summary["par_ligue"].items()},
    }
    (OUT_DIR / "latest_params.json").write_text(json.dumps(params, ensure_ascii=False, indent=1),
                                                encoding="utf-8")
    print(f"Matchs classés : {summary['n_matchs_total']} · prédits : {summary['n_predits']}")
    print(f"Exactitude top-1 : {summary['acc_top1']:.1%} (baseline {summary['baseline_top1']:.1%}) · "
          f"log-loss {summary['logloss_moy']:.3f}")
    print("Profils globaux : " + " · ".join(
        f"{p} {summary['taux_profil_global'][p]:.0%}" for p in PROFILS))
    print(f"REPORT : {report.relative_to(ROOT)}")
    print(f"PARAMS : {(OUT_DIR / 'latest_params.json').relative_to(ROOT)}")


def cmd_classify(a):
    prof = profil_of(a.hg, a.ag)
    tags = dynamique_tags(a.hg, a.ag, a.hthg, a.htag)
    print(json.dumps({"score": f"{a.hg}-{a.ag}", "mi_temps": (f"{a.hthg}-{a.htag}"
          if a.hthg is not None and a.htag is not None else None),
          "profil": prof, "label": PROFIL_LABEL[prof], "tags_dynamique": tags},
          ensure_ascii=False, indent=1))


def cmd_profile(a):
    print(json.dumps(expected_profile(a.lh, a.la), ensure_ascii=False, indent=1))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)

    b = sp.add_parser("backtest", help="classer + backtester le caractère sur tout l'historique")
    b.add_argument("--half-life", type=int, default=20)
    b.set_defaults(func=cmd_backtest)

    c = sp.add_parser("classify", help="profil + tags d'un score (après match)")
    c.add_argument("--hg", type=int, required=True)
    c.add_argument("--ag", type=int, required=True)
    c.add_argument("--hthg", type=int, default=None)
    c.add_argument("--htag", type=int, default=None)
    c.set_defaults(func=cmd_classify)

    pr = sp.add_parser("profile", help="schéma AVANT match (probas des 6 profils) depuis λ structurels")
    pr.add_argument("--lh", type=float, required=True, help="λ domicile (buts attendus)")
    pr.add_argument("--la", type=float, required=True, help="λ extérieur")
    pr.set_defaults(func=cmd_profile)

    a = p.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
