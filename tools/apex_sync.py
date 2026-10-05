#!/usr/bin/env python3
"""APEX-SYNC — chef d'orchestre WORM ↔ PROTOCOL.

APEX-WORM est le radar rapide : il scanne toute la fenêtre APEX et repère les anomalies
(Sharp / Blowout / Upset / StatsConvergence, mouvements de ligne). APEX-SYNC lit sa sortie,
filtre et priorise les candidats, puis ne transmet que les meilleurs au PROTOCOL lent et rigoureux
(moteur de simulation calibré `tools/apex_bsm.py`). Un Risk Manager strict s'applique ensuite, et
chaque match reçoit un feu :

  VERT   (GO)      : anomalie WORM + sélection PROTOCOL à EV ≥ seuil + Risk Manager OK.
  ORANGE (WATCH)   : candidat valide mais PROTOCOL non exécuté, compositions manquantes,
                     value non confirmée, ou mise sous le plancher.
  ROUGE  (NO BET)  : porte échouée, veto/abstention PROTOCOL, ou refus du Risk Manager.

Boucle (spec du plan §3) :
  WORM scan → candidats → filtres/priorité → PROTOCOL (bsm simulate) → Risk Manager → feu → journal.

Honnêteté (CLAUDE.md) : APEX-SYNC n'invente JAMAIS une simulation, une cote ni une probabilité.
Il ne fait que relayer ce que WORM a réellement relevé (λ structurels issus du classement, cotes
horodatées du snapshot) vers le moteur BSM, qui marque lui-même ces λ comme NON VALIDÉS tant qu'ils
ne sont pas comparés au marché en backtest. Ce qui manque est écrit comme manquant. APEX-SYNC
optimise le PROCESSUS de décision, pas le résultat : aucun outil ne garantit un gain.

Usage :
  python3 tools/apex_sync.py candidates [--date AAAA-MM-JJ] [--min-priority 0.5] [--max N]
  python3 tools/apex_sync.py sync       [--date AAAA-MM-JJ] [--run-protocol] [--bankroll 1000]
  python3 tools/apex_sync.py risk --p 0.58 --odds 2.05 [--bankroll 1000] [--match-id ID] [--league L]
  python3 tools/apex_sync.py kpi        [--date AAAA-MM-JJ]
  python3 tools/apex_sync.py window
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_worm as W      # noqa: E402  (fenêtre APEX, snapshots, démarge, sélection de book via W.AF)
import apex_common as C    # noqa: E402  (identité de match, horodatage UTC)

ROOT = Path(__file__).resolve().parent.parent
SYNC_DIR = ROOT / "data" / "sync"
CAND_DIR = SYNC_DIR / "candidates"
DEC_DIR = SYNC_DIR / "decisions"
LEDGER_DIR = ROOT / "ledger"
BSM = ROOT / "tools" / "apex_bsm.py"

# ─── feux ───
GREEN, ORANGE, RED = "VERT", "ORANGE", "ROUGE"
LIGHT_LABEL = {GREEN: "🟢 GO", ORANGE: "🟠 WATCH", RED: "🔴 NO BET"}

# Gel de promotion (audit 2026-10-05) : tant que la stratégie n'est pas validée, AUCUN chemin
# (sync, risk autonome) ne doit afficher d'autorisation financière VERTE. Le calcul reste exact en
# interne (testable), mais toute sortie « VERT » est rabattue sur ORANGE « gelé ».
PROMOTION_FROZEN = True

# Poids des tags d'anomalie (plan §4) : un signal Sharp confirmé prime, la convergence stats aussi.
TAG_WEIGHT = {"SHARP": 1.0, "STATSCONVERGENCE": 0.9, "UPSET": 0.7, "BLOWOUT": 0.6}

DEFAULT_CONFIG = {
    # ── filtres / priorisation (plan §3.6, §4) ──
    "min_lead_minutes": 30,      # trop proche du coup d'envoi = cotes figées, PROTOCOL n'a pas le temps
    "max_lead_hours": 96,        # trop tôt = données incomplètes
    "min_data_quality": 45,      # en dessous, WORM lui-même refuse une reco
    "min_worm_score": 45,        # seuil d'anomalie exploitable (identique à WORM)
    "queue_capacity": 12,        # ne pas engorger le pipeline PROTOCOL (plan §3.6)
    "min_priority": 0.0,
    # ── Risk Manager (plan §6) ──
    "bankroll": 1000.0,
    "kelly_fraction": 0.25,      # Kelly fractionné (1/4)
    "max_stake_pct": 0.01,       # mise max 1 % de bankroll par pari
    "min_stake_pct": 0.0025,     # sous 0,25 % → WATCH plutôt que GO
    "max_exposure_match": 0.01,  # exposition max par match
    "max_exposure_league": 0.03,
    "max_exposure_market": 0.03,
    "stop_loss_daily": -0.05,    # stop-loss quotidien (−5 % de bankroll)
    "stop_loss_weekly": -0.10,
    "min_ev": 0.03,              # EV ≥ 3 % (CLAUDE.md §4)
}

# Marchés à règlement plein, sûrs pour un dimensionnement Kelly direct (p, cote).
FULL_SETTLE_PREFIX = ("1X2", "Over", "Under", "BTTS")


# ───────────────────────── primitives pures (testables, hors réseau) ─────────────────────────

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def worm_best(rec: dict) -> float:
    """Meilleur score d'anomalie WORM (0-100). None = absent, pas 0."""
    vals = [rec.get(k) for k in ("sharp", "blowout", "upset", "convergence")]
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else 0


def active_tags(rec: dict, threshold: int = 45) -> list[str]:
    m = {"SHARP": rec.get("sharp"), "BLOWOUT": rec.get("blowout"),
         "UPSET": rec.get("upset"), "STATSCONVERGENCE": rec.get("convergence")}
    ranked = sorted(((k, v) for k, v in m.items() if v is not None and v >= threshold),
                    key=lambda kv: kv[1], reverse=True)
    return [k for k, _ in ranked]


def tag_weight(tags: list[str]) -> float:
    """Poids [0,1] des tags actifs. Sharp + convergence simultanés font monter la confiance (plan §4)."""
    if not tags:
        return 0.0
    base = max(TAG_WEIGHT.get(t, 0.5) for t in tags)
    if "SHARP" in tags and "STATSCONVERGENCE" in tags:
        base += 0.15
    return clamp01(base)


def liquidity_score(rec: dict) -> float:
    """Liquidité/fiabilité marché [0,1] : nb de books, faible dispersion, volume d'échange si connu.
    Dispersion ou volume inconnus ne sont pas comptés comme nuls — terme neutre, jamais inventé."""
    odds = rec.get("odds") or {}
    nbook = len([k for k in odds if not k.startswith("_")])
    book_term = min(1.0, nbook / 8.0)
    disp = rec.get("market_dispersion")
    disp_term = 0.4 if disp is None else clamp01(1.0 - disp / 0.02)
    exch = rec.get("exchange") or {}
    vol = exch.get("total_matched")
    exch_term = clamp01(math.log10(vol) / 6.0) if vol and vol > 0 else 0.0  # ~1 M€ → 1.0
    return clamp01(0.5 * book_term + 0.3 * disp_term + 0.2 * exch_term)


def hours_to_kickoff(rec: dict, now: dt.datetime | None = None):
    ko = rec.get("kickoff")
    if not ko:
        return None
    try:
        k = dt.datetime.fromisoformat(ko)
    except (ValueError, TypeError):
        return None
    if k.tzinfo is None:
        k = k.replace(tzinfo=dt.timezone.utc)
    now = now or dt.datetime.now(dt.timezone.utc)
    return (k - now).total_seconds() / 3600.0


def time_score(h) -> float:
    """Fenêtre d'opportunité [0,1] en fonction des heures avant le coup d'envoi (plan §4).
    Trop tôt (>72 h) ou trop tard (<30 min) décote ; palier haut entre 2 h et 18 h."""
    if h is None or h <= 0:
        return 0.0
    if h < 0.5:
        return 0.2
    if h < 2:
        return 0.5 + (h - 0.5) / 1.5 * 0.4      # 0.5 h → 0.5 ; 2 h → 0.9
    if h <= 18:
        return 1.0
    if h <= 72:
        return clamp01(1.0 - (h - 18) / 54 * 0.6)  # 18 h → 1.0 ; 72 h → 0.4
    return 0.2


def compute_priority(rec: dict, now: dt.datetime | None = None) -> dict:
    """priority = 0.4·worm + 0.2·liquidité + 0.2·temps + 0.2·tags  (plan §4)."""
    ws = clamp01(worm_best(rec) / 100.0)
    liq = liquidity_score(rec)
    h = hours_to_kickoff(rec, now)
    ts = time_score(h)
    tags = active_tags(rec)
    tw = tag_weight(tags)
    pri = 0.4 * ws + 0.2 * liq + 0.2 * ts + 0.2 * tw
    return {"priority": round(pri, 4), "worm_score": round(ws, 4), "liquidity": round(liq, 4),
            "time_score": round(ts, 4), "tag_weight": round(tw, 4),
            "hours_to_kickoff": (round(h, 2) if h is not None else None), "tags": tags}


def basic_filters(rec: dict, cfg: dict, now: dt.datetime | None = None):
    """Portes d'entrée (plan §3.6) : renvoie (eligible, [raisons de rejet])."""
    reasons = []
    if rec.get("phase") != "PREMATCH":
        reasons.append(f"phase {rec.get('phase')} (SYNC ne traite que le prématch)")
    h = hours_to_kickoff(rec, now)
    if h is None:
        reasons.append("coup d'envoi illisible")
    else:
        if h * 60 < cfg["min_lead_minutes"]:
            reasons.append(f"coup d'envoi trop proche ({h * 60:.0f} min < {cfg['min_lead_minutes']} min)")
        if h > cfg["max_lead_hours"]:
            reasons.append(f"coup d'envoi trop lointain ({h:.0f} h > {cfg['max_lead_hours']} h)")
    dq = rec.get("data_quality") or 0
    if dq < cfg["min_data_quality"]:
        reasons.append(f"data_quality {dq} < {cfg['min_data_quality']}")
    if worm_best(rec) < cfg["min_worm_score"]:
        reasons.append(f"meilleur signal WORM {worm_best(rec)} < {cfg['min_worm_score']}")
    reco = rec.get("reco") or {}
    if reco.get("primary_market") in (None, "NO BET"):
        reasons.append("WORM : NO BET (aucune anomalie exploitable)")
    # Veto d'intégrité : un match dont le handicap asiatique a bougé anormalement est NEUTRALISÉ par
    # WORM (jamais parié). SYNC doit propager ce veto et refuser la candidature (audit 2026-10-05).
    if reco.get("integrity_blocked") or (rec.get("asian_integrity") or {}).get("suspect"):
        reasons.append("intégrité AH suspecte — match NEUTRALISÉ (jamais candidat)")
    _, o1 = W.AF.pick_book(rec.get("odds") or {}, "1X2")
    if not o1:
        reasons.append("aucune cote 1X2 (liquidité/marché indisponible)")
    return (len(reasons) == 0, reasons)


def candidate_of(rec: dict, cfg: dict, now: dt.datetime | None = None) -> dict:
    ok, reasons = basic_filters(rec, cfg, now)
    pr = compute_priority(rec, now)
    reco = rec.get("reco") or {}
    dec = reco.get("decision") or {}
    return {
        "match_id": C.match_id_of(rec.get("home", ""), rec.get("away", ""), rec.get("kickoff", "")),
        "fixture_id": rec.get("fixture_id"),
        "home": rec.get("home"), "away": rec.get("away"),
        "league": rec.get("league"), "league_id": rec.get("league_id"),
        "country": rec.get("country"), "kickoff": rec.get("kickoff"), "phase": rec.get("phase"),
        "data_quality": rec.get("data_quality"), "confidence": rec.get("confidence"),
        "worm_market": reco.get("primary_market"), "worm_tier": dec.get("tier"),
        "worm_signal": reco.get("signal_dominant"),
        "caractere_attendu": _caractere(rec),
        "eligible": ok, "filter_reasons": reasons,
        **pr,
    }


def _caractere(rec: dict):
    """Schéma AVANT match (APEX-CHARACTER) depuis les λ structurels du snapshot — odds-free.
    Renvoie {profil, p_top} ou None si les λ manquent (jamais inventé)."""
    lam = rec.get("lambdas")
    if not lam or len(lam) != 2:
        return None
    try:
        import apex_character as CH
        ep = CH.expected_profile(lam[0], lam[1])
        return {"profil": ep["profil_attendu"], "p_top": ep["p_top"], "buts_attendus": ep["buts_attendus"]}
    except Exception:  # noqa: BLE001
        return None


def select_candidates(records: list[dict], cfg: dict, now: dt.datetime | None = None) -> dict:
    """Filtre + priorise + applique la capacité de file (plan §3.6, §4)."""
    cands = [candidate_of(r, cfg, now) for r in records]
    eligible = [c for c in cands if c["eligible"] and c["priority"] >= cfg["min_priority"]]
    eligible.sort(key=lambda c: c["priority"], reverse=True)
    queued = eligible[:cfg["queue_capacity"]]
    overflow = eligible[cfg["queue_capacity"]:]
    rejected = [c for c in cands if not c["eligible"]]
    return {"queued": queued, "overflow": overflow, "rejected": rejected, "total": len(cands)}


# ───────────────────────── pont vers le PROTOCOL (bsm simulate) ─────────────────────────

def build_protocol_command(rec: dict, cfg: dict) -> dict:
    """Construit la commande `apex_bsm.py simulate` à partir du SEUL snapshot WORM (plan §3.7).
    N'invente rien : les λ sont les λ structurels réels de WORM (classement), les cotes sont celles
    du snapshot horodaté. Renvoie {argv, runnable, reason} — runnable=False si WORM n'a pas pu
    estimer de λ (pas de force d'équipe) : dans ce cas le PROTOCOL n'est pas exécutable hors ligne."""
    odds = rec.get("odds") or {}
    _, o1 = W.AF.pick_book(odds, "1X2")
    _, ou = W.AF.pick_book(odds, "OU")
    _, bt = W.AF.pick_book(odds, "BTTS")
    lam = rec.get("lambdas")
    argv = ["python3", str(BSM), "simulate", "--home", rec.get("home", ""), "--away", rec.get("away", "")]
    if rec.get("kickoff"):
        argv += ["--kickoff", rec["kickoff"]]
    runnable, reason = True, None
    if lam and len(lam) == 2 and all(lam):
        argv += ["--lh", f"{float(lam[0]):.3f}", "--la", f"{float(lam[1]):.3f}",
                 "--lambda-source", "APEX-WORM λ structurels (classement)",
                 "--status-note", "NON VALIDÉ — λ structurels WORM (non comparés au marché)"]
    else:
        runnable = False
        reason = "λ structurels absents (force d'équipe non disponible) : PROTOCOL non exécutable hors ligne"
    if o1:
        argv += ["--odds-1x2", ",".join(f"{x:.2f}" for x in o1)]
    if ou and ou.get("2.5") and all(ou["2.5"]):
        argv += ["--odds-ou25", ",".join(f"{x:.2f}" for x in ou["2.5"])]
    if bt and all(bt):
        argv += ["--odds-btts", ",".join(f"{x:.2f}" for x in bt)]
    if rec.get("fixture_id"):
        argv += ["--fixture-id", str(rec["fixture_id"])]
    argv += ["--odds-source", "APEX-WORM snapshot", "--odds-time", rec.get("scan_time_utc", "")]
    if not rec.get("compositions"):
        argv += ["--missing-lineup"]   # honnêteté : compositions absentes → PROTOCOL posera un veto WAIT
    return {"argv": argv, "runnable": runnable, "reason": reason}


def run_protocol(argv: list[str], home: str, away: str, kickoff: str) -> dict:
    """Exécute bsm simulate --record et relit le forecast déposé au journal (append-only).
    Renvoie une synthèse {status, decision, official, forecast_id, ...} ou {status:'error'}."""
    before = _ledger_ids()
    argv = list(argv) + ["--record"]
    try:
        proc = subprocess.run(argv, cwd=str(ROOT), capture_output=True, text=True, timeout=300)
    except (subprocess.TimeoutExpired, OSError) as e:
        return {"status": "error", "reason": f"exécution PROTOCOL impossible : {e}"}
    if proc.returncode != 0:
        return {"status": "error", "reason": (proc.stderr or proc.stdout or "code != 0").strip()[:300]}
    fc = _ledger_new_forecast(before, home, away, kickoff)
    if not fc:
        return {"status": "error", "reason": "forecast non retrouvé au journal après exécution"}
    return summarize_forecast(fc)


# Statuts de modèle considérés comme VALIDÉS (liste blanche POSITIVE et tracée). Tout autre statut —
# vide, « INCONNU », « NON VALIDÉ », texte libre — n'autorise RIEN (audit 2026-10-05 : l'ancienne
# règle « non vide et ne contient pas NON VALID » laissait passer des statuts inconnus).
VALIDATED_STATUSES = {"VALIDÉ", "VALIDE", "VALIDATED", "MODÈLE VALIDÉ", "MODELE VALIDE", "OK"}


def _model_validated(fc: dict) -> bool:
    """Validation POSITIVE et explicite : le feu VERT exige un statut_modele COMMENÇANT par un
    préfixe de la liste blanche (BSM émet « VALIDÉ (walk-forward …) »). Les λ structurels WORM
    (« NON VALIDÉ »), « NON CONCLUANT » et tout statut inconnu/vide sont refusés (audit 2026-10-05)."""
    sm = (fc.get("statut_modele") or "").strip().upper()
    if not sm or "NON VALID" in sm:
        return False
    return any(sm.startswith(p) for p in VALIDATED_STATUSES)


def _is_full_settle(market) -> bool:
    """Marché à règlement PLEIN binaire (p, cote) : 1X2, BTTS, ou Over/Under sur ligne .5 uniquement.
    Les lignes quart (2.25/2.75) règlent en demi, les lignes entières (2.0) peuvent faire push, un
    marché absent/handicap est partiel — tous REFUSÉS (audit 2026-10-05)."""
    if not market:
        return False
    m = str(market).strip()
    if m.startswith(("1X2", "BTTS")):
        return True
    mm = re.match(r"^(Over|Under)\s+([0-9]+(?:\.[0-9]+)?)\b", m)
    if mm:
        x = float(mm.group(2))
        return abs((x % 1.0) - 0.5) < 1e-9   # seule une ligne .5 règle en binaire sans push ni split
    return False


def summarize_forecast(fc: dict, min_ev: float = DEFAULT_CONFIG["min_ev"]) -> dict:
    """Traduit un forecast BSM en statut PROTOCOL + sélection officielle (plan §3.9).

    AUDIT 2026-10-05 : SYNC ne CHOISIT PLUS un marché. Il conserve EXACTEMENT la sélection officielle
    que BSM a transmise (`official_selection`), avec son éventuel veto ; il ne reconstruit jamais une
    sélection depuis la liste d'EV brute (sinon il réintroduit une branche écartée par BSM pour
    instabilité). Sans sélection officielle structurée → abstention."""
    decision = fc.get("decision", "")
    out = {"status": None, "decision": decision, "forecast_id": fc.get("forecast_id"),
           "statut_modele": fc.get("statut_modele"), "official": None}
    if fc.get("statut_mise") != "PROPOSÉE":
        if decision.startswith("SURVEILLANCE") or "WAIT" in decision.upper() or "composition" in decision.lower():
            out["status"] = "wait"
        else:
            out["status"] = "abstention"
        return out
    # Garde-fou : modèle NON VALIDÉ (ou statut inconnu) → jamais de sélection, jamais VERT.
    if not _model_validated(fc):
        out["status"] = "unvalidated"
        return out
    off = fc.get("official_selection")
    if not isinstance(off, dict) or not off.get("marche"):
        out["status"] = "abstention"
        out["raison"] = "aucune sélection officielle structurée transmise par BSM"
        return out
    # Veto/sensibilité propagés exactement depuis BSM.
    if off.get("veto") or off.get("suspect") or off.get("instable"):
        out["status"] = "abstention"
        out["raison"] = "sélection officielle sous veto/suspecte/instable (BSM)"
        return out
    # EV RECALCULÉE depuis p & cote de la sélection officielle (on n'utilise jamais une EV fournie telle
    # quelle). Sans p & cote, pas de sélection.
    p, cote = C.as_float(off.get("p")), C.as_float(off.get("cote"))
    if p is None or cote is None:
        out["status"] = "abstention"
        out["raison"] = "p/cote manquants sur la sélection officielle"
        return out
    ev = p * cote - 1.0
    if ev < min_ev:
        out["status"] = "abstention"
        out["raison"] = f"EV recalculée {ev * 100:.2f} % < seuil {min_ev * 100:.0f} %"
        return out
    out["status"] = "selection"
    out["official"] = {"marche": off["marche"], "p": p, "cote": cote, "ev": round(ev, 4),
                       "full_settle": _is_full_settle(off["marche"]),
                       "sensibilite": off.get("sensibilite")}
    return out


def _ledger_ids() -> set:
    path = LEDGER_DIR / "forecasts.jsonl"
    if not path.exists():
        return set()
    out = set()
    for line in open(path, encoding="utf-8"):
        try:
            out.add(json.loads(line)["forecast_id"])
        except (json.JSONDecodeError, KeyError):
            continue
    return out


def _ledger_new_forecast(before: set, home: str, away: str, kickoff: str):
    path = LEDGER_DIR / "forecasts.jsonl"
    if not path.exists():
        return None
    best = None
    for line in open(path, encoding="utf-8"):
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("forecast_id") in before:
            continue
        if r.get("home") == home and r.get("away") == away:
            best = r   # le plus récent gagne
    return best


# ───────────────────────── Risk Manager (plan §6) ─────────────────────────

def kelly_stake_pct(p, odds, fraction: float, cap: float) -> float:
    """Kelly fractionné, borné par le plafond de mise. 0 si pas de bord."""
    p = C.as_float(p)
    odds = C.as_float(odds)
    if p is None or odds is None:
        return 0.0
    b = odds - 1.0
    if b <= 0 or not (0.0 < p < 1.0):
        return 0.0
    f = (p * b - (1.0 - p)) / b
    if f <= 0:
        return 0.0
    return min(cap, fraction * f)


def risk_decision(sel: dict, cfg: dict, exposure: dict | None = None,
                  pnl_day: float = 0.0, pnl_week: float = 0.0) -> dict:
    """Décision de mise d'un pari candidat. `sel` = {match_id, league, market, p, odds}.
    `exposure` = {matchs:{id:pct}, ligues:{l:pct}, marches:{m:pct}} déjà engagé.
    Renvoie {approved, stake_pct, stake_amount, reasons, caps_applied}."""
    exposure = exposure or {"matchs": {}, "ligues": {}, "marches": {}}
    reasons, caps = [], []
    bankroll = cfg["bankroll"]

    # stop-loss (plan §6)
    if pnl_day <= cfg["stop_loss_daily"] * bankroll:
        reasons.append(f"stop-loss quotidien atteint ({pnl_day:+.0f} ≤ {cfg['stop_loss_daily'] * bankroll:+.0f})")
    if pnl_week <= cfg["stop_loss_weekly"] * bankroll:
        reasons.append(f"stop-loss hebdomadaire atteint ({pnl_week:+.0f} ≤ {cfg['stop_loss_weekly'] * bankroll:+.0f})")

    # corrélation : au plus un pari par match (plan §6 + CLAUDE.md §4)
    mid = sel.get("match_id")
    if mid and exposure["matchs"].get(mid, 0.0) > 0:
        reasons.append("corrélation : un pari est déjà engagé sur ce match")

    # EV minimale (CLAUDE.md §4) : TOUJOURS recalculée depuis p & cote (on n'accepte jamais une EV
    # fournie telle quelle — audit 2026-10-05 : ev=0.10 fourni contournait le seuil). Sans p & cote,
    # pas de bord démontrable → refus.
    p_ = C.as_float(sel.get("p"))
    o_ = C.as_float(sel.get("odds"))
    if p_ is None or o_ is None:
        reasons.append("p/cote manquants — EV non recalculable")
    else:
        ev = p_ * o_ - 1.0
        if ev < cfg["min_ev"]:
            reasons.append(f"EV {ev * 100:.2f} % < seuil {cfg['min_ev'] * 100:.0f} %")

    # Type de règlement : le Kelly binaire (p, cote) suppose un règlement PLEIN sur ligne .5. Le type
    # est TOUJOURS recalculé depuis le marché — un `full_settle` fourni n'est jamais cru (audit
    # 2026-10-05 : full_settle=True sur « Over 2.25 » contournait l'identification du contrat).
    full_settle = _is_full_settle(sel.get("market"))
    if not full_settle:
        reasons.append("règlement partiel/non identifié — incompatible avec le Kelly binaire")

    base_pct = kelly_stake_pct(sel.get("p"), sel.get("odds"), cfg["kelly_fraction"], cfg["max_stake_pct"])
    if base_pct <= 0:
        reasons.append("Kelly ≤ 0 (aucun bord exploitable)")

    # Un refus FERME (stop-loss, corrélation, EV sous seuil, règlement partiel, Kelly ≤ 0) doit
    # produire ROUGE en aval, pas ORANGE (audit 2026-10-05).
    hard_block = bool(reasons)

    # plafonds d'exposition résiduels
    stake_pct = base_pct
    rem_match = cfg["max_exposure_match"] - exposure["matchs"].get(mid, 0.0)
    rem_league = cfg["max_exposure_league"] - exposure["ligues"].get(sel.get("league"), 0.0)
    rem_market = cfg["max_exposure_market"] - exposure["marches"].get(sel.get("market"), 0.0)
    for label, rem in (("match", rem_match), ("ligue", rem_league), ("marché", rem_market)):
        if stake_pct > rem:
            stake_pct = max(0.0, rem)
            caps.append(f"plafond {label}")

    if not reasons and stake_pct < cfg["min_stake_pct"]:
        reasons.append(f"mise sous le plancher ({stake_pct * 100:.2f} % < {cfg['min_stake_pct'] * 100:.2f} %)")

    approved = not reasons and stake_pct >= cfg["min_stake_pct"]
    if not approved:
        stake_pct = 0.0
    return {"approved": approved, "stake_pct": round(stake_pct, 5),
            "stake_amount": round(stake_pct * bankroll, 2),
            "reasons": reasons, "caps_applied": caps, "hard_block": hard_block}


def add_exposure(exposure: dict, sel: dict, stake_pct: float) -> None:
    for scope, key in (("matchs", sel.get("match_id")), ("ligues", sel.get("league")),
                       ("marches", sel.get("market"))):
        if key is not None:
            exposure[scope][key] = exposure[scope].get(key, 0.0) + stake_pct


def traffic_light(eligible: bool, protocol: dict | None, risk: dict | None) -> str:
    if not eligible:
        return RED
    if protocol is None:
        return ORANGE                         # WORM seul : jamais VERT (modèle ne bat pas le marché)
    st = protocol.get("status")
    if st in ("abstention", "error"):
        return RED
    if st in ("wait", "unvalidated"):
        return ORANGE                         # modèle non validé → jamais VERT (audit 2026-10-05)
    if st == "selection":
        if risk and risk.get("approved"):
            return ORANGE if PROMOTION_FROZEN else GREEN   # gel : jamais de VERT tant que non levé
        if risk and risk.get("hard_block"):
            return RED                         # refus Risk FERME → ROUGE (audit 2026-10-05)
        return ORANGE                          # refus mou (plancher de mise) → à surveiller
    return ORANGE


# ───────────────────────── chargement config ─────────────────────────

def load_config(args) -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if getattr(args, "config", None):
        user = C.read_json(args.config) or {}
        cfg.update({k: v for k, v in user.items() if k in cfg})
    for key in ("bankroll", "min_priority", "queue_capacity", "kelly_fraction"):
        val = getattr(args, key, None)
        if val is not None:
            cfg[key] = val
    return cfg


def resolve_day(date_str: str | None):
    tz = W.apex_tz()
    now_local = dt.datetime.now(tz)
    if date_str:
        base = dt.datetime.combine(dt.date.fromisoformat(date_str), dt.time(12, 0), tzinfo=tz)
        day, _, _ = W.apex_window(base)
    else:
        day, _, _ = W.apex_window(now_local)
    return day


# ───────────────────────── commandes ─────────────────────────

def cmd_candidates(a):
    cfg = load_config(a)
    day = resolve_day(a.date)
    records = W.latest_by_fixture(day)
    if not records:
        print(f"Aucun snapshot WORM pour la journée APEX {day}.")
        print(f"Lancer d'abord :  python3 tools/apex_worm.py scan" + (f" --date {a.date}" if a.date else ""))
        return
    sel = select_candidates(records, cfg)
    CAND_DIR.mkdir(parents=True, exist_ok=True)
    out = {"apex_day": day.isoformat(), "generated_at_utc": C.now_utc(), "config": cfg, **sel}
    C.write_json(str(CAND_DIR / f"{day.isoformat()}.json"), out)

    print(f"APEX-SYNC · candidats · journée APEX {day}")
    print(f"{sel['total']} matchs analysés · {len(sel['queued'])} en file "
          f"(capacité {cfg['queue_capacity']}) · {len(sel['overflow'])} en débordement · "
          f"{len(sel['rejected'])} rejetés")
    if sel["queued"]:
        print("\n# FILE PROTOCOL (triée par priorité)")
        print("| # | pri | match | ligue | KO (h) | signal WORM | marché WORM | DQ |")
        print("|---|-----|-------|-------|--------|-------------|-------------|----|")
        for i, c in enumerate(sel["queued"], 1):
            print(f"| {i} | {c['priority']:.2f} | {c['home']} – {c['away']} | {c['league']} | "
                  f"{c['hours_to_kickoff']} | {c['worm_signal'] or '—'} | {c['worm_market']} | {c['data_quality']} |")
    if sel["overflow"]:
        print(f"\nDébordement (au-delà de la capacité) : "
              + ", ".join(f"{c['home']}–{c['away']} ({c['priority']:.2f})" for c in sel["overflow"][:8]))
    print(f"\nÉcrit : {(CAND_DIR / f'{day.isoformat()}.json').relative_to(ROOT)}")


def cmd_sync(a):
    cfg = load_config(a)
    day = resolve_day(a.date)
    records = W.latest_by_fixture(day)
    if not records:
        print(f"Aucun snapshot WORM pour la journée APEX {day}.")
        print(f"Lancer d'abord :  python3 tools/apex_worm.py scan" + (f" --date {a.date}" if a.date else ""))
        return
    sel = select_candidates(records, cfg)
    rec_by_id = {r["fixture_id"]: r for r in records}

    exposure = {"matchs": {}, "ligues": {}, "marches": {}}
    pnl_day = a.pnl_day or 0.0
    pnl_week = a.pnl_week or 0.0
    decisions = []
    counts = {GREEN: 0, ORANGE: 0, RED: 0}

    for c in sel["queued"]:
        rec = rec_by_id.get(c["fixture_id"], {})
        cmd = build_protocol_command(rec, cfg)
        entry = {"match_id": c["match_id"], "fixture_id": c["fixture_id"],
                 "home": c["home"], "away": c["away"], "league": c["league"],
                 "kickoff": c["kickoff"], "priority": c["priority"], "tags": c["tags"],
                 "worm_market": c["worm_market"], "worm_tier": c["worm_tier"],
                 "protocol_command": " ".join(cmd["argv"]), "protocol": None, "risk": None}
        protocol = None
        risk = None
        if a.run_protocol and cmd["runnable"]:
            protocol = run_protocol(cmd["argv"], c["home"], c["away"], c["kickoff"] or "")
            entry["protocol"] = protocol
            if protocol.get("status") == "selection":
                off = protocol["official"]
                s = {"match_id": c["match_id"], "league": c["league"],
                     "market": off["marche"], "p": off["p"], "odds": off["cote"]}
                risk = risk_decision(s, cfg, exposure, pnl_day, pnl_week)
                entry["risk"] = risk
                # On n'engage l'exposition que pour une autorisation RÉELLE (feu VERT) — jamais sous
                # gel (sinon une mise non autorisée bloquerait d'autres candidats).
                if risk["approved"] and not PROMOTION_FROZEN:
                    add_exposure(exposure, s, risk["stake_pct"])
        elif a.run_protocol and not cmd["runnable"]:
            protocol = {"status": "wait", "reason": cmd["reason"]}
            entry["protocol"] = protocol

        light = traffic_light(c["eligible"], protocol, risk)
        entry["light"] = light
        counts[light] += 1
        decisions.append(entry)

    # journal des décisions SYNC (append-only, horodaté)
    DEC_DIR.mkdir(parents=True, exist_ok=True)
    dec_path = DEC_DIR / f"{day.isoformat()}.jsonl"
    run_at = C.now_utc()
    with open(dec_path, "a", encoding="utf-8") as fh:
        for e in decisions:
            fh.write(json.dumps({**e, "sync_run_utc": run_at}, ensure_ascii=False) + "\n")

    _print_sync_report(day, cfg, sel, decisions, counts, a.run_protocol)
    print(f"\nJournal : {dec_path.relative_to(ROOT)} (append-only)")


def _print_sync_report(day, cfg, sel, decisions, counts, ran):
    print(f"APEX-SYNC · orchestration · journée APEX {day}")
    print(f"{sel['total']} matchs · {len(sel['queued'])} candidats en file · "
          f"PROTOCOL {'exécuté' if ran else 'NON exécuté (dry-run : commandes émises)'}")
    print(f"Feux : {LIGHT_LABEL[GREEN]} {counts[GREEN]} · {LIGHT_LABEL[ORANGE]} {counts[ORANGE]} · "
          f"{LIGHT_LABEL[RED]} {counts[RED]}")
    if not decisions:
        print("Aucun candidat éligible.")
        return
    for e in decisions:
        print(f"\n{LIGHT_LABEL[e['light']]}  {e['home']} – {e['away']}  ({e['league']}, pri {e['priority']:.2f})")
        print(f"   WORM : {e['worm_market']} · signal {', '.join(e['tags']) or '—'} · palier {e['worm_tier']}")
        p = e.get("protocol")
        if p is None:
            print(f"   PROTOCOL : non exécuté → {e['protocol_command']}")
        elif p.get("status") == "selection":
            off = p["official"]
            print(f"   PROTOCOL : SÉLECTION {off['marche']} @ {off['cote']} · "
                  f"p={off['p']:.3f} · EV {off['ev']:+.1%} · {p.get('statut_modele', '')}")
            r = e.get("risk") or {}
            # Le rapport ne doit JAMAIS afficher « GO · mise » tant que le feu n'est pas VERT : sous
            # gel, un risk approved + feu ORANGE s'affiche GELÉ, pas GO (audit 2026-10-05).
            if e["light"] == GREEN and r.get("approved"):
                print(f"   RISK : GO · mise {r['stake_pct'] * 100:.2f} % = {r['stake_amount']:.2f} "
                      f"(bankroll {cfg['bankroll']:.0f})" + (f" · {', '.join(r['caps_applied'])}" if r.get("caps_applied") else ""))
            elif r.get("approved") and PROMOTION_FROZEN:
                print("   RISK : GELÉ — mise calculée mais NON autorisée (promotion suspendue, audit 2026-10-05)")
            else:
                print(f"   RISK : refus · {', '.join(r.get('reasons', [])) or '—'}")
        else:
            why = p.get("reason") or p.get("decision") or p.get("status")
            print(f"   PROTOCOL : {p.get('status', '?').upper()} · {why}")
    print("\nRappel : cotes à re-vérifier et horodater avant toute mise. Aucun outil ne garantit un gain ; "
          "APEX-SYNC optimise le processus, pas le résultat.")


def risk_light(validated: bool, risk: dict) -> str:
    """Feu d'une décision Risk autonome. Exige une validation POSITIVE du modèle ET applique le gel
    de promotion : aucun VERT tant que PROMOTION_FROZEN (audit 2026-10-05)."""
    if not validated:
        return RED                             # modèle non validé → jamais d'autorisation
    if not risk.get("approved"):
        return RED if risk.get("hard_block") else ORANGE
    return ORANGE if PROMOTION_FROZEN else GREEN


def cmd_risk(a):
    cfg = load_config(a)
    sel = {"match_id": a.match_id, "league": a.league, "market": a.market, "p": a.p, "odds": a.odds}
    r = risk_decision(sel, cfg, pnl_day=a.pnl_day or 0.0, pnl_week=a.pnl_week or 0.0)
    validated = _model_validated({"statut_modele": getattr(a, "statut_modele", None)})
    light = risk_light(validated, r)
    print(f"APEX-SYNC · Risk Manager")
    print(f"p={a.p} · cote={a.odds} · marché={a.market or '—'} · modèle={'VALIDÉ' if validated else 'NON VALIDÉ/inconnu'}"
          f" · bankroll={cfg['bankroll']:.0f} · Kelly ×{cfg['kelly_fraction']}")
    print(f"{LIGHT_LABEL[light]}")
    if light == GREEN:
        print(f"Mise : {r['stake_pct'] * 100:.2f} % = {r['stake_amount']:.2f}"
              + (f" · {', '.join(r['caps_applied'])}" if r["caps_applied"] else ""))
    elif light == ORANGE and validated and r["approved"]:
        print("Mise GELÉE (promotion suspendue — audit 2026-10-05) : aucune autorisation financière.")
    else:
        motifs = r["reasons"] if r["reasons"] else (["modèle non validé"] if not validated else ["—"])
        print("Refus : " + ", ".join(motifs))


def cmd_kpi(a):
    day = resolve_day(a.date)
    path = DEC_DIR / f"{day.isoformat()}.jsonl"
    if not path.exists():
        print(f"Aucune décision SYNC pour la journée APEX {day}.")
        return
    rows = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    latest = {r["match_id"]: r for r in rows}.values()   # dernier passage par match
    counts = {GREEN: 0, ORANGE: 0, RED: 0}
    n_queued = len(latest)
    n_protocol = n_selection = 0
    for r in latest:
        counts[r.get("light", RED)] = counts.get(r.get("light", RED), 0) + 1
        p = r.get("protocol")
        if p:
            n_protocol += 1
            if p.get("status") == "selection":
                n_selection += 1
    print(f"APEX-SYNC · KPI pipeline · journée APEX {day}")
    print(f"Candidats en file : {n_queued} · passages SYNC : {len(rows)}")
    print(f"PROTOCOL exécutés : {n_protocol} · sélections : {n_selection}")
    print(f"Feux (dernier passage/match) : {LIGHT_LABEL[GREEN]} {counts[GREEN]} · "
          f"{LIGHT_LABEL[ORANGE]} {counts[ORANGE]} · {LIGHT_LABEL[RED]} {counts[RED]}")
    if n_queued:
        rate = counts[GREEN] / n_queued
        print(f"Taux de GO : {rate:.0%} "
              + ("(seuils peut-être trop laxistes)" if rate > 0.5 else
                 "(seuils stricts)" if rate < 0.1 else "(équilibré)"))
    print("\nNote : la notation par résultat (ROI, Brier, CLV) passe par le journal du PROTOCOL — "
          "`python3 tools/apex_bsm.py settle` puis `audit`. CLV indisponible sans cote de clôture relevée.")


def cmd_window(a):
    W.cmd_window(a)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)

    def common(sub):
        sub.add_argument("--date", help="journée APEX AAAA-MM-JJ (défaut : courante)")
        sub.add_argument("--config", help="fichier JSON de surcharge des seuils / Risk Manager")
        sub.add_argument("--bankroll", type=float)
        sub.add_argument("--min-priority", type=float, dest="min_priority")
        sub.add_argument("--queue-capacity", type=int, dest="queue_capacity")
        sub.add_argument("--kelly-fraction", type=float, dest="kelly_fraction")

    c = sp.add_parser("candidates", help="lire le snapshot WORM, filtrer et prioriser")
    common(c)
    c.add_argument("--max", type=int, dest="queue_capacity", help="alias de --queue-capacity")

    s = sp.add_parser("sync", help="orchestration complète WORM → PROTOCOL → Risk → feu")
    common(s)
    s.add_argument("--run-protocol", action="store_true", dest="run_protocol",
                   help="exécute réellement bsm simulate (sinon dry-run : commandes émises)")
    s.add_argument("--pnl-day", type=float, dest="pnl_day", help="P/L réalisé du jour (stop-loss)")
    s.add_argument("--pnl-week", type=float, dest="pnl_week", help="P/L réalisé de la semaine (stop-loss)")

    r = sp.add_parser("risk", help="Risk Manager autonome sur une sélection")
    r.add_argument("--p", type=float, required=True, help="probabilité modèle de l'issue")
    r.add_argument("--odds", type=float, required=True, help="cote décimale proposée")
    r.add_argument("--match-id", dest="match_id")
    r.add_argument("--league")
    r.add_argument("--market")
    r.add_argument("--statut-modele", dest="statut_modele",
                   help="statut de validation du modèle (VALIDÉ requis pour une autorisation)")
    r.add_argument("--bankroll", type=float)
    r.add_argument("--config")
    r.add_argument("--kelly-fraction", type=float, dest="kelly_fraction")
    r.add_argument("--pnl-day", type=float, dest="pnl_day")
    r.add_argument("--pnl-week", type=float, dest="pnl_week")

    k = sp.add_parser("kpi", help="KPI du pipeline SYNC pour une journée")
    k.add_argument("--date")

    sp.add_parser("window", help="afficher la fenêtre APEX courante")

    a = p.parse_args()
    {"candidates": cmd_candidates, "sync": cmd_sync, "risk": cmd_risk,
     "kpi": cmd_kpi, "window": cmd_window}[a.cmd](a)


if __name__ == "__main__":
    main()
