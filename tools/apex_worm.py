#!/usr/bin/env python3
"""APEX-WORM — scanner football « ver informationnel », indépendant des protocoles APEX précédents.

Boucle conceptuelle (spec §28) :
  DISCOVER → COLLECT → NORMALIZE → STORE → COMPARE → ANALYZE → RANK → REPORT

Ne cherche pas seulement des value bets : cherche des ANOMALIES exploitables (spec §45) et recommande
toujours un marché quand le match est assez documenté, sinon NO BET. Chaque donnée porte sa provenance
(OBSERVED / CALCULATED / INFERRED / UNCONFIRMED, spec §34) et son heure de relevé. Rien n'est inventé :
une famille de données absente est écrite comme absente, jamais estimée en douce.

Honnêteté sur les sources : la source structurée disponible dans cet environnement (API-Football) ne
fournit ni volume de mises ni % de parieurs publics. Les composantes Sharp/RLM qui exigent ces données
sont donc marquées UNAVAILABLE ; ce qui est calculable (dispersion inter-books, écart Pinnacle↔médiane,
trajectoire de la ligne entre nos propres relevés horodatés) l'est, et rien de plus.

Fenêtre APEX (spec §2) : une journée va de 08:00:00 à 07:59:59 le lendemain, dans le fuseau APEX_TIMEZONE
(jamais codé en dur ; défaut UTC).

Usage :
  python3 tools/apex_worm.py scan [--date AAAA-MM-JJ] [--leagues 39,140] [--max-calls N] [--max-fixtures N]
  python3 tools/apex_worm.py report [--date AAAA-MM-JJ]
  python3 tools/apex_worm.py window   # affiche la fenêtre APEX courante et sort
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import re
import statistics
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apex_apifootball as AF  # noqa: E402
try:
    import apex_excapper as XC  # noqa: E402  (volume d'argent public, données publiques excapper)
except Exception:  # pragma: no cover
    XC = None
try:
    import apex_arbworld as AW  # noqa: E402  (surebets, sous réserve d'API autorisée)
except Exception:  # pragma: no cover
    AW = None


def norm_name(s):
    import unicodedata
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    s = s.replace("manchester", "man").replace("united", "utd")
    return re.sub(r"[^a-z]", "", re.sub(r"\b(fc|afc|cf|sc|ac|as|ss|us|club|de|the|w|women|u\d+)\b", "", s))


def name_sim(a, b):
    A = {a[i:i + 3] for i in range(max(1, len(a) - 2))}
    B = {b[i:i + 3] for i in range(max(1, len(b) - 2))}
    return len(A & B) / max(1, len(A | B))

ROOT = Path(__file__).resolve().parent.parent
SNAP = ROOT / "data" / "worm" / "snapshots"
REP = ROOT / "reports" / "worm"

LIVE_STATUS = {"1H", "HT", "2H", "ET", "BT", "P", "LIVE", "INT"}
DONE_STATUS = {"FT", "AET", "PEN"}
DEAD_STATUS = {"PST", "CANC", "ABD", "AWD", "WO", "SUSP"}

# provenance (spec §34)
OBSERVED, CALCULATED, INFERRED, UNCONFIRMED = "OBSERVED", "CALCULATED", "INFERRED", "UNCONFIRMED"
UNAVAILABLE = "UNAVAILABLE"   # donnée absente de la source (jamais estimée en douce)


# ───────────────────────── fenêtre APEX (spec §2) ─────────────────────────

def apex_tz() -> ZoneInfo:
    return ZoneInfo(os.environ.get("APEX_TIMEZONE", "UTC"))


def apex_window(now_local: dt.datetime):
    """Renvoie (apex_day, start_local, end_local). La journée commence à 08:00 ; avant 08:00 on est
    encore dans la journée de la veille."""
    day = now_local.date() if now_local.hour >= 8 else now_local.date() - dt.timedelta(days=1)
    start = dt.datetime.combine(day, dt.time(8, 0, 0), tzinfo=now_local.tzinfo)
    end = start + dt.timedelta(days=1) - dt.timedelta(seconds=1)
    return day, start, end


def in_window(kickoff_local: dt.datetime, start, end) -> bool:
    return start <= kickoff_local <= end


# ───────────────────────── marché : primitives pures ─────────────────────────

def demargin(odds):
    """[cotes] → probabilités justes (marge retirée), ou None."""
    if not odds or not all(o and o > 1 for o in odds):
        return None
    inv = [1.0 / o for o in odds]
    s = sum(inv)
    return [round(x / s, 4) for x in inv]


def margin(odds):
    if not odds or not all(o and o > 1 for o in odds):
        return None
    return round(sum(1.0 / o for o in odds) - 1.0, 4)


def align_exchange(es, home, away):
    """Réaligne des dictionnaires {nom_équipe/draw: valeur} sur le vecteur [dom, nul, ext].
    Renvoie {fair:[h,d,a]|None, money:[h,d,a]|None, total_matched}. None si l'appariement échoue."""
    if not es:
        return None

    def to_vec(d):
        if not d:
            return None
        names = list(d)
        draw = next((k for k in names if "draw" in k.lower()), None)
        rest = [k for k in names if k != draw]
        if len(rest) < 2:
            return None
        hn = max(rest, key=lambda k: name_sim(norm_name(home), norm_name(k)))
        an = max(rest, key=lambda k: name_sim(norm_name(away), norm_name(k)))
        if hn == an:
            return None
        return [d[hn], d.get(draw, 0.0) if draw else 0.0, d[an]]

    return {"fair": to_vec(es.get("exchange_fair")), "money": to_vec(es.get("money_pct")),
            "total_matched": es.get("total_matched")}


def book_dispersion(odds_by_book, market="1X2"):
    """Écart-type de la proba juste (issue 1) entre bookmakers = (dés)accord du marché. Plus bas = consensus."""
    fair = []
    for bk, m in odds_by_book.items():
        if bk.startswith("_") or market not in m:
            continue
        d = demargin(m[market] if market == "1X2" else None)
        if d:
            fair.append(d[0])
    if len(fair) < 2:
        return None, len(fair)
    return round(statistics.pstdev(fair), 4), len(fair)


# ───────────────────────── modèle structurel léger (spec §19) ─────────────────────────
# CALCULATED : Poisson à partir des taux de buts marqués/encaissés issus du classement.
# INFERRED quand on complète par une hypothèse (avantage terrain générique). Jamais présenté comme certain.

HOME_ADV = 1.10  # avantage terrain générique, INFERRED (spec §19 : ne jamais prétendre à la certitude)


def poisson_1x2(lh, la, max_goals=15):
    """Probabilités 1X2 par Poisson indépendant (modèle structurel, pas de calage marché)."""
    ph = [math.exp(-lh) * lh ** k / math.factorial(k) for k in range(max_goals + 1)]
    pa = [math.exp(-la) * la ** k / math.factorial(k) for k in range(max_goals + 1)]
    h = d = a = 0.0
    for i, xi in enumerate(ph):
        for j, xj in enumerate(pa):
            p = xi * xj
            if i > j:
                h += p
            elif i == j:
                d += p
            else:
                a += p
    return [round(h, 4), round(d, 4), round(a, 4)]


def poisson_over(lh, la, line=2.5, max_goals=12):
    tot = lh + la
    under = 0.0
    thr = math.floor(line)
    for k in range(thr + 1):
        under += math.exp(-tot) * tot ** k / math.factorial(k)
    return round(1 - under, 4)


def team_rates(strength, tid, avg_gf):
    """(buts marqués/match, buts encaissés/match) d'une équipe depuis le classement, ou None."""
    s = strength.get(tid)
    if not s or not s.get("played"):
        return None
    return s["gf"] / s["played"], s["ga"] / s["played"]


def structural_lambdas(strength, home_id, away_id, avg_gf):
    """λ attendus (dom, ext) = force d'attaque × faiblesse défensive adverse, normalisées à avg_gf.
    Renvoie (lh, la) ou None si le classement ne couvre pas les deux équipes."""
    rh = team_rates(strength, home_id, avg_gf)
    ra = team_rates(strength, away_id, avg_gf)
    if not rh or not ra:
        return None
    gf_h, ga_h = rh
    gf_a, ga_a = ra
    half = avg_gf / 2 if avg_gf else 1.35
    atk_h, def_h = gf_h / half, ga_h / half
    atk_a, def_a = gf_a / half, ga_a / half
    # bornes : au-delà de ~4.5 buts attendus, c'est de l'extrapolation d'un écart de division non fiable.
    lh = min(4.5, max(0.2, half * atk_h * def_a * HOME_ADV))
    la = min(4.5, max(0.2, half * atk_a * def_h / HOME_ADV))
    return round(lh, 3), round(la, 3)


# ───────────────────────── moteurs d'anomalies (spec §13-18) ─────────────────────────

def clamp(x):
    return max(0, min(100, int(round(x))))


def fmt_score(sc):
    """{'home':1,'away':0} → '1–0' ; None → '—'."""
    if isinstance(sc, dict):
        return f"{sc.get('home', '?')}–{sc.get('away', '?')}"
    return "—" if sc is None else str(sc)


def sharp_signal(fair_now, fair_prev, hours_between, dispersion, pinnacle_vs_median, exchange=None):
    """SHARP (spec §13). Composantes calculables sans argent public : trajectoire de ligne entre nos
    relevés, consensus (dispersion), divergence Pinnacle↔médiane. Quand l'argent public est fourni
    (`exchange` = {total_matched, money:[h,d,a]|None, fair:[h,d,a]|None}, ex. volume excapper), on ajoute
    le VOLUME réel matché, et — si la répartition d'argent est connue — la confirmation et un vrai Reverse
    Line Movement. Sans ces données, volume/public/exchange restent UNAVAILABLE (spec §34)."""
    comp = {"volume": UNAVAILABLE, "public_pct": UNAVAILABLE, "exchange": UNAVAILABLE}
    score = 0.0
    if fair_now and fair_prev and hours_between and hours_between > 0:
        move = max(abs(a - b) for a, b in zip(fair_now, fair_prev))
        velocity = move / hours_between
        comp["line_move"] = {"valeur": round(move, 4), "provenance": CALCULATED}
        comp["velocity"] = {"valeur": round(velocity, 4), "provenance": CALCULATED}
        score += min(40, move * 400)          # 10 pts de proba ≈ 40
        score += min(20, velocity * 400)
    else:
        comp["line_move"] = {"valeur": None, "provenance": UNCONFIRMED}
    if dispersion is not None:
        comp["consensus"] = {"dispersion": dispersion, "provenance": CALCULATED}
        score += max(0, 20 - dispersion * 400)  # faible dispersion = consensus serré
    if pinnacle_vs_median is not None:
        comp["pinnacle_vs_median"] = {"valeur": round(pinnacle_vs_median, 4), "provenance": CALCULATED}
        score += min(20, abs(pinnacle_vs_median) * 200)

    if exchange:
        tm = exchange.get("total_matched")
        if tm is not None:
            comp["volume"] = {"total_matched": tm, "provenance": OBSERVED}
            score += min(15, math.log10(max(tm, 1)) * 4)   # liquidité (spec §13) : ~+12 pour 10^3, +24 plafonné
        ef = exchange.get("fair")
        if ef:
            comp["exchange"] = {"fair": ef, "provenance": OBSERVED}
            if fair_now and fair_prev:
                moved = [a - b for a, b in zip(fair_now, fair_prev)]
                j = max(range(3), key=lambda i: moved[i])   # issue vers laquelle la ligne se déplace
                if ef[j] >= fair_now[j]:                     # l'échange price cette issue au moins aussi haut
                    comp["exchange_confirmation"] = {"issue": j, "provenance": OBSERVED}
                    score += 15
        mv = exchange.get("money")
        if mv:
            comp["public_pct"] = {"repartition": [round(x, 4) for x in mv], "provenance": OBSERVED}
            if fair_now and fair_prev:
                pub = max(range(3), key=lambda i: mv[i])     # côté où va l'argent public
                if mv[pub] >= 0.55 and fair_now[pub] < fair_prev[pub] - 0.01:
                    comp["rlm"] = {"issue": pub, "public_money": round(mv[pub], 3), "provenance": CALCULATED,
                                   "note": "argent public majoritaire mais cote qui dérive → Reverse Line Movement"}
                    score += 15
    return clamp(score), comp


def blowout_engine(fair_1x2, strength, home_id, away_id):
    """BLOWOUT (spec §15) : supériorité multidimensionnelle. None si classement absent."""
    if not fair_1x2:
        return None, {"raison": "cotes 1X2 absentes"}
    sh, sa = strength.get(home_id), strength.get(away_id)
    if not sh or not sa or not sh.get("played") or not sa.get("played"):
        return None, {"raison": "classement/forme absents pour une des équipes", "provenance": UNCONFIRMED}
    fav_home = fair_1x2[0] >= fair_1x2[2]
    fav_prob = max(fair_1x2[0], fair_1x2[2])
    ppg_h, ppg_a = sh["points"] / sh["played"], sa["points"] / sa["played"]
    gd_h = (sh["gf"] - sh["ga"]) / sh["played"]
    gd_a = (sa["gf"] - sa["ga"]) / sa["played"]
    ppg_gap = (ppg_h - ppg_a) if fav_home else (ppg_a - ppg_h)
    gd_gap = (gd_h - gd_a) if fav_home else (gd_a - gd_h)
    comp = {"fav": "domicile" if fav_home else "extérieur",
            "market_fav_prob": {"valeur": round(fav_prob, 4), "provenance": CALCULATED},
            "ppg_gap": {"valeur": round(ppg_gap, 3), "provenance": OBSERVED},
            "gd_per_game_gap": {"valeur": round(gd_gap, 3), "provenance": OBSERVED},
            "home_edge": fav_home}
    score = (max(0, fav_prob - 0.5) * 120        # marché très favorable
             + max(0, ppg_gap) * 18              # écart de points par match
             + max(0, gd_gap) * 12               # écart de différence de buts
             + (6 if fav_home else 0))
    return clamp(score), comp


def upset_engine(fair_1x2, strength, home_id, away_id):
    """UPSET (spec §16) : outsider sous-évalué (petit écart structurel malgré une cote élevée)."""
    if not fair_1x2:
        return None, {"raison": "cotes 1X2 absentes"}
    sh, sa = strength.get(home_id), strength.get(away_id)
    if not sh or not sa or not sh.get("played") or not sa.get("played"):
        return None, {"raison": "classement absent", "provenance": UNCONFIRMED}
    dog_home = fair_1x2[0] < fair_1x2[2]
    dog_prob = min(fair_1x2[0], fair_1x2[2])
    ppg_h, ppg_a = sh["points"] / sh["played"], sa["points"] / sa["played"]
    ppg_gap = abs(ppg_h - ppg_a)
    comp = {"dog": "domicile" if dog_home else "extérieur",
            "market_dog_prob": {"valeur": round(dog_prob, 4), "provenance": CALCULATED},
            "ppg_gap_absolu": {"valeur": round(ppg_gap, 3), "provenance": OBSERVED},
            "dog_at_home": dog_home}
    # petit écart de niveau + cote généreuse + avantage terrain de l'outsider = résistance de prix
    score = (max(0, 0.9 - ppg_gap) * 40          # équipes proches au classement
             + max(0, 0.40 - dog_prob) * 120     # le marché price un vrai outsider
             + (12 if dog_home else 0))
    return clamp(score), comp


def convergence_engine(strength, home_id, away_id, market_over, avg_gf):
    """STATSCONVERGENCE (spec §17) : combien de familles indépendantes pointent vers Over/Under 2.5."""
    sh, sa = strength.get(home_id), strength.get(away_id)
    if not sh or not sa or not sh.get("played") or not sa.get("played"):
        return None, None, {"raison": "classement absent", "provenance": UNCONFIRMED}
    gf_h, ga_h = sh["gf"] / sh["played"], sh["ga"] / sh["played"]
    gf_a, ga_a = sa["gf"] / sa["played"], sa["ga"] / sa["played"]
    ref = (avg_gf / 2) if avg_gf else 1.35
    votes_over, votes_under, fams = 0, 0, []
    for name, val in (("dom_buts_marques", gf_h), ("ext_buts_encaisses", ga_a),
                      ("ext_buts_marques", gf_a), ("dom_buts_encaisses", ga_h)):
        if val > ref * 1.05:
            votes_over += 1
            fams.append({"famille": name, "sens": "over", "valeur": round(val, 2), "provenance": OBSERVED})
        elif val < ref * 0.95:
            votes_under += 1
            fams.append({"famille": name, "sens": "under", "valeur": round(val, 2), "provenance": OBSERVED})
    lh_lambda = (gf_h + ga_a) / 2
    la_lambda = (gf_a + ga_h) / 2
    p_over_struct = poisson_over(lh_lambda, la_lambda, 2.5)
    if p_over_struct > 0.55:
        votes_over += 1
        fams.append({"famille": "poisson_structurel", "sens": "over", "valeur": p_over_struct, "provenance": CALCULATED})
    elif p_over_struct < 0.45:
        votes_under += 1
        fams.append({"famille": "poisson_structurel", "sens": "under", "valeur": p_over_struct, "provenance": CALCULATED})
    if market_over is not None:
        if market_over > 0.55:
            votes_over += 1
            fams.append({"famille": "marche_over25", "sens": "over", "valeur": round(market_over, 3), "provenance": CALCULATED})
        elif market_over < 0.45:
            votes_under += 1
            fams.append({"famille": "marche_over25", "sens": "under", "valeur": round(market_over, 3), "provenance": CALCULATED})
    total_fams = 6 if market_over is not None else 5
    if votes_over >= votes_under:
        direction, votes = "Over 2.5", votes_over
    else:
        direction, votes = "Under 2.5", votes_under
    score = clamp(100 * votes / total_fams)
    return score, direction, {"familles": fams, "votes_over": votes_over, "votes_under": votes_under,
                              "p_over_structurel": p_over_struct}


# ───────────────────────── qualité & confiance (spec §24-25) ─────────────────────────

def data_quality(rec) -> int:
    q = 0
    _, o1 = AF.pick_book(rec.get("odds", {}), "1X2")
    nbook = len([k for k in rec.get("odds", {}) if not k.startswith("_")])
    if o1:
        q += 25 + min(15, nbook * 3)
    if rec.get("strength_ok"):
        q += 25
    if rec.get("compositions"):
        q += 15
    if rec.get("blessures") is not None:
        q += 10
    _, ou = AF.pick_book(rec.get("odds", {}), "OU")
    if ou and ou.get("2.5"):
        q += 10
    return clamp(q)


def confidence(rec, dispersion) -> int:
    c = 0.4 * rec["data_quality"]
    if dispersion is not None:
        c += max(0, 25 - dispersion * 500)
    else:
        c += 5
    sp = rec.get("min_played") or 0
    c += min(15, sp * 2)                       # taille d'échantillon (matchs joués)
    if rec.get("compositions"):
        c += 15                                # certitude compositions
    if rec.get("signal_stable"):
        c += 10
    return clamp(c)


# ───────────────────────── recommandation de marché (spec §22) ─────────────────────────

def recommend(rec):
    """Traduit l'anomalie la plus forte en PRIMARY MARKET, ou NO BET. Marque VALUE: NON CONFIRMÉE
    dès que l'edge n'est pas robuste (le modèle structurel ne bat pas le marché : c'est la règle, pas
    une exception). Ne jamais inventer un pari pour remplir une case (spec §22)."""
    scores = {"BLOWOUT": rec.get("blowout"), "UPSET": rec.get("upset"), "STATSCONVERGENCE": rec.get("convergence"),
              "SHARP": rec.get("sharp")}
    ranked = sorted(((k, v) for k, v in scores.items() if v is not None), key=lambda kv: kv[1], reverse=True)
    if not ranked or ranked[0][1] < 45 or rec["data_quality"] < 40:
        return {"primary_market": "NO BET", "raison": "aucune anomalie assez nette ou données insuffisantes",
                "value": "NON CONFIRMÉE",
                "decision": {"tier": "NO BET", "unites_indicatives": 0.0, "marche": "NO BET"}}
    tag, sc = ranked[0]
    _, o1 = AF.pick_book(rec.get("odds", {}), "1X2")
    fair = demargin(o1) if o1 else None
    fav_home = fair[0] >= fair[2] if fair else None
    if tag == "BLOWOUT":
        side = "domicile" if fav_home else "extérieur"
        market = f"Handicap asiatique -0.5/-1 {side} (ou Team Over 1.5 {side})"
    elif tag == "UPSET":
        dog_home = (fair[0] < fair[2]) if fair else True
        market = ("Double chance 1X / +0.5 AH domicile" if dog_home else "Double chance X2 / +0.5 AH extérieur")
    elif tag == "STATSCONVERGENCE":
        market = rec.get("convergence_dir", "Over 2.5")
    else:  # SHARP : suivre le sens du mouvement de ligne, jamais aveuglément (spec §14)
        market = "Aligné sur le mouvement de ligne (voir trajectoire) — confirmer avant mise"
    # VALUE : uniquement sur un marché RÉELLEMENT price (1X2). Les marchés d'anomalie (AH, DC, O/U) ne
    # sont pas price ici → value NON CONFIRMÉE, jamais déduite d'une EV 1X2 sans rapport (honnêteté).
    ev = rec.get("ev_best")            # meilleure EV 1X2, indicative
    ev_idx = rec.get("ev_best_idx")
    value = "NON CONFIRMÉE"
    out = {"primary_market": market, "signal_dominant": f"{tag} {sc}/100", "value": value,
           "ev_indicatif_1x2": ev, "tags": [k for k, v in ranked if v >= 45]}
    # Cas d'une vraie value 1X2 directe : le marché price affiche EV ≥ 3 % → on la remonte comme telle.
    if ev is not None and ev >= 0.03 and ev_idx is not None and o1:
        issue = ["1 (domicile)", "X (nul)", "2 (extérieur)"][ev_idx]
        out["value_1x2_directe"] = {"issue": issue, "cote": o1[ev_idx], "ev": ev, "value": "CONFIRMÉE",
                                    "note": "EV sur cote price ; le modèle structurel ne bat pas le marché en backtest — à confirmer en avant"}

    # DÉCISION de marché (spec §22, §44) : un signal n'est pas qu'à surveiller, il conduit à un choix.
    # Le palier module la conviction ET la mise indicative ; l'échange (confirmation, RLM, volume) et une
    # value 1X2 directe font monter d'un cran. Sizing volontairement prudent (unités de suivi, pas un
    # conseil de mise réelle : le modèle ne bat pas le marché, la cote reste à vérifier avant tout pari).
    dq = rec["data_quality"]
    exch_conf = bool(rec.get("exchange_confirmation")) or bool(rec.get("rlm"))
    strong_extra = exch_conf or bool(out.get("value_1x2_directe"))
    if sc >= 70 and dq >= 65 and strong_extra:
        tier, units = "JOUER", 1.0
    elif sc >= 55 and dq >= 50:
        tier, units = "JOUER_PETIT", 0.5
    else:
        tier, units = "SURVEILLER", 0.25
    out["decision"] = {"tier": tier, "unites_indicatives": units, "marche": market,
                       "signal": f"{tag} {sc}/100", "confirmation_echange": exch_conf,
                       "note": "cote à vérifier et horodater avant toute mise ; unités indicatives de suivi, non un conseil de value"}
    return out


# ───────────────────────── réseau : DISCOVER / COLLECT ─────────────────────────

def discover(tz_name, day, start, end, leagues):
    """Fixtures dont le coup d'envoi tombe dans la fenêtre APEX. 1-2 appels (dates locales couvertes)."""
    seen, fixtures, calls = set(), [], 0
    for d in (day, day + dt.timedelta(days=1)):
        b = AF.api("fixtures", date=d.isoformat(), timezone=tz_name)
        calls += 1
        for f in b["response"]:
            ko_local = dt.datetime.fromisoformat(f["fixture"]["date"])
            if not in_window(ko_local, start, end):
                continue
            if leagues and f["league"]["id"] not in leagues:
                continue
            if f["fixture"]["id"] in seen:
                continue
            seen.add(f["fixture"]["id"])
            fixtures.append(f)
    return fixtures, calls


def fetch_standings(league_id, season, cache):
    """Classement d'une ligue → {team_id: {points, played, gf, ga}} ; mis en cache par (ligue, saison)."""
    key = (league_id, season)
    if key in cache:
        return cache[key]
    table = {}
    try:
        b = AF.api("standings", league=league_id, season=season)
        for lg in b.get("response", []):
            for group in lg.get("league", {}).get("standings", []):
                for row in group:
                    allst = row.get("all", {})
                    goals = allst.get("goals", {})
                    table[row["team"]["id"]] = {
                        "points": row.get("points", 0), "played": allst.get("played", 0),
                        "gf": goals.get("for", 0), "ga": goals.get("against", 0), "rank": row.get("rank")}
    except AF.ApiError:
        table = {}
    cache[key] = table
    return table


def prev_snapshot_index(day):
    """Dernier état connu par fixture depuis le snapshot du jour APEX (pour COMPARE)."""
    path = SNAP / f"{day.isoformat()}.jsonl"
    idx = {}
    if path.exists():
        for line in open(path, encoding="utf-8"):
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            idx[r["fixture_id"]] = r
    return idx


# ───────────────────────── SCAN (boucle WORM complète) ─────────────────────────

def cmd_scan(a):
    tz = apex_tz()
    tz_name = os.environ.get("APEX_TIMEZONE", "UTC")
    now_local = dt.datetime.now(tz)
    if a.date:
        base = dt.datetime.combine(dt.date.fromisoformat(a.date), dt.time(12, 0), tzinfo=tz)
        day, start, end = apex_window(base)
    else:
        day, start, end = apex_window(now_local)
    leagues = {int(x) for x in a.leagues.split(",")} if a.leagues else None
    print(f"Fenêtre APEX {day} · {start.strftime('%d/%m %H:%M')} → {end.strftime('%d/%m %H:%M')} ({tz_name})")

    # Rafraîchissement de clôture de la veille : finalise a posteriori les matchs de bord de
    # fenêtre encore figés PREMATCH/LIVE, pour débloquer leur bilan de fin de journée.
    try:
        veille = day - dt.timedelta(days=1)
        u, c, _ = closeout_refresh(veille)
        if c:
            print(f"CLÔTURE VEILLE {veille} : {u}/{c} match(s) finalisé(s) a posteriori."
                  + ("" if u == c else " (reste des matchs non terminés)"))
    except Exception as e:  # noqa: BLE001
        print(f"Clôture veille ignorée ({str(e)[:80]}).")

    fixtures, calls = discover(tz_name, day, start, end, leagues)
    print(f"DISCOVER : {len(fixtures)} matchs dans la fenêtre ({calls} appels)"
          + (f" · ligues {sorted(leagues)}" if leagues else " · toutes compétitions"))
    if a.max_fixtures:
        fixtures = fixtures[:a.max_fixtures]

    # Argent public : volume matché excapper (Betfair MoneyWay, données PUBLIQUES) — un seul appel.
    money_index = {}
    if a.money and XC is not None:
        try:
            ms = XC.list_matches()
            for m in ms:
                if m.get("all_money_eur") and m.get("home") and m.get("away"):
                    key = (norm_name(m["home"]), norm_name(m["away"]))
                    money_index[key] = m
            print(f"ARGENT excapper : {len(ms)} matchs publics, {len(money_index)} avec volume "
                  f"(total {sum(m['all_money_eur'] for m in ms if m.get('all_money_eur')):,.0f} €)")
        except Exception as e:  # noqa: BLE001
            print(f"excapper indisponible ({str(e)[:100]}) → composante volume UNAVAILABLE.")
    if a.money and AW is not None and not AW.available():
        print("arbworld : API autorisée non configurée → composante arbitrage UNAVAILABLE (pas de scraping).")

    prev = prev_snapshot_index(day)
    SNAP.mkdir(parents=True, exist_ok=True)
    out_path = SNAP / f"{day.isoformat()}.jsonl"
    scan_time = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    strength_cache, results, changes, skipped = {}, [], [], 0

    for f in fixtures:
        if calls + 2 > a.max_calls:
            skipped = len(fixtures) - len(results)
            print(f"Budget d'appels atteint ({a.max_calls}) : {skipped} matchs non traités ce passage.")
            break
        fid = f["fixture"]["id"]
        status = f["fixture"]["status"]["short"]
        lg = f["league"]
        rec = {"fixture_id": fid, "scan_time_utc": scan_time, "kickoff": f["fixture"]["date"],
               "status": status, "phase": "LIVE" if status in LIVE_STATUS else ("DONE" if status in DONE_STATUS else
                                                                                ("DEAD" if status in DEAD_STATUS else "PREMATCH")),
               "league_id": lg["id"], "league": lg["name"], "country": lg.get("country"), "season": lg["season"],
               "home": f["teams"]["home"]["name"], "away": f["teams"]["away"]["name"],
               "home_id": f["teams"]["home"]["id"], "away_id": f["teams"]["away"]["id"],
               "score": f.get("goals")}

        # COLLECT — cotes (toujours), puis compositions/blessures si prématch et budget
        try:
            raw = AF.api_all("odds", fixture=fid); calls += 1
            rec["odds"] = AF.parse_odds(raw)
        except AF.ApiError as e:
            rec["odds"] = {}; rec["odds_erreur"] = str(e)[:120]
        if rec["phase"] == "PREMATCH" and calls + 1 <= a.max_calls and not a.odds_only:
            try:
                lu = AF.api("fixtures/lineups", fixture=fid)["response"]; calls += 1
                rec["compositions"] = [{"equipe": t["team"]["name"], "titulaires":
                                        [p["player"]["name"] for p in t.get("startXI", [])]} for t in lu] or None
            except AF.ApiError:
                rec["compositions"] = None

        # NORMALIZE + force des équipes (classement, 1 appel/ligue mis en cache)
        strength = {}
        if calls + 1 <= a.max_calls:
            before = len(strength_cache)
            strength = fetch_standings(lg["id"], lg["season"], strength_cache)
            if len(strength_cache) > before:
                calls += 1
        rec["strength_ok"] = bool(strength.get(rec["home_id"]) and strength.get(rec["away_id"]))
        avg_gf = 1.35 * 2
        if rec["strength_ok"]:
            sh, sa = strength[rec["home_id"]], strength[rec["away_id"]]
            rec["min_played"] = min(sh.get("played", 0), sa.get("played", 0))

        # marché
        _, o1 = AF.pick_book(rec["odds"], "1X2")
        fair = demargin(o1)
        rec["market_prob_1x2"] = fair
        rec["margin_1x2"] = margin(o1)
        disp, nbook = book_dispersion(rec["odds"], "1X2")
        rec["market_dispersion"] = disp
        _, ou = AF.pick_book(rec["odds"], "OU")
        market_over = None
        if ou and ou.get("2.5") and all(ou["2.5"]):
            d = demargin(ou["2.5"])
            market_over = d[0] if d else None
        rec["market_over25"] = market_over

        # modèle structurel + probas (spec §19)
        rec["model_prob_1x2"] = None
        if rec["strength_ok"]:
            lam = structural_lambdas(strength, rec["home_id"], rec["away_id"], avg_gf)
            if lam:
                rec["lambdas"] = lam
                rec["model_prob_1x2"] = poisson_1x2(*lam)
                rec["expected_score"] = {"xg_dom": lam[0], "xg_ext": lam[1]}
        # proba ajustée : ancrée sur le marché (le modèle ne bat pas le marché → poids modèle faible, honnête)
        adj = fair
        if fair and rec["model_prob_1x2"]:
            w = 0.15
            bl  = [ (1-w)*fair[i] + w*rec["model_prob_1x2"][i] for i in range(3) ]
            s = sum(bl); adj = [round(x/s, 4) for x in bl]
        rec["adjusted_prob_1x2"] = adj

        # COMPARE (mouvement de ligne vs relevé précédent du jour)
        p = prev.get(fid)
        fair_prev = p.get("market_prob_1x2") if p else None
        hours_between = None
        if p and p.get("scan_time_utc"):
            t0 = dt.datetime.fromisoformat(p["scan_time_utc"])
            t1 = dt.datetime.fromisoformat(scan_time)
            hours_between = max(0.0, (t1 - t0).total_seconds() / 3600)
        rec["signal_stable"] = bool(fair_prev and fair and max(abs(x - y) for x, y in zip(fair, fair_prev)) < 0.01)

        # pinnacle vs médiane
        pin_vs_med = None
        if "Pinnacle" in rec["odds"] and "1X2" in rec["odds"]["Pinnacle"]:
            dpin = demargin(rec["odds"]["Pinnacle"]["1X2"])
            if dpin and disp is not None and fair:
                pin_vs_med = dpin[0] - fair[0]

        # Argent public excapper apparié à ce match (volume matché réel). Appariement noms + tolérance.
        exch = None
        if money_index:
            k = (norm_name(rec["home"]), norm_name(rec["away"]))
            hit = money_index.get(k)
            if not hit:  # appariement approché : meilleure similarité combinée dom+ext
                best, bs = None, 0.0
                for (hn, an), m in money_index.items():
                    sc2 = (name_sim(k[0], hn) + name_sim(k[1], an)) / 2
                    if sc2 > bs:
                        best, bs = m, sc2
                if best and bs >= 0.6:
                    hit = best
            if hit:
                exch = {"total_matched": hit["all_money_eur"], "fair": None, "money": None,
                        "source": "excapper", "match": f"{hit['home']} - {hit['away']}"}
        rec["exchange"] = exch

        # ANALYZE — moteurs d'anomalies
        rec["sharp"], rec["sharp_components"] = sharp_signal(fair, fair_prev, hours_between, disp, pin_vs_med, exch)
        rec["exchange_confirmation"] = "exchange_confirmation" in rec["sharp_components"]
        rec["rlm"] = rec["sharp_components"].get("rlm")
        rec["blowout"], rec["blowout_components"] = blowout_engine(fair, strength, rec["home_id"], rec["away_id"])
        rec["upset"], rec["upset_components"] = upset_engine(fair, strength, rec["home_id"], rec["away_id"])
        conv = convergence_engine(strength, rec["home_id"], rec["away_id"], market_over, avg_gf)
        rec["convergence"], rec["convergence_dir"], rec["convergence_components"] = conv

        # divergence stats↔marché (spec §18)
        if rec["convergence"] and market_over is not None and rec.get("convergence_dir"):
            struct_over = rec["convergence_components"].get("p_over_structurel")
            if struct_over is not None:
                if (struct_over > 0.55 and market_over < 0.45) or (struct_over < 0.45 and market_over > 0.55):
                    rec["divergence_alert"] = {"stats": round(struct_over, 3), "marche": round(market_over, 3),
                                               "note": "contradiction stats↔marché : chercher la cause (météo, absence, échantillon)"}

        # EV 1X2 indicative (proba ajustée × cote), honnête : rarement ≥ 3 %
        ev_best = None
        if adj and o1:
            evs = [adj[i] * o1[i] - 1 for i in range(3)]
            rec["ev_best_idx"] = max(range(3), key=lambda i: evs[i])
            ev_best = round(evs[rec["ev_best_idx"]], 4)
        rec["ev_best"] = ev_best

        # qualité, confiance, recommandation
        rec["data_quality"] = data_quality(rec)
        rec["confidence"] = confidence(rec, disp)
        rec["reco"] = recommend(rec)

        # détection de changements (spec §27)
        for ch in detect_changes(p, rec):
            changes.append(ch)

        with open(out_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        results.append(rec)

    print(f"COLLECT/ANALYZE : {len(results)} matchs traités · ~{calls} appels API")
    if changes:
        print(f"CHANGEMENTS : {len(changes)}")
        for c in changes[:15]:
            print(f"  [{c['type']}] {c['match']} · {c['detail']}")

    print(f"STORE : {out_path.relative_to(ROOT)} (append-only)")
    write_report(day)
    if getattr(a, "email", False):
        subject, html = build_email_html(day)
        out_html = REP / f"{day.isoformat()}.email.html"
        out_html.write_text(html, encoding="utf-8")
        print(f"EMAIL HTML : {out_html.relative_to(ROOT)}")
        send_email_smtp(subject, html)


def detect_changes(prev, rec):
    out = []
    m = f"{rec['home']}–{rec['away']}"
    if prev is None:
        if (rec.get("reco") or {}).get("primary_market") not in (None, "NO BET"):
            out.append({"type": "NEW SIGNAL", "match": m, "detail": rec["reco"]["primary_market"]})
        return out
    # bascule de phase
    if prev.get("phase") != rec.get("phase"):
        out.append({"type": "PHASE", "match": m, "detail": f"{prev.get('phase')} → {rec['phase']}"})
    # mouvement de cote
    fp, fn = prev.get("market_prob_1x2"), rec.get("market_prob_1x2")
    if fp and fn:
        mv = max(abs(x - y) for x, y in zip(fn, fp))
        if mv >= 0.03:
            out.append({"type": "ODDS MOVE", "match": m, "detail": f"Δ proba max {mv*100:+.1f} pts"})
    # compositions
    if not prev.get("compositions") and rec.get("compositions"):
        out.append({"type": "LINEUP CHANGE", "match": m, "detail": "compositions publiées → recalcul"})
    # signaux
    for tag in ("sharp", "blowout", "upset", "convergence"):
        a0, a1 = prev.get(tag), rec.get(tag)
        if a0 is None or a1 is None:
            continue
        if a1 - a0 >= 15:
            out.append({"type": "SIGNAL STRENGTHENED", "match": m, "detail": f"{tag} {a0}→{a1}"})
        elif a0 - a1 >= 15:
            out.append({"type": "SIGNAL WEAKENED", "match": m, "detail": f"{tag} {a0}→{a1}"})
        if a0 >= 45 and a1 < 45:
            out.append({"type": "SIGNAL INVALIDATED", "match": m, "detail": f"{tag} sous le seuil ({a1})"})
    return out


# ───────────────────────── REPORT (spec §36-38) ─────────────────────────

def latest_by_fixture(day):
    path = SNAP / f"{day.isoformat()}.jsonl"
    latest = {}
    if not path.exists():
        return []
    for line in open(path, encoding="utf-8"):
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        latest[r["fixture_id"]] = r  # le dernier relevé écrase (on veut l'état courant)
    return list(latest.values())


def closeout_refresh(day, max_fixtures=25):
    """Rafraîchissement de clôture : récupère le statut/score FINAL des matchs de `day` encore
    figés en PREMATCH/LIVE dans le snapshot (matchs de bord de fenêtre APEX dont le passage
    horaire a basculé sur la journée suivante avant d'avoir capté leur fin). Réécrit une ligne
    append-only à jour par fixture résolu, en conservant reco/signaux/équipes du dernier relevé.
    N'invente rien : seuls les matchs réellement passés en DONE/DEAD côté API sont réécrits ;
    ceux encore en cours sont laissés tels quels. Retourne (n_mis_a_jour, n_verifies, n_appels)."""
    path = SNAP / f"{day.isoformat()}.jsonl"
    if not path.exists():
        return 0, 0, 0
    stale = [r for r in latest_by_fixture(day)
             if r.get("phase") in ("PREMATCH", "LIVE") and r.get("fixture_id")][:max_fixtures]
    if not stale:
        return 0, 0, 0
    scan_time = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    updated = calls = 0
    for r in stale:
        try:
            resp = AF.api("fixtures", id=r["fixture_id"]).get("response", []); calls += 1
        except AF.ApiError:
            continue
        if not resp:
            continue
        fx = resp[0]
        status = fx["fixture"]["status"]["short"]
        phase = ("LIVE" if status in LIVE_STATUS else "DONE" if status in DONE_STATUS
                 else "DEAD" if status in DEAD_STATUS else "PREMATCH")
        if phase in ("PREMATCH", "LIVE"):
            continue  # toujours pas terminé → ne rien réécrire
        nr = dict(r)
        nr.update({"status": status, "phase": phase, "score": fx.get("goals"),
                   "scan_time_utc": scan_time, "closeout": True})
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(nr, ensure_ascii=False) + "\n")
        updated += 1
    return updated, len(stale), calls


def relevance(r):
    """Priorité = force du signal × qualité des données × stabilité (spec §38), pas la cote."""
    best = max([x for x in (r.get("sharp"), r.get("blowout"), r.get("upset"), r.get("convergence")) if x is not None],
               default=0)
    stab = 1.0 if r.get("signal_stable") else 0.85
    return best * (r.get("data_quality", 0) / 100) * stab


def write_report(day):
    REP.mkdir(parents=True, exist_ok=True)
    rows = latest_by_fixture(day)
    rows.sort(key=relevance, reverse=True)
    L = [f"# APEX-WORM — journée {day} (fenêtre 08:00→07:59, {os.environ.get('APEX_TIMEZONE','UTC')})",
         f"Généré {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%MZ')} · {len(rows)} matchs scannés.",
         "",
         "*Signaux calculés uniquement à partir des données disponibles (API-Football). Volume de mises et "
         "% parieurs publics NON DISPONIBLES : composantes Sharp/RLM partielles. Le modèle structurel ne "
         "bat pas le marché — toute value non robuste est marquée NON CONFIRMÉE.*", ""]

    def top(tag, label):
        cand = sorted([r for r in rows if r.get(tag)], key=lambda r: r[tag], reverse=True)[:5]
        if not cand:
            return []
        out = [f"### {label}"]
        for r in cand:
            out.append(f"- **{r['home']}–{r['away']}** ({r['league']}) · {tag} {r[tag]}/100 · "
                       f"conf {r.get('confidence','?')} · DQ {r.get('data_quality','?')} · {r['reco']['primary_market']}")
        return out + [""]

    # DÉCISIONS DU JOUR : les signaux conduisent à un choix de marché (spec §22, §44)
    deci = [r for r in rows if (r.get("reco", {}).get("decision", {}) or {}).get("tier") in ("JOUER", "JOUER_PETIT")]
    deci.sort(key=lambda r: (r["reco"]["decision"]["tier"] != "JOUER", -relevance(r)))
    L += ["## DÉCISIONS DU JOUR", ""]
    if deci:
        L += ["| Palier | Match | Comp. | KO | Marché retenu | Signal | Éch. | Unités | Conf |",
              "|---|---|---|---|---|---|:--:|--:|--:|"]
        for r in deci:
            d = r["reco"]["decision"]
            L.append(f"| **{d['tier']}** | {r['home']}–{r['away']} | {(r.get('country') or '')[:3]} {r['league'][:12]} | "
                     f"{r['kickoff'][11:16]} | {d['marche'][:30]} | {d['signal']} | "
                     f"{'✓' if d.get('confirmation_echange') else '—'} | {d['unites_indicatives']} | {r.get('confidence','?')} |")
        L += ["", "*Unités indicatives de suivi, pas un conseil de mise : la cote est à vérifier et horodater "
              "avant tout pari, et le modèle structurel ne bat pas le marché.*", ""]
    else:
        L += ["*Aucune décision JOUER/JOUER_PETIT ce passage — le reste est à surveiller ou NO BET.*", ""]

    L += ["## TOP SIGNALS", ""]
    L += top("sharp", "TOP SHARP")
    L += top("blowout", "TOP BLOWOUT")
    L += top("upset", "TOP UPSET")
    L += top("convergence", "TOP STATSCONVERGENCE")
    live = [r for r in rows if r.get("phase") == "LIVE"]
    if live:
        L += ["### TOP LIVE", ""] + [f"- **{r['home']}–{r['away']}** {fmt_score(r.get('score'))} · {r['status']}" for r in live[:8]] + [""]

    L += ["## Tableau principal", "",
          "| Match | Comp. | KO | Marché | Prob(adj) | Sharp | Blow | Upset | Conv | Conf | DQ | Value |",
          "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---|"]
    for r in rows[:60]:
        adj = r.get("adjusted_prob_1x2")
        pa = f"{adj[0]:.2f}/{adj[1]:.2f}/{adj[2]:.2f}" if adj else "—"
        reco = r.get("reco", {})
        vd = reco.get("value_1x2_directe")
        value_cell = f"1X2 {vd['issue'][0]} @{vd['cote']} EV{vd['ev']:+.2f}" if vd else reco.get("value", "")
        L.append(f"| {r['home']}–{r['away']} | {(r.get('country') or '')[:3]} {r['league'][:14]} | "
                 f"{r['kickoff'][11:16]} | {reco.get('primary_market','?')[:26]} | {pa} | "
                 f"{r.get('sharp','–')} | {r.get('blowout','–')} | {r.get('upset','–')} | "
                 f"{r.get('convergence','–')} | {r.get('confidence','–')} | {r.get('data_quality','–')} | "
                 f"{value_cell} |")

    # fiches détaillées des meilleures anomalies (spec §37)
    L += ["", "## Fiches détaillées (top anomalies)", ""]
    for r in rows[:8]:
        if relevance(r) < 30:
            continue
        reco = r.get("reco", {})
        vd = reco.get("value_1x2_directe")
        L += [f"### {r['home']} – {r['away']}  ({r['country']} · {r['league']})",
              f"- Coup d'envoi : {r['kickoff']} · phase {r['phase']}",
              f"- PRIMARY MARKET : **{reco.get('primary_market')}** · VALUE marché recommandé : {reco.get('value')}"
              + (f" · EV 1X2 indicative {r['ev_best']:+.3f}" if r.get('ev_best') is not None else ""),]
        if vd:
            L.append(f"- Value 1X2 directe : **{vd['issue']} @ {vd['cote']}** · EV {vd['ev']:+.3f} (CONFIRMÉE sur cote price ; {vd['note']})")
        L += [
              f"- Probabilités 1X2 — marché {r.get('market_prob_1x2')} · modèle {r.get('model_prob_1x2')} · ajustée {r.get('adjusted_prob_1x2')}",
              f"- Scores — Sharp {r.get('sharp')} · Blowout {r.get('blowout')} · Upset {r.get('upset')} · Convergence {r.get('convergence')} ({r.get('convergence_dir')})",
              f"- Confiance {r.get('confidence')}/100 · Qualité données {r.get('data_quality')}/100"]
        if r.get("expected_score"):
            L.append(f"- Score attendu (xG structurel) : {r['expected_score']['xg_dom']} – {r['expected_score']['xg_ext']}")
        if r.get("divergence_alert"):
            L.append(f"- ⚠ DIVERGENCE : {r['divergence_alert']['note']}")
        if r.get("compositions"):
            L.append("- Compositions publiées : oui (recalcul possible)")
        L.append(f"- Dernier relevé : {r.get('scan_time_utc')}")
        L.append("")
    path = REP / f"{day.isoformat()}.md"
    path.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"REPORT : {path.relative_to(ROOT)}")


# ───────────────────────── recoupement APEX-PROTOCOL (ledger) ─────────────────────────

def ledger_index():
    """Prévisions APEX-PROTOCOL (journal apex_bsm) indexées par (dom, ext) normalisés — la plus récente.
    Lecture seule ; le ledger est append-only et n'est pas modifié ici."""
    path = ROOT / "ledger" / "forecasts.jsonl"
    idx = {}
    if not path.exists():
        return idx
    for line in open(path, encoding="utf-8"):
        try:
            fc = json.loads(line)
        except json.JSONDecodeError:
            continue
        if fc.get("home") and fc.get("away"):
            idx[(norm_name(fc["home"]), norm_name(fc["away"]))] = fc
    return idx


def apex_align(reco_market: str, fc: dict):
    """Le marché recommandé par WORM s'aligne-t-il avec la prévision APEX-PROTOCOL du ledger ?
    Renvoie (aligne: bool|None, resume: str). None si non comparable."""
    if not fc or not reco_market:
        return None, "—"
    m = fc.get("marches") or {}
    p1, px, p2 = m.get("1"), m.get("X"), m.get("2")
    over = m.get("Over2.5")
    deci = fc.get("decision", "")
    rm = reco_market.lower()
    aligned = None
    if p1 is not None and p2 is not None:
        if "domicile" in rm or "1x" in rm:
            aligned = p1 >= p2
        elif "extérieur" in rm or "exterieur" in rm or "x2" in rm:
            aligned = p2 >= p1
    if over is not None and ("over" in rm or "under" in rm):
        aligned = (over > 0.5) if "over" in rm else (over < 0.5)
    parts = []
    if p1 is not None:
        parts.append(f"1X2 {p1:.2f}/{px:.2f}/{p2:.2f}")
    if over is not None:
        parts.append(f"O2.5 {over:.2f}")
    resume = " · ".join(parts) + (f" · {deci[:24]}" if deci else "")
    return aligned, resume


# ───────────────────────── BILAN de fin de journée (post-match, spec §39-43) ─────────────────────────

def grade_market(market: str, hg, ag):
    """Note un marché recommandé contre le score final. → 'gagné' / 'demi-gagné' / 'push' / 'perdu' /
    'non-gradé'. Pur, testable. hg/ag = buts domicile/extérieur."""
    if market is None or hg is None or ag is None:
        return "non-gradé"
    m = market.lower()
    total = hg + ag
    diff = hg - ag
    if "over 2.5" in m:
        return "gagné" if total >= 3 else "perdu"
    if "under 2.5" in m:
        return "gagné" if total <= 2 else "perdu"
    if "handicap asiatique -0.5/-1 domicile" in m:
        return "gagné" if diff >= 2 else ("demi-gagné" if diff == 1 else "perdu")
    if "handicap asiatique -0.5/-1 extérieur" in m or "handicap asiatique -0.5/-1 exterieur" in m:
        return "gagné" if -diff >= 2 else ("demi-gagné" if -diff == 1 else "perdu")
    if "double chance 1x" in m or "+0.5 ah domicile" in m:
        return "gagné" if diff >= 0 else "perdu"
    if "double chance x2" in m or "+0.5 ah extérieur" in m or "+0.5 ah exterieur" in m:
        return "gagné" if diff <= 0 else "perdu"
    if "team over 1.5 domicile" in m:
        return "gagné" if hg >= 2 else "perdu"
    if "team over 1.5 extérieur" in m or "team over 1.5 exterieur" in m:
        return "gagné" if ag >= 2 else "perdu"
    return "non-gradé"   # ex. « aligné sur le mouvement de ligne » : pas de marché ferme


def _hit_rate(rows):
    """(gagné + 0.5·demi) / (gradés hors push). Renvoie (taux|None, n_gradés)."""
    g = sum(1 for r in rows if r["result"] == "gagné")
    d = sum(1 for r in rows if r["result"] == "demi-gagné")
    p = sum(1 for r in rows if r["result"] == "push")
    n = sum(1 for r in rows if r["result"] in ("gagné", "demi-gagné", "perdu"))  # push exclu
    if n == 0:
        return None, 0
    return round((g + 0.5 * d) / n, 3), n


def compute_bilan(day):
    """Note chaque décision de la journée contre le résultat final. Renvoie un dict complet
    (décisions gradées + agrégats par palier/signal/marché/confiance) pour l'email ET la mémoire de données."""
    rows = latest_by_fixture(day)
    graded = []
    for r in rows:
        d = (r.get("reco", {}) or {}).get("decision", {}) or {}
        if d.get("tier") not in ("JOUER", "JOUER_PETIT"):
            continue
        sc = r.get("score") or {}
        hg, ag = (sc.get("home"), sc.get("away")) if isinstance(sc, dict) else (None, None)
        phase = r.get("phase")
        done = phase == "DONE" and hg is not None and ag is not None
        if done:
            res = grade_market(d.get("marche"), hg, ag)
        elif phase == "DEAD":
            # match reporté/annulé/abandonné (PST/CANC/ABD/AWD/WO/SUSP) : terminé mais NON GRADABLE.
            # Ne bloque pas la complétude de la journée (n_non_terminees), comme la gate CLI --only-if-complete.
            res = "non-gradé"
        else:
            res = "non-terminé"
        graded.append({"match": f"{r['home']} – {r['away']}", "league": r.get("league"),
                       "tier": d["tier"], "signal": d.get("signal", ""),
                       "marche": d.get("marche"), "confidence": r.get("confidence"),
                       "data_quality": r.get("data_quality"),
                       "score": f"{hg}-{ag}" if done else None, "result": res})
    termines = [g for g in graded if g["result"] not in ("non-terminé", "non-gradé")]

    def agg(keyfn):
        buckets = {}
        for g in termines:
            buckets.setdefault(keyfn(g), []).append(g)
        return {k: {"taux": _hit_rate(v)[0], "n": _hit_rate(v)[1],
                    "gagné": sum(1 for x in v if x["result"] == "gagné"),
                    "demi": sum(1 for x in v if x["result"] == "demi-gagné"),
                    "perdu": sum(1 for x in v if x["result"] == "perdu")}
                for k, v in sorted(buckets.items())}

    def conf_bucket(g):
        c = g.get("confidence") or 0
        return "conf<50" if c < 50 else ("conf50-69" if c < 70 else "conf>=70")

    taux_global, n_global = _hit_rate(termines)
    return {
        "jour": str(day), "genere_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "n_decisions": len(graded), "n_terminees_gradees": len(termines),
        "n_non_terminees": sum(1 for g in graded if g["result"] == "non-terminé"),
        "n_non_gradees": sum(1 for g in graded if g["result"] == "non-gradé"),
        "taux_global": taux_global,
        "par_palier": agg(lambda g: g["tier"]),
        "par_signal": agg(lambda g: (g["signal"].split()[0] if g["signal"] else "?")),
        "par_marche": agg(lambda g: g["marche"].split(" (")[0][:26] if g["marche"] else "?"),
        "par_confiance": agg(conf_bucket),
        "decisions": graded,
        "note": ("Taux = (gagné + 0.5·demi) / gradés hors push. Résultats de marché, PAS un ROI "
                 "(les cotes exactes ne sont pas verrouillées ici). Échantillon d'une journée : indicatif, "
                 "à cumuler sur plusieurs jours avant toute recalibration."),
    }


def bilan_conclusions(b) -> list:
    """Phrases de conclusion factuelles, sans surinterprétation."""
    out = []
    tg = b["taux_global"]
    out.append(f"Journée {b['jour']} : {b['n_terminees_gradees']} décisions terminées et gradées"
               + (f", taux de réussite pondéré {tg:.0%}." if tg is not None else "."))
    for sig, s in sorted(b["par_signal"].items(), key=lambda kv: (kv[1]["taux"] is None, -(kv[1]["taux"] or 0))):
        if s["n"] >= 3 and s["taux"] is not None:
            out.append(f"Signal {sig} : {s['taux']:.0%} sur {s['n']} ({s['gagné']}G/{s['demi']}½/{s['perdu']}P).")
    best_m = [(k, v) for k, v in b["par_marche"].items() if v["n"] >= 3 and v["taux"] is not None]
    if best_m:
        best = max(best_m, key=lambda kv: kv[1]["taux"]); worst = min(best_m, key=lambda kv: kv[1]["taux"])
        out.append(f"Marché le plus fiable : {best[0]} ({best[1]['taux']:.0%}/{best[1]['n']}). "
                   f"Le moins fiable : {worst[0]} ({worst[1]['taux']:.0%}/{worst[1]['n']}).")
    out.append("Rappel : échantillon d'une seule journée — indicatif, pas une preuve. À cumuler pour recalibrer.")
    return out


def save_bilan(b):
    """Écrit le bilan en mémoire de données pour les recalibrations futures (append-only par jour)."""
    d = ROOT / "data" / "worm" / "bilans"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{b['jour']}.json").write_text(json.dumps(b, ensure_ascii=False, indent=1), encoding="utf-8")
    # ligne agrégée cumulable pour la recalibration
    with open(d / "history.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"jour": b["jour"], "genere_utc": b["genere_utc"],
                             "n": b["n_terminees_gradees"], "taux_global": b["taux_global"],
                             "par_signal": {k: {"taux": v["taux"], "n": v["n"]} for k, v in b["par_signal"].items()},
                             "par_marche": {k: {"taux": v["taux"], "n": v["n"]} for k, v in b["par_marche"].items()}},
                            ensure_ascii=False) + "\n")
    return d / f"{b['jour']}.json"


def build_bilan_email(b) -> tuple:
    def esc(x):
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    tg = f"{b['taux_global']:.0%}" if b["taux_global"] is not None else "n/c"
    subject = f"APEX-WORM BILAN {b['jour']} · {b['n_terminees_gradees']} décisions gradées · réussite {tg}"
    css = ("body{font-family:-apple-system,Segoe UI,Arial,sans-serif;color:#1a1a2e;background:#f4f5f7;padding:16px}"
           ".card{background:#fff;border-radius:12px;padding:16px 18px;max-width:820px;margin:0 auto}"
           "table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:6px 8px;border-bottom:1px solid #eee;text-align:left}"
           "th{background:#fafafe;color:#555}.muted{color:#6b7280;font-size:12px}h1{font-size:19px}h2{font-size:15px;color:#3b3b58}"
           ".g{color:#137333;font-weight:700}.p{color:#b3261e;font-weight:700}.d{color:#8a6d00;font-weight:700}.r{text-align:right}")
    H = [f"<html><head><meta charset='utf-8'><style>{css}</style></head><body><div class='card'>",
         f"<h1>APEX-WORM — BILAN journée {b['jour']}</h1>",
         f"<div class='muted'>{b['n_decisions']} décisions · {b['n_terminees_gradees']} terminées et gradées · "
         f"{b['n_non_gradees']} non gradées (marché non ferme) · réussite pondérée <b>{tg}</b></div>",
         "<h2>Conclusions</h2><ul>"]
    H += [f"<li>{esc(c)}</li>" for c in bilan_conclusions(b)]
    H += ["</ul>"]

    def tbl(title, agg):
        rows = [f"<h2>{title}</h2><table><tr><th>Clé</th><th class='r'>Réussite</th><th class='r'>N</th>"
                "<th class='r'>G</th><th class='r'>½</th><th class='r'>P</th></tr>"]
        for k, v in sorted(agg.items(), key=lambda kv: (kv[1]["taux"] is None, -(kv[1]["taux"] or 0))):
            t = f"{v['taux']:.0%}" if v["taux"] is not None else "n/c"
            rows.append(f"<tr><td>{esc(k)}</td><td class='r'>{t}</td><td class='r'>{v['n']}</td>"
                        f"<td class='r g'>{v['gagné']}</td><td class='r d'>{v['demi']}</td><td class='r p'>{v['perdu']}</td></tr>")
        return "".join(rows) + "</table>"
    H += [tbl("Par signal", b["par_signal"]), tbl("Par marché", b["par_marche"]),
          tbl("Par palier", b["par_palier"]), tbl("Par confiance", b["par_confiance"])]

    H += ["<h2>Détail des décisions</h2><table><tr><th>Match</th><th>Palier</th><th>Marché</th>"
          "<th>Signal</th><th>Score</th><th>Résultat</th></tr>"]
    order = {"gagné": 0, "demi-gagné": 1, "push": 2, "perdu": 3, "non-gradé": 4, "non-terminé": 5}
    for g in sorted(b["decisions"], key=lambda x: order.get(x["result"], 9)):
        cls = {"gagné": "g", "demi-gagné": "d", "perdu": "p"}.get(g["result"], "muted")
        H.append(f"<tr><td>{esc(g['match'])}</td><td>{esc(g['tier'])}</td><td>{esc((g['marche'] or '')[:28])}</td>"
                 f"<td>{esc(g['signal'])}</td><td>{esc(g['score'] or '—')}</td>"
                 f"<td class='{cls}'>{esc(g['result'])}</td></tr>")
    H += ["</table>",
          f"<div class='muted'>{esc(b['note'])} Enregistré dans data/worm/bilans/{b['jour']}.json pour les recalibrations.</div>",
          "</div></body></html>"]
    return subject, "\n".join(H)


def cmd_bilan(a):
    tz = apex_tz()
    base = dt.datetime.combine(dt.date.fromisoformat(a.date), dt.time(12, 0), tzinfo=tz) if a.date else dt.datetime.now(tz)
    day, start, end = apex_window(base)
    sentinel = ROOT / "data" / "worm" / "bilans" / f"{day}.done"
    if a.only_if_complete:
        rows = latest_by_fixture(day)
        if not rows:
            print("BILAN_SKIP : aucun relevé pour cette journée.")
            return
        pending = [r for r in rows if r.get("phase") in ("PREMATCH", "LIVE")]
        if pending:
            print(f"BILAN_SKIP : journée non terminée ({len(pending)} matchs encore à jouer/en cours).")
            return
        if sentinel.exists():
            print("BILAN_DÉJÀ_FAIT : bilan de la journée déjà émis.")
            return
    b = compute_bilan(day)
    path = save_bilan(b)
    subject, html = build_bilan_email(b)
    out_html = REP / f"{day}.bilan.html"
    REP.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text(b["genere_utc"], encoding="utf-8")
    print("BILAN_READY")
    print(f"  {b['n_terminees_gradees']} décisions gradées · réussite pondérée "
          + (f"{b['taux_global']:.0%}" if b["taux_global"] is not None else "n/c"))
    print(f"  données : {path.relative_to(ROOT)} · email HTML : {out_html.relative_to(ROOT)}")
    if getattr(a, "email", False):
        send_email_smtp(subject, html)


# ───────────────────────── EMAIL (notification mise en forme, spec §36-38) ─────────────────────────

def build_email_html(day) -> tuple:
    """(sujet, html) — digest du dernier état du jour, priorisé sur les décisions puis les anomalies."""
    rows = latest_by_fixture(day)
    rows.sort(key=relevance, reverse=True)
    tz_name = os.environ.get("APEX_TIMEZONE", "UTC")
    # Email : on écarte les matchs déjà terminés (ou annulés) — seuls pré-match et live sont utiles à parier.
    active = [r for r in rows if r.get("phase") not in ("DONE", "DEAD")]
    deci = [r for r in active if (r.get("reco", {}).get("decision", {}) or {}).get("tier") in ("JOUER", "JOUER_PETIT")]
    deci.sort(key=lambda r: (r["reco"]["decision"]["tier"] != "JOUER", -relevance(r)))
    n_jouer = sum(1 for r in deci if r["reco"]["decision"]["tier"] == "JOUER")
    live = [r for r in rows if r.get("phase") == "LIVE"]
    now = dt.datetime.now(dt.timezone.utc)
    stamp = now.strftime("%Y-%m-%d %H:%MZ")
    subject = f"APEX-WORM {day} · {len(deci)} décisions ({n_jouer} JOUER) · {len(rows)} matchs"

    # matchs imminents : coup d'envoi dans 0 à 60 min (pré-match)
    def mins_to_ko(r):
        try:
            ko = dt.datetime.fromisoformat(r["kickoff"].replace("Z", "+00:00"))
            return (ko - now).total_seconds() / 60
        except (ValueError, KeyError, AttributeError):
            return None
    imminent = []
    for r in rows:
        mk = mins_to_ko(r)
        if r.get("phase") == "PREMATCH" and mk is not None and 0 <= mk <= 60:
            imminent.append((mk, r))
    imminent.sort(key=lambda x: x[0])

    def esc(x):
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    css = ("body{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;color:#1a1a2e;margin:0;"
           "background:#f4f5f7;padding:16px} .card{background:#fff;border-radius:12px;padding:16px 18px;"
           "max-width:820px;margin:0 auto 14px;box-shadow:0 1px 4px rgba(0,0,0,.08)} h1{font-size:19px;margin:0 0 4px}"
           "h2{font-size:15px;margin:18px 0 8px;color:#3b3b58} .muted{color:#6b7280;font-size:12px}"
           "table{border-collapse:collapse;width:100%;font-size:13px} th,td{padding:6px 8px;border-bottom:1px solid #eee;text-align:left}"
           "th{background:#fafafe;color:#555} .jouer{background:#e7f6ec;color:#137333;font-weight:700;border-radius:6px;padding:1px 7px}"
           ".petit{background:#fef7e0;color:#8a6d00;font-weight:700;border-radius:6px;padding:1px 7px}"
           ".r{text-align:right} .tag{color:#4338ca;font-size:12px} .warn{color:#8a6d00}")
    H = [f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>",
         "<div class='card'>",
         f"<h1>APEX-WORM — journée {day}</h1>",
         f"<div class='muted'>Fenêtre 08:00→07:59 ({tz_name}) · généré {stamp} · {len(rows)} matchs · "
         f"{len(deci)} décisions dont {n_jouer} JOUER · {len(live)} en direct</div>"]

    # Matchs imminents (coup d'envoi dans 0–60 min)
    H += ["<h2>Matchs imminents (coup d'envoi dans 0–60 min)</h2>"]
    if imminent:
        # priorité aux matchs porteurs d'une décision, puis par temps restant
        imminent.sort(key=lambda x: (0 if (x[1].get("reco", {}).get("decision", {}) or {}).get("tier")
                                     in ("JOUER", "JOUER_PETIT") else 1, x[0]))
        total_imm = len(imminent)
        H += [f"<div class='muted'>{total_imm} matchs débutent dans l'heure (liste complète, décisions en tête).</div>",
              "<table><tr><th>Dans</th><th>Match</th><th>Compét.</th><th>KO</th><th>Décision</th>"
              "<th>Marché</th><th class='r'>Conf</th></tr>"]
        for mk, r in imminent:
            d = (r.get("reco", {}).get("decision", {}) or {})
            tier = d.get("tier", "—")
            cls = "jouer" if tier == "JOUER" else ("petit" if tier == "JOUER_PETIT" else "")
            badge = f"<span class='{cls}'>{tier}</span>" if cls else esc(tier)
            H.append(f"<tr><td><b>{int(mk)} min</b></td><td><b>{esc(r['home'])}–{esc(r['away'])}</b></td>"
                     f"<td>{esc((r.get('country') or '')[:3])} {esc(r['league'][:16])}</td><td>{r['kickoff'][11:16]}</td>"
                     f"<td>{badge}</td><td>{esc((d.get('marche') or r.get('reco',{}).get('primary_market','?'))[:30])}</td>"
                     f"<td class='r'>{r.get('confidence','?')}</td></tr>")
        H += ["</table>"]
    else:
        H += ["<div class='muted'>Aucun match ne débute dans les 60 prochaines minutes.</div>"]

    # Caractère attendu (APEX-CHARACTER) depuis les λ structurels — odds-free, jamais inventé
    def _carac(r):
        lam = r.get("lambdas")
        if not lam or len(lam) != 2:
            return "—"
        try:
            import apex_character as _CH
            ep = _CH.expected_profile(lam[0], lam[1])
            return f"{_CH.PROFIL_LABEL[ep['profil_attendu']].split(' ', 1)[0]} {ep['profil_attendu']} {int(ep['p_top']*100)}%"
        except Exception:  # noqa: BLE001
            return "—"

    if deci:
        H += ["<h2>Décisions du jour</h2>",
              "<table><tr><th>Palier</th><th>Match</th><th>Compét.</th><th>KO</th><th>Marché retenu</th>"
              "<th>Signal</th><th>Caractère attendu</th><th>Éch.</th><th class='r'>Unités</th><th class='r'>Conf</th></tr>"]
        for r in deci:
            d = r["reco"]["decision"]
            cls = "jouer" if d["tier"] == "JOUER" else "petit"
            H.append(f"<tr><td><span class='{cls}'>{d['tier']}</span></td><td><b>{esc(r['home'])}–{esc(r['away'])}</b></td>"
                     f"<td>{esc((r.get('country') or '')[:3])} {esc(r['league'][:16])}</td><td>{r['kickoff'][11:16]}</td>"
                     f"<td>{esc(d['marche'][:34])}</td><td class='tag'>{esc(d['signal'])}</td>"
                     f"<td class='muted'>{esc(_carac(r))}</td>"
                     f"<td>{'✓' if d.get('confirmation_echange') else '—'}</td>"
                     f"<td class='r'>{d['unites_indicatives']}</td><td class='r'>{r.get('confidence','?')}</td></tr>")
        H += ["</table>",
              "<div class='muted warn'>Unités indicatives de suivi, pas un conseil de mise : cote à vérifier et "
              "horodater avant tout pari ; le modèle structurel ne bat pas le marché. « Caractère attendu » = "
              "schéma de match prédit par les λ structurels (APEX-CHARACTER, sans cote).</div>"]
    else:
        H += ["<h2>Décisions du jour</h2><div class='muted'>Aucune décision JOUER/JOUER_PETIT ce passage.</div>"]

    # Recoupement avec APEX-PROTOCOL (ledger apex_bsm)
    lidx = ledger_index()
    cross = []
    for r in deci:
        fc = lidx.get((norm_name(r["home"]), norm_name(r["away"])))
        if not fc:
            continue
        aligned, resume = apex_align((r.get("reco", {}).get("decision", {}) or {}).get("marche", ""), fc)
        cross.append((r, aligned, resume))
    H += ["<h2>Recoupement APEX-PROTOCOL (football)</h2>"]
    if cross:
        H += ["<table><tr><th>Match</th><th>Marché WORM</th><th>Alignement</th><th>Prévision APEX-PROTOCOL</th></tr>"]
        for r, aligned, resume in cross:
            flag = "✅ aligné" if aligned else ("❌ divergent" if aligned is False else "— n/c")
            H.append(f"<tr><td><b>{esc(r['home'])}–{esc(r['away'])}</b></td>"
                     f"<td>{esc((r.get('reco',{}).get('decision',{}) or {}).get('marche','?')[:30])}</td>"
                     f"<td>{flag}</td><td class='muted'>{esc(resume)}</td></tr>")
        H += ["</table>",
              "<div class='muted'>« Aligné » = le marché recommandé par WORM va dans le même sens que la "
              "prévision APEX-PROTOCOL enregistrée au journal. Les deux protocoles restent indépendants.</div>"]
    else:
        H += ["<div class='muted'>Aucune décision du jour n'a de prévision APEX-PROTOCOL correspondante "
              "au journal (ledger/forecasts.jsonl). Lance le protocole APEX sur ces matchs pour recouper.</div>"]

    if live:
        H += ["<h2>En direct</h2><table><tr><th>Match</th><th>Score</th><th>Statut</th></tr>"]
        for r in live[:10]:
            H.append(f"<tr><td>{esc(r['home'])}–{esc(r['away'])}</td><td>{esc(fmt_score(r.get('score')))}</td><td>{esc(r['status'])}</td></tr>")
        H += ["</table>"]

    H += ["<h2>Meilleures anomalies</h2><table><tr><th>Match</th><th>Sharp</th><th>Blow</th><th>Upset</th>"
          "<th>Conv</th><th>Marché</th><th>Value</th></tr>"]
    for r in active[:15]:
        reco = r.get("reco", {})
        vd = reco.get("value_1x2_directe")
        val = f"1X2 {vd['issue'][0]} EV{vd['ev']:+.2f}" if vd else reco.get("value", "")
        H.append(f"<tr><td>{esc(r['home'])}–{esc(r['away'])}</td><td>{r.get('sharp','–')}</td><td>{r.get('blowout','–')}</td>"
                 f"<td>{r.get('upset','–')}</td><td>{r.get('convergence','–')}</td>"
                 f"<td>{esc(reco.get('primary_market','?')[:30])}</td><td>{esc(val)}</td></tr>")
    H += ["</table>",
          "<div class='muted'>Volume d'argent : réel via excapper (Betfair MoneyWay, données publiques) quand "
          "--money est actif, sinon UNAVAILABLE (jamais estimé). Détail complet dans reports/worm/.</div>"]

    # ─── APEX-SYNC — file PROTOCOL (candidats priorisés par l'orchestrateur apex_sync) ───
    sync_rows, sync_overflow, sync_err = [], 0, None
    try:
        import apex_sync as _S
        _cfg = dict(_S.DEFAULT_CONFIG)
        _sel = _S.select_candidates(rows, _cfg, now)
        sync_rows = _sel.get("queued", [])
        sync_overflow = len(_sel.get("overflow", []))
    except Exception as e:  # noqa: BLE001 — le digest ne doit jamais casser si apex_sync évolue
        sync_err = str(e)[:120]
    H += ["<h2>APEX-SYNC — file PROTOCOL (candidats priorisés)</h2>",
          "<div class='muted'>Orchestrateur <code>tools/apex_sync.py</code> : les anomalies WORM filtrées "
          "(prématch, DQ ≥ 45, signal ≥ 45, cote 1X2 présente) et triées par priorité "
          "(0.4·WORM + 0.2·liquidité + 0.2·temps + 0.2·tags). Candidats à transmettre au PROTOCOL (BSM) "
          "pour validation — ce passage ne lance pas la simulation (file seule).</div>"]
    if sync_err:
        H += [f"<div class='muted warn'>APEX-SYNC indisponible ce passage : {esc(sync_err)}</div>"]
    elif sync_rows:
        H += ["<table><tr><th>#</th><th class='r'>Prio</th><th>Match</th><th>Compét.</th>"
              "<th class='r'>KO (h)</th><th>Signal WORM</th><th>Marché WORM</th><th class='r'>DQ</th></tr>"]
        for i, c in enumerate(sync_rows, 1):
            h2k = c.get("hours_to_kickoff")
            H.append(f"<tr><td>{i}</td><td class='r'>{c.get('priority','?')}</td>"
                     f"<td><b>{esc(c.get('home',''))}–{esc(c.get('away',''))}</b></td>"
                     f"<td>{esc((c.get('country') or '')[:3])} {esc((c.get('league') or '')[:16])}</td>"
                     f"<td class='r'>{h2k if h2k is not None else '—'}</td>"
                     f"<td class='tag'>{esc(c.get('worm_signal') or '—')}</td>"
                     f"<td>{esc((c.get('worm_market') or '—')[:34])}</td>"
                     f"<td class='r'>{c.get('data_quality','?')}</td></tr>")
        H += ["</table>"]
        if sync_overflow:
            H += [f"<div class='muted'>+ {sync_overflow} candidats au-delà de la capacité de file "
                  f"({_cfg['queue_capacity']}).</div>"]
    else:
        H += ["<div class='muted'>Aucun candidat éligible ce passage (anomalies trop faibles, "
              "hors fenêtre de lead, ou cote 1X2 absente).</div>"]

    # ─── APEX-SYNC — architecture & synchronisation (rapport technique du passage) ───
    def _prov_volume(r):
        v = (r.get("sharp_components") or {}).get("volume")
        return isinstance(v, dict) and v.get("provenance") == "OBSERVED"
    n_vol = sum(1 for r in rows if _prov_volume(r))
    tot_vol = sum((r["sharp_components"]["volume"].get("total_matched") or 0)
                  for r in rows if _prov_volume(r))
    n_prematch = sum(1 for r in rows if r.get("phase") == "PREMATCH")
    n_done = sum(1 for r in rows if r.get("phase") in ("DONE", "DEAD"))
    money_on = n_vol > 0
    arbworld_ok = bool(os.environ.get("ARBWORLD_API_URL") and os.environ.get("ARBWORLD_KEY"))
    footystats_ok = bool(os.environ.get("FOOTYSTATS_KEY"))
    gmail_mode = not bool(os.environ.get("WORM_SMTP_HOST"))

    def _ok(label, state, detail=""):
        color = {"OK": "#137333", "UNAVAILABLE": "#8a6d00", "OFF": "#b3261e"}.get(state, "#6b7280")
        dash = f" — {esc(detail)}" if detail else ""
        return (f"<tr><td>{esc(label)}</td><td style='color:{color};font-weight:700'>{state}</td>"
                f"<td class='muted'>{dash}</td></tr>")

    H += ["<h2>APEX-SYNC — architecture &amp; synchronisation</h2>",
          "<div class='muted'>Boucle WORM : DISCOVER → COLLECT → NORMALIZE → STORE → COMPARE → ANALYZE → RANK → REPORT "
          "(spec §28). État du pipeline à ce passage.</div>",
          "<table><tr><th>Connecteur / source</th><th>État</th><th>Détail</th></tr>",
          _ok("API-Football (fixtures, cotes, live)", "OK" if rows else "OFF",
              f"{len(rows)} matchs dans la fenêtre"),
          _ok("excapper (volume Betfair MoneyWay, public)", "OK" if money_on else "UNAVAILABLE",
              f"{n_vol} matchs appariés · {tot_vol:,.0f} € matchés".replace(",", " ") if money_on
              else "--money inactif ou aucun appariement"),
          _ok("arbworld (arbitrage)", "OK" if arbworld_ok else "UNAVAILABLE",
              "API autorisée configurée" if arbworld_ok else "pas d'API autorisée → aucun scraping (robots.txt)"),
          _ok("FootyStats (xG historique)", "OK" if footystats_ok else "UNAVAILABLE",
              "clé présente" if footystats_ok else "FOOTYSTATS_KEY absente"),
          _ok("Notification e-mail", "OK",
              "connecteur Gmail (session)" if gmail_mode else "SMTP (secrets CI)"),
          "</table>",
          "<table><tr><th>Moteur</th><th>Rôle</th></tr>"
          "<tr><td>Sharp</td><td class='muted'>trajectoire de ligne + dispersion + Pinnacle↔médiane + volume réel</td></tr>"
          "<tr><td>Blowout</td><td class='muted'>supériorité multidimensionnelle du favori</td></tr>"
          "<tr><td>Upset</td><td class='muted'>outsider sous-évalué</td></tr>"
          "<tr><td>StatsConvergence</td><td class='muted'>familles indépendantes Over/Under 2.5</td></tr>"
          "<tr><td>Divergence</td><td class='muted'>contradiction stats ↔ marché</td></tr>"
          "</table>",
          "<div class='muted'>"
          f"<b>Flux du passage :</b> {len(rows)} matchs ({n_prematch} pré-match · {len(live)} live · {n_done} terminés) · "
          f"{len(deci)} décisions ({n_jouer} JOUER) · {len(imminent)} imminents · "
          f"3 probabilités par match (MODEL / MARKET / ADJUSTED).<br>"
          f"<b>Provenance :</b> chaque valeur porte OBSERVED / CALCULATED / INFERRED / UNCONFIRMED / UNAVAILABLE — "
          f"aucune donnée inventée (spec §34).<br>"
          f"<b>Persistance :</b> snapshots append-only data/worm/snapshots/{day}.jsonl · "
          f"rapport reports/worm/{day}.md · ce digest reports/worm/{day}.email.html · "
          f"fuseau {tz_name} · orchestré par trigger horaire (jour 07h–23h GMT)."
          "</div>"]

    H += ["</div></body></html>"]
    return subject, "\n".join(H)


def send_email_smtp(subject, html) -> bool:
    """Envoie le digest par SMTP si configuré (secrets CI / variables d'env). Renvoie True si envoyé.
    Variables : WORM_SMTP_HOST, WORM_SMTP_PORT (587), WORM_SMTP_USER, WORM_SMTP_PASS, WORM_EMAIL_TO,
    WORM_EMAIL_FROM (défaut = WORM_SMTP_USER)."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    host = os.environ.get("WORM_SMTP_HOST")
    to = os.environ.get("WORM_EMAIL_TO")
    user = os.environ.get("WORM_SMTP_USER")
    pwd = os.environ.get("WORM_SMTP_PASS")
    if not (host and to and user and pwd):
        print("Email non envoyé : WORM_SMTP_* / WORM_EMAIL_TO non configurés (secrets).")
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = os.environ.get("WORM_EMAIL_FROM", user)
    msg["To"] = to
    msg.attach(MIMEText(html, "html", "utf-8"))
    port = int(os.environ.get("WORM_SMTP_PORT", "587"))
    try:
        with smtplib.SMTP(host, port, timeout=30) as srv:
            srv.starttls()
            srv.login(user, pwd)
            srv.sendmail(msg["From"], [x.strip() for x in to.split(",")], msg.as_string())
        print(f"Email envoyé à {to}.")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"Email non envoyé (SMTP) : {str(e)[:120]}")
        return False


def cmd_report(a):
    tz = apex_tz()
    base = dt.datetime.combine(dt.date.fromisoformat(a.date), dt.time(12, 0), tzinfo=tz) if a.date else dt.datetime.now(tz)
    day, _, _ = apex_window(base)
    write_report(day)
    if getattr(a, "email", False):
        subject, html = build_email_html(day)
        out = REP / f"{day.isoformat()}.email.html"
        out.write_text(html, encoding="utf-8")
        print(f"EMAIL HTML : {out.relative_to(ROOT)}")
        send_email_smtp(subject, html)


def cmd_closeout(a):
    tz = apex_tz()
    base = dt.datetime.combine(dt.date.fromisoformat(a.date), dt.time(12, 0), tzinfo=tz) if a.date else dt.datetime.now(tz)
    day, _, _ = apex_window(base)
    u, c, nc = closeout_refresh(day, max_fixtures=a.max_fixtures)
    print(f"CLÔTURE {day} : {u}/{c} match(s) finalisé(s) a posteriori · {nc} appel(s) API.")


def cmd_window(a):
    tz = apex_tz()
    day, start, end = apex_window(dt.datetime.now(tz))
    print(f"Journée APEX : {day}")
    print(f"Début  : {start.isoformat()}")
    print(f"Fin    : {end.isoformat()}")
    print(f"Fuseau : {os.environ.get('APEX_TIMEZONE','UTC')}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("scan")
    s.add_argument("--date"); s.add_argument("--leagues"); s.add_argument("--max-calls", type=int, default=90)
    s.add_argument("--max-fixtures", type=int); s.add_argument("--odds-only", action="store_true")
    s.add_argument("--money", action="store_true", help="brancher l'argent public excapper (volume matché) + arbworld si API autorisée")
    s.add_argument("--email", action="store_true", help="envoyer le digest par email (SMTP via secrets)")
    r = sp.add_parser("report"); r.add_argument("--date"); r.add_argument("--email", action="store_true")
    bi = sp.add_parser("bilan"); bi.add_argument("--date"); bi.add_argument("--email", action="store_true")
    bi.add_argument("--only-if-complete", action="store_true", help="ne produit le bilan que si tous les matchs du jour sont terminés (une seule fois)")
    co = sp.add_parser("closeout", help="finalise a posteriori les matchs d'une journée encore figés PREMATCH/LIVE (bord de fenêtre)")
    co.add_argument("--date"); co.add_argument("--max-fixtures", type=int, default=25)
    sp.add_parser("window")
    a = p.parse_args()
    try:
        {"scan": cmd_scan, "report": cmd_report, "bilan": cmd_bilan,
         "closeout": cmd_closeout, "window": cmd_window}[a.cmd](a)
    except AF.ApiError as e:
        sys.exit(f"Erreur API-Football : {e}")


if __name__ == "__main__":
    main()
