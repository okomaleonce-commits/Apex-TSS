#!/usr/bin/env python3
"""APEX-FAVORITES — détecteur de « favoris raisonnables », SANS cote.

Cellule DESCRIPTIVE, miroir d'APEX-CHARACTER : elle lit la structure du modèle
(classement Poisson-λ WORM : model_prob_1x2, lambdas, data_quality, signal_stable,
flags upset/blowout) et classe, pour chaque match, le favori que le MODÈLE soutient
— indépendamment du marché (aucune cote lue, aucun EV calculé).

Ce qu'elle N'EST PAS :
  - Elle ne price pas, n'émet aucun pari (gel actif). « Favori raisonnable » ≠ « pari ».
  - Sans cote, pas de value/EV possible : la sortie est une liste SURVEILLER, point.
  - Elle alimente la brique DATA de la convergence 3-voix ; elle ne la remplace pas.

Calibration (selon les coupons 1xBet partagés) : un favori modèle à 82 % (Augsburg–Bayern,
Bayern ext.) a perdu ; d'où un seuil EXIGEANT. La variance n'est pas supprimable — un
FAVORI NET gagne « souvent », pas « toujours ».

Source honnête : la proba vient du classement Poisson WORM (λ structurels), JAMAIS
étiquetée BSM. Pour les ligues backtestées (E0/SP1/I1/D1/F1) elle est corroborée par BSM,
ce qui est signalé par `corrobore_bsm=true` — sans changer l'étiquette de source.
"""
import argparse, json, sys, os, glob
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNAP_DIR = os.path.join(ROOT, "data", "worm", "snapshots")

# Ligues à moteur APEX-ENGINE (pour l'étiquette de ligue)
LEAGUES = {
    342: "Armenia PL (ARM)", 144: "Jupiler Pro League (BEL)", 71: "Brasileirao (BRA)",
    119: "Superligaen (DEN)", 39: "Premier League (ENG)", 61: "Ligue 1 (FRA)",
    62: "Ligue 2 (FRA)", 78: "Bundesliga (GER)", 383: "Premier League (ISR)",
    135: "Serie A (ITA)", 88: "Eredivisie (NED)", 103: "Eliteserien (NOR)",
    94: "Liga Portugal (POR)", 283: "Superliga (ROU)", 235: "Premier League (RUS)",
    307: "Saudi Pro League (KSA)", 179: "Scottish Prem (SCO)", 140: "La Liga (ESP)",
    141: "Segunda (ESP)", 113: "Allsvenskan (SWE)", 203: "Super Lig (TUR)",
}
# Ligues couvertes par un backtest BSM (corroboration, pas changement de source)
BSM_DIV = {39: "E0", 140: "SP1", 135: "I1", 78: "D1", 61: "F1"}

LIVE = {"FT", "AET", "PEN", "ABANDONED", "POSTPONED", "CANC", "1H", "HT", "2H", "ET", "P", "LIVE"}

# --- Seuils calibrés sur les coupons partagés (exigeants : le favori à 82 % a perdu) ---
PROFILES = {
    "strict":  dict(net_p=0.75, net_m=0.40, dom_p=0.85, dom_m=0.60, dq_floor=65, upset_cap=55),
    "modere":  dict(net_p=0.70, net_m=0.35, dom_p=0.82, dom_m=0.55, dq_floor=55, upset_cap=60),
    "large":   dict(net_p=0.60, net_m=0.25, dom_p=0.78, dom_m=0.45, dq_floor=45, upset_cap=65),
}


def _p3(mp):
    if isinstance(mp, (list, tuple)) and len(mp) >= 3:
        return [mp[0], mp[1], mp[2]]
    if isinstance(mp, dict):
        return [mp.get("H") or mp.get("1"), mp.get("D") or mp.get("X"), mp.get("A") or mp.get("2")]
    return None


def load_snapshot(day):
    path = os.path.join(SNAP_DIR, f"{day}.jsonl")
    if not os.path.exists(path):
        return None
    seen = {}
    with open(path) as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            try:
                r = json.loads(ln)
            except Exception:
                continue
            if r.get("league_id") not in LEAGUES:
                continue
            fx = r.get("fixture_id") or (r.get("home"), r.get("away"))
            seen[(r.get("league_id"), fx)] = r  # garde le dernier passage
    return list(seen.values())


def evaluate(r, prof):
    """Renvoie la lecture favori d'un match, ou None si pas de proba modèle."""
    p = _p3(r.get("model_prob_1x2"))
    if not p or not all(isinstance(x, (int, float)) for x in p):
        return None
    pH, pX, pA = p
    # favori = meilleure des deux issues de victoire (le nul n'est jamais un « favori »)
    if pH >= pA:
        side, p_fav, p_second = "1 (domicile)", pH, max(pX, pA)
    else:
        side, p_fav, p_second = "2 (extérieur)", pA, max(pX, pH)
    marge = p_fav - p_second
    dq = r.get("data_quality")
    stab = bool(r.get("signal_stable"))
    upset = r.get("upset") or 0
    blow = r.get("blowout") or 0
    lid = r.get("league_id")

    # garde-fous (fiabilité / volatilité) → disqualifie le statut favori
    disq = []
    if dq is None or dq < prof["dq_floor"]:
        disq.append("données faibles")
    if not stab:
        disq.append("signal instable")
    if isinstance(upset, (int, float)) and upset >= prof["upset_cap"]:
        disq.append("profil volatil (upset)")

    if disq:
        tier = "ECARTE"
    elif p_fav >= prof["dom_p"] and marge >= prof["dom_m"]:
        tier = "DOMINANT"
    elif p_fav >= prof["net_p"] and marge >= prof["net_m"]:
        tier = "FAVORI_NET"
    else:
        tier = "PAS_FAVORI"

    return {
        "league_id": lid, "league": LEAGUES.get(lid, str(lid)),
        "home": r.get("home"), "away": r.get("away"), "kickoff": r.get("kickoff") or r.get("date") or "",
        "side": side, "p_fav": round(p_fav, 3), "marge": round(marge, 3),
        "dq": dq, "stable": stab, "upset": upset, "blowout": blow,
        "tier": tier, "disq": disq,
        "source": "λ WORM (classement structurel, indicatif)",
        "corrobore_bsm": lid in BSM_DIV,
    }


def cmd_scan(a):
    prof = PROFILES[a.profile]
    rows = load_snapshot(a.date)
    if rows is None:
        print(f"[APEX-FAVORITES] snapshot absent : {a.date}", file=sys.stderr)
        sys.exit(2)
    out = []
    for r in rows:
        if not a.include_live and (r.get("status") or "") in LIVE:
            continue
        ev = evaluate(r, prof)
        if ev:
            out.append(ev)
    keep = [e for e in out if e["tier"] in ("DOMINANT", "FAVORI_NET")]
    keep.sort(key=lambda e: (-{"DOMINANT": 2, "FAVORI_NET": 1}[e["tier"]], -e["p_fav"]))

    if a.json:
        print(json.dumps({"date": a.date, "profile": a.profile, "seuils": prof,
                          "favoris": keep, "total_matchs": len(out)}, ensure_ascii=False, indent=1))
        return

    print(f"APEX-FAVORITES — {a.date}  (profil {a.profile})")
    print("Détecteur DESCRIPTIF, sans cote : liste SURVEILLER. Gel actif, aucune mise, rien inventé.")
    print(f"Favoris retenus : {len(keep)} / {len(out)} matchs analysés\n")
    hdr = f"{'tier':11} {'p':>4} {'marge':>6} {'dq':>4} {'match':42} {'sens':13} ligue"
    print(hdr); print("-" * len(hdr))
    for e in keep:
        bsm = " +BSM" if e["corrobore_bsm"] else ""
        print(f"{e['tier']:11} {int(e['p_fav']*100):3d}% {int(e['marge']*100):5d}% "
              f"{str(e['dq']):>4} {(e['home']+' – '+e['away'])[:42]:42} {e['side']:13} {e['league']}{bsm}")
    print("\nλ WORM = structure (jamais étiqueté BSM). +BSM = ligue backtestée (corroboration).")
    print("Favori raisonnable ≠ pari : même un favori à ~82 % perd (ex. Augsburg–Bayern). SURVEILLER.")


def cmd_calibrate(a):
    """Score une liste de jambes (match + issue + résultat) contre le détecteur,
    par bande de probabilité — 'calibration selon les coupons'."""
    legs = json.load(open(a.coupon))
    import unicodedata, re
    def norm(s):
        s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
        return re.sub(r"[^a-z]", "", s)
    idx = {}
    for day in a.dates.split(","):
        for r in (load_snapshot(day) or []):
            p = _p3(r.get("model_prob_1x2"))
            if p and all(isinstance(x, (int, float)) for x in p):
                idx[(norm(r.get("home")), norm(r.get("away")))] = (p, r)
    bands = {">=85": [], "75-85": [], "65-75": [], "<65": []}
    for leg in legs:
        key = (norm(leg["home"]), norm(leg["away"]))
        hit = idx.get(key)
        if not hit:
            continue
        p, _ = hit
        p_fav = p[0] if leg["pick"] == "1" else p[2]
        b = ">=85" if p_fav >= .85 else "75-85" if p_fav >= .75 else "65-75" if p_fav >= .65 else "<65"
        if leg.get("won") is not None:
            bands[b].append(bool(leg["won"]))
    print("Calibration selon les coupons (jambes 1X2 réglées) :")
    for b, res in bands.items():
        if res:
            print(f"  p_fav {b:6} : {sum(res)}/{len(res)} gagnés")
        else:
            print(f"  p_fav {b:6} : (aucune jambe réglée)")
    print("Note : échantillon minuscule — anchor réel = backtest BSM. Même ≥85 % n'est pas sûr.")


def main():
    ap = argparse.ArgumentParser(description="APEX-FAVORITES — favoris raisonnables, sans cote (descriptif)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan", help="liste les favoris raisonnables d'une journée")
    s.add_argument("--date", required=True, dest="date")
    s.add_argument("--profile", default="strict", choices=list(PROFILES))
    s.add_argument("--include-live", action="store_true", dest="include_live")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_scan)
    c = sub.add_parser("calibrate", help="score des jambes de coupon contre le modèle")
    c.add_argument("--coupon", required=True)
    c.add_argument("--dates", default="2026-10-10,2026-10-11")
    c.set_defaults(func=cmd_calibrate)
    a = ap.parse_args()
    a.func(a)


if __name__ == "__main__":
    main()
