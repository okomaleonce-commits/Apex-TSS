#!/usr/bin/env python3
"""Connecteur Betfair Exchange (API OFFICIELLE) — volume matché et probabilité d'échange pour APEX-WORM.

L'échange Betfair donne la donnée que les bookmakers cachent : le VOLUME réellement matché par marché et
par sélection, et un prix d'échange (back/lay) qui est la probabilité la plus « vraie » du marché. C'est
ce qui permet un vrai signal Sharp et une vraie répartition d'argent (spec §6, §12-13).

ACCÈS : API officielle Betfair, avec les identifiants du compte de l'utilisateur, lus dans des variables
d'environnement — JAMAIS dans le code, les logs ou les commits (spec §33) :
  BETFAIR_APP_KEY        clé applicative Betfair (Application Key)
  BETFAIR_USERNAME       identifiant du compte
  BETFAIR_PASSWORD       mot de passe du compte
  (optionnel) BETFAIR_SESSION_TOKEN  jeton déjà obtenu, pour sauter le login

Aucun contournement d'authentification ni de CAPTCHA, aucun scraping : uniquement l'API documentée, avec
le compte de l'utilisateur. Si les identifiants sont absents, le connecteur le dit et APEX-WORM garde ses
composantes de volume marquées UNAVAILABLE (jamais estimées).

Discipline : le volume et le prix d'échange portent leur heure de relevé ; rien n'est inventé.

Usage :
  python3 tools/apex_betfair.py status
  python3 tools/apex_betfair.py volumes --date 2026-09-30   # marchés Match Odds football du jour + volumes
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGIN_URL = "https://identitysso.betfair.com/api/login"
BETTING_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
SOCCER = "1"  # eventTypeId football


class BFError(RuntimeError):
    pass


# ───────────────────────── auth ─────────────────────────

def _app_key() -> str:
    k = os.environ.get("BETFAIR_APP_KEY")
    if not k:
        raise BFError("BETFAIR_APP_KEY absente : configure les identifiants Betfair comme variables "
                      "d'environnement (App Key, username, password), puis relance.")
    return k


def login() -> str:
    """Jeton de session : soit fourni (BETFAIR_SESSION_TOKEN), soit obtenu par login interactif documenté."""
    tok = os.environ.get("BETFAIR_SESSION_TOKEN")
    if tok:
        return tok
    user, pwd = os.environ.get("BETFAIR_USERNAME"), os.environ.get("BETFAIR_PASSWORD")
    if not user or not pwd:
        raise BFError("BETFAIR_USERNAME / BETFAIR_PASSWORD absents (ou fournis BETFAIR_SESSION_TOKEN).")
    data = urllib.parse.urlencode({"username": user, "password": pwd}).encode()
    req = urllib.request.Request(LOGIN_URL, data=data, headers={
        "X-Application": _app_key(), "Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise BFError(f"login HTTP {e.code}") from e
    if body.get("status") != "SUCCESS":
        raise BFError(f"login refusé : {body.get('status')} / {body.get('error')}")
    return body["token"]


def rpc(method: str, params: dict, token: str) -> list:
    payload = json.dumps({"jsonrpc": "2.0", "method": f"SportsAPING/v1.0/{method}", "params": params, "id": 1}).encode()
    req = urllib.request.Request(BETTING_URL, data=payload, headers={
        "X-Application": _app_key(), "X-Authentication": token,
        "Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            body = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise BFError(f"{method} HTTP {e.code}") from e
    if "error" in body:
        raise BFError(f"{method} : {body['error']}")
    return body["result"]


# ───────────────────────── marchés & volumes ─────────────────────────

def football_match_odds(token, date_from, date_to) -> list:
    """Marchés Match Odds football commençant dans [date_from, date_to] (ISO).
    → [{market_id, event_name, open_date, runners:{selectionId: name}}]"""
    params = {"filter": {"eventTypeIds": [SOCCER], "marketTypeCodes": ["MATCH_ODDS"],
                         "marketStartTime": {"from": date_from, "to": date_to}},
              "maxResults": 1000, "marketProjection": ["EVENT", "RUNNER_DESCRIPTION", "MARKET_START_TIME"]}
    out = []
    for m in rpc("listMarketCatalogue", params, token):
        out.append({"market_id": m["marketId"],
                    "event_name": (m.get("event") or {}).get("name", ""),
                    "open_date": (m.get("event") or {}).get("openDate") or m.get("marketStartTime"),
                    "runners": {r["selectionId"]: r["runnerName"] for r in m.get("runners", [])}})
    return out


def market_volumes(token, market_ids) -> dict:
    """Volume matché total et par sélection + meilleur prix back. → {market_id: {...}} (batch de 40)."""
    res = {}
    for i in range(0, len(market_ids), 40):
        chunk = market_ids[i:i + 40]
        params = {"marketIds": chunk,
                  "priceProjection": {"priceData": ["EX_BEST_OFFERS", "EX_TRADED"], "virtualise": True}}
        for mb in rpc("listMarketBook", params, token):
            runners = {}
            for r in mb.get("runners", []):
                back = (r.get("ex", {}).get("availableToBack") or [{}])
                best_back = back[0].get("price") if back and back[0] else None
                runners[r["selectionId"]] = {"best_back": best_back,
                                             "last_traded": r.get("lastPriceTraded"),
                                             "traded_volume": round(r.get("totalMatched", 0.0), 2)}
            res[mb["marketId"]] = {"total_matched": round(mb.get("totalMatched", 0.0), 2),
                                   "status": mb.get("status"), "runners": runners}
    return res


# ───────────────────────── appariement match ↔ marché ─────────────────────────

def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    s = s.replace("manchester", "man").replace("united", "utd")
    return re.sub(r"[^a-z]", "", re.sub(r"\b(fc|afc|cf|sc|ac|as|ss|us|club|de|the|w|women)\b", "", s))


def _tri(a):
    return {a[i:i + 3] for i in range(max(1, len(a) - 2))}


def sim(a, b):
    A, B = _tri(norm(a)), _tri(norm(b))
    return len(A & B) / max(1, len(A | B))


def match_fixture(home, away, kickoff_iso, markets, min_score=0.5):
    """Trouve le marché Betfair correspondant au match (noms + proximité horaire). → (market, score) ou (None, 0)."""
    ko = None
    try:
        ko = dt.datetime.fromisoformat(kickoff_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    best, best_s = None, 0.0
    for m in markets:
        ev = m["event_name"]
        # l'event Betfair est du type "Home v Away"
        parts = re.split(r"\s+v\s+|\s+vs\s+", ev, flags=re.I)
        if len(parts) == 2:
            s = (sim(home, parts[0]) + sim(away, parts[1])) / 2
        else:
            s = (sim(home, ev) + sim(away, ev)) / 2
        if ko and m.get("open_date"):
            try:
                od = dt.datetime.fromisoformat(m["open_date"].replace("Z", "+00:00"))
                if abs((od - ko).total_seconds()) > 6 * 3600:
                    s *= 0.5   # trop loin dans le temps : pénalité
            except ValueError:
                pass
        if s > best_s:
            best, best_s = m, s
    return (best, round(best_s, 3)) if best_s >= min_score else (None, round(best_s, 3))


def exchange_signal(market, vols):
    """À partir d'un marché apparié + ses volumes → probabilité d'échange (démarge back) et répartition
    d'argent par sélection (proxy du % d'argent public). Renvoie None si volumes absents."""
    mv = vols.get(market["market_id"]) if market else None
    if not mv or not mv.get("runners"):
        return None
    names = market["runners"]
    backs, money = {}, {}
    for sid, info in mv["runners"].items():
        nm = names.get(sid, str(sid))
        if info.get("best_back"):
            backs[nm] = info["best_back"]
        money[nm] = info.get("traded_volume", 0.0)
    fair = None
    if len(backs) >= 2:
        inv = {k: 1.0 / v for k, v in backs.items()}
        s = sum(inv.values())
        fair = {k: round(v / s, 4) for k, v in inv.items()}
    tot_money = sum(money.values())
    money_pct = {k: round(v / tot_money, 4) for k, v in money.items()} if tot_money > 0 else None
    return {"total_matched": mv["total_matched"], "exchange_fair": fair, "money_pct": money_pct,
            "retrieved_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}


# ───────────────────────── CLI ─────────────────────────

def cmd_status(a):
    tok = login()
    print(f"Session Betfair OK (token {tok[:6]}…). App key présente.")


def cmd_volumes(a):
    tok = login()
    date = a.date or dt.date.today().isoformat()
    d0, d1 = f"{date}T00:00:00Z", f"{date}T23:59:59Z"
    markets = football_match_odds(tok, d0, d1)
    print(f"{len(markets)} marchés Match Odds football le {date}.")
    vols = market_volumes(tok, [m["market_id"] for m in markets])
    tot = sum(v["total_matched"] for v in vols.values())
    print(f"Volume total matché : {tot:,.0f} £. Top 10 par volume :")
    ranked = sorted(markets, key=lambda m: vols.get(m["market_id"], {}).get("total_matched", 0), reverse=True)
    for m in ranked[:10]:
        v = vols.get(m["market_id"], {})
        print(f"  {m['event_name'][:34]:34s}  {v.get('total_matched',0):>12,.0f} £")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("status")
    v = sp.add_parser("volumes"); v.add_argument("--date")
    a = p.parse_args()
    try:
        {"status": cmd_status, "volumes": cmd_volumes}[a.cmd](a)
    except BFError as e:
        sys.exit(f"Erreur Betfair : {e}")


if __name__ == "__main__":
    main()
