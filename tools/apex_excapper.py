#!/usr/bin/env python3
"""Connecteur excapper (répartition d'argent / % de paris) pour APEX-WORM — via API AUTORISÉE uniquement.

excapper agrège le « money %/bets % » et les drops de cote sur bookmakers et échanges. APEX-WORM peut
l'utiliser pour la composante « % parieurs publics » du moteur Sharp (spec §12-13), MAIS seulement par
une API autorisée avec abonnement : ni scraping du site, ni contournement (spec §6).

Configuration (variables d'environnement, jamais dans le code — spec §33) :
  EXCAPPER_KEY       clé d'API de ton abonnement excapper
  EXCAPPER_API_URL   URL de base de l'API telle que fournie par excapper à ses abonnés

Sans ces deux variables, le connecteur renvoie « indisponible » et APEX-WORM garde la composante
% public marquée UNAVAILABLE — jamais estimée. On ne devine pas d'endpoint : c'est à l'abonné de fournir
l'URL documentée de son API.

Usage :
  python3 tools/apex_excapper.py status
  python3 tools/apex_excapper.py money --date 2026-09-30
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


class XCError(RuntimeError):
    pass


def _conf():
    key = os.environ.get("EXCAPPER_KEY")
    base = os.environ.get("EXCAPPER_API_URL")
    if not key or not base:
        raise XCError("EXCAPPER_KEY et/ou EXCAPPER_API_URL absents. Renseigne-les comme variables "
                      "d'environnement (clé + URL d'API de ton abonnement excapper). Sans API autorisée, "
                      "la composante % public reste indisponible — on ne scrape pas le site.")
    return key, base.rstrip("/")


def api(path, **params):
    key, base = _conf()
    params["key"] = key
    url = f"{base}/{path.lstrip('/')}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "apex-worm"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise XCError(f"clé refusée (HTTP {e.code}).") from e
        raise XCError(f"HTTP {e.code} sur {path}.") from e


def available() -> bool:
    return bool(os.environ.get("EXCAPPER_KEY") and os.environ.get("EXCAPPER_API_URL"))


def money_for_date(date: str) -> list:
    """Renvoie la liste brute des entrées money/bets % du jour, telle que servie par l'API de l'abonné.
    La forme exacte dépend de l'API excapper ; APEX-WORM lit défensivement les clés présentes."""
    data = api("matches", date=date)
    return data.get("data", data) if isinstance(data, dict) else data


def cmd_status(a):
    if not available():
        print("excapper NON CONFIGURÉ (EXCAPPER_KEY / EXCAPPER_API_URL absents) → % public restera UNAVAILABLE.")
        return
    try:
        _ = api("ping") if False else _conf()
        print("excapper configuré (clé + URL présentes). Test d'appel au premier `money`.")
    except XCError as e:
        sys.exit(f"Erreur excapper : {e}")


def cmd_money(a):
    date = a.date or dt.date.today().isoformat()
    rows = money_for_date(date)
    print(f"{len(rows)} entrées money/bets % pour le {date} (via API abonné).")
    for r in rows[:10]:
        print(" ", {k: r.get(k) for k in list(r)[:6]} if isinstance(r, dict) else r)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("status")
    m = sp.add_parser("money"); m.add_argument("--date")
    a = p.parse_args()
    try:
        {"status": cmd_status, "money": cmd_money}[a.cmd](a)
    except XCError as e:
        sys.exit(f"Erreur excapper : {e}")


if __name__ == "__main__":
    main()
