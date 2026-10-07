#!/usr/bin/env python3
"""Connecteur arbworld (surebets / value bets) — SOUS RÉSERVE d'un accès autorisé.

arbworld.net sert ses cotes et surebets en direct via son endpoint /api/, que son robots.txt
INTERDIT explicitement aux robots (Disallow: /api/). La page HTML publique ne contient, elle, aucune
donnée exploitable (coquille chargée en JavaScript). Conséquence honnête : APEX-WORM ne collecte PAS
arbworld par scraping — ce serait contourner robots.txt (spec §6).

Deux voies restent propres :
  1. API AUTORISÉE : si tu disposes d'un accès API arbworld (abonnement), renseigne
       ARBWORLD_API_URL   URL de base de l'API telle que fournie par arbworld
       ARBWORLD_KEY       ta clé
     et le connecteur l'utilisera. Sans ça, il renvoie « indisponible » — jamais de scraping de /api/.
  2. Rien : la composante arbitrage/désaccord de marché reste UNAVAILABLE, jamais inventée.

Usage :
  python3 tools/apex_arbworld.py status
  python3 tools/apex_arbworld.py arbs --date 2026-09-30   # nécessite ARBWORLD_API_URL + ARBWORLD_KEY
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


class AWError(RuntimeError):
    pass


def available() -> bool:
    return bool(os.environ.get("ARBWORLD_API_URL") and os.environ.get("ARBWORLD_KEY"))


def _conf():
    base, key = os.environ.get("ARBWORLD_API_URL"), os.environ.get("ARBWORLD_KEY")
    if not base or not key:
        raise AWError("ARBWORLD_API_URL / ARBWORLD_KEY absents. La page publique n'expose pas les données "
                      "et robots.txt interdit /api/ : sans API autorisée, arbworld reste indisponible "
                      "(aucun scraping du endpoint interdit).")
    return base.rstrip("/"), key


def api(path, **params):
    base, key = _conf()
    params["key"] = key
    url = f"{base}/{path.lstrip('/')}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "apex-worm/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise AWError(f"clé refusée (HTTP {e.code}).") from e
        raise AWError(f"HTTP {e.code} sur {path}.") from e


def arbs_for_date(date: str) -> list:
    """Surebets/value bets du jour via l'API autorisée. Forme dépendante de l'API de l'abonné ;
    APEX-WORM lit défensivement les clés présentes."""
    data = api("surebets", date=date)
    return data.get("data", data) if isinstance(data, dict) else data


def cmd_status(a):
    if not available():
        print("arbworld NON CONFIGURÉ (ARBWORLD_API_URL / ARBWORLD_KEY absents). "
              "Données publiques indisponibles (robots.txt interdit /api/) → composante arbitrage UNAVAILABLE.")
        return
    print("arbworld configuré (API autorisée présente). Test au premier `arbs`.")


def cmd_arbs(a):
    date = a.date or dt.date.today().isoformat()
    rows = arbs_for_date(date)
    print(f"{len(rows)} surebets/value bets le {date} (via API autorisée).")
    for r in rows[:10]:
        print(" ", {k: r.get(k) for k in list(r)[:6]} if isinstance(r, dict) else r)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("status")
    m = sp.add_parser("arbs"); m.add_argument("--date")
    a = p.parse_args()
    try:
        {"status": cmd_status, "arbs": cmd_arbs}[a.cmd](a)
    except AWError as e:
        sys.exit(f"Erreur arbworld : {e}")


if __name__ == "__main__":
    main()
