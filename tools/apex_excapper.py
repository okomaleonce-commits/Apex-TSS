#!/usr/bin/env python3
"""Collecteur excapper (Betfair MoneyWay) — VOLUME d'argent matché par match, données PUBLIQUES.

excapper.com publie, sur sa page d'accueil (rendue côté serveur, sans login), le tableau « Money Way » :
pour chaque match, l'argent total matché sur l'échange Betfair. C'est la donnée de volume que les
bookmakers cachent, et elle est PUBLIQUE — pas d'API, pas de clé, pas de compte.

Éthique de collecte (spec §6) : uniquement des pages publiques, en respectant robots.txt (excapper
autorise tout : `Disallow:` vide), avec un User-Agent identifiable et un rythme raisonnable. Aucun
contournement d'authentification, de paywall ou de CAPTCHA. Si robots.txt venait à interdire la page,
le collecteur s'arrête et renvoie « indisponible » — jamais de contournement.

Usage :
  python3 tools/apex_excapper.py status              # nb de matchs + top volumes du tableau public
  python3 tools/apex_excapper.py list [--limit 40]
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

BASE = "https://www.excapper.com"
ROOT = Path(__file__).resolve().parent.parent
CACHE = ROOT / "data" / "worm" / "excapper"
UA = "apex-worm/1.0 (+public Betfair MoneyWay data; personal research)"


class XCError(RuntimeError):
    pass


# ───────────────────────── robots.txt (respect strict) ─────────────────────────

_ROBOTS_CACHE = {}


def robots_allows(url: str) -> bool:
    """True si robots.txt autorise notre UA (*) à récupérer ce chemin. Défaut prudent : autoriser si
    robots illisible, refuser si une règle Disallow correspond."""
    host = urlparse(url).netloc
    path = urlparse(url).path or "/"
    if host not in _ROBOTS_CACHE:
        rules = []
        try:
            req = urllib.request.Request(f"https://{host}/robots.txt", headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=20) as r:
                txt = r.read().decode(errors="replace")
            ua_star, cur = False, []
            for line in txt.splitlines():
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                k, _, v = line.partition(":")
                k, v = k.strip().lower(), v.strip()
                if k == "user-agent":
                    ua_star = (v == "*")
                elif k == "disallow" and ua_star and v:
                    cur.append(v)
            rules = cur
        except Exception:  # noqa: BLE001
            rules = []
        _ROBOTS_CACHE[host] = rules
    return not any(path.startswith(d) for d in _ROBOTS_CACHE[host])


def fetch(url: str) -> str:
    if not robots_allows(url):
        raise XCError(f"robots.txt interdit {url} — collecte refusée (pas de contournement).")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            return r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        raise XCError(f"HTTP {e.code} sur {url}") from e
    except urllib.error.URLError as e:
        raise XCError(f"réseau : {e}") from e


# ───────────────────────── parsing (page publique) ─────────────────────────

_TR = re.compile(r'<tr[^>]*game_id="(\d+)"[^>]*>(.*?)</tr>', re.S | re.I)
_TD = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_ALT = re.compile(r'alt="([^"]*)"', re.I)


def _text(html: str) -> str:
    return re.sub(r"\s+", " ", _TAG.sub(" ", html)).strip()


def _money(cell: str):
    m = re.search(r"([\d\s.,]+)\s*€", cell)
    if not m:
        return None
    digits = re.sub(r"[^\d]", "", m.group(1))
    return float(digits) if digits else None


def parse_matches(html: str) -> list:
    """Tableau public → [{game_id, date, country, league, home, away, all_money_eur}]."""
    out = []
    for gid, body in _TR.findall(html):
        tds = _TD.findall(body)
        if len(tds) < 5:
            continue
        date = _text(tds[0])
        country = (_ALT.search(tds[1]) or [None, ""])[1] if _ALT.search(tds[1]) else _text(tds[1])
        league = _text(tds[2])
        teams = _text(tds[3])
        money = _money(tds[4])
        if " - " in teams:
            home, away = [t.strip() for t in teams.split(" - ", 1)]
        else:
            home, away = teams, ""
        out.append({"game_id": gid, "date": date, "country": country, "league": league,
                    "home": home, "away": away, "all_money_eur": money})
    return out


def list_matches() -> list:
    """Récupère et parse le tableau MoneyWay public de la page d'accueil."""
    return parse_matches(fetch(BASE + "/"))


def cache_matches(matches: list) -> Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    import json
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    p = CACHE / f"moneyway_{stamp}.jsonl"
    with open(p, "w", encoding="utf-8") as fh:
        for m in matches:
            fh.write(json.dumps(m, ensure_ascii=False) + "\n")
    return p


# ───────────────────────── CLI ─────────────────────────

def cmd_status(a):
    ms = list_matches()
    with_money = [m for m in ms if m["all_money_eur"]]
    print(f"excapper MoneyWay public : {len(ms)} matchs, {len(with_money)} avec volume.")
    for m in sorted(with_money, key=lambda x: x["all_money_eur"], reverse=True)[:10]:
        print(f"  {m['all_money_eur']:>12,.0f} €  {m['home']} - {m['away']}  ({m['country']} {m['league']})")


def cmd_list(a):
    ms = sorted(list_matches(), key=lambda x: x["all_money_eur"] or 0, reverse=True)
    p = cache_matches(ms)
    print(f"{len(ms)} matchs → {p.relative_to(ROOT)}")
    for m in ms[:a.limit]:
        print(f"  {(m['all_money_eur'] or 0):>10,.0f} €  {m['date']}  {m['home']} - {m['away']}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("status")
    l = sp.add_parser("list"); l.add_argument("--limit", type=int, default=40)
    a = p.parse_args()
    try:
        {"status": cmd_status, "list": cmd_list}[a.cmd](a)
    except XCError as e:
        sys.exit(f"Erreur excapper : {e}")


if __name__ == "__main__":
    main()
