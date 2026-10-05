#!/usr/bin/env python3
"""APEX-LEDGER — état durable de la chaîne de décision (audit 2026-10-05, étape 2).

Trois journaux APPEND-ONLY distincts, reconstruits depuis le disque à chaque lecture (donc
sûrs au redémarrage du conteneur) :

  state/ledger/<jour>.decisions.jsonl    décisions FIGÉES avant le coup d'envoi (une par match,
                                         première écriture gagnante — jamais réécrite)
  state/ledger/<jour>.reservations.jsonl réservations d'exposition ATOMIQUES et IDEMPOTENTES
                                         (verrou inter-processus fcntl ; clé déterministe)
  state/ledger/<jour>.settlements.jsonl  règlements AJOUTÉS séparément (ne touchent jamais les
                                         décisions ni les réservations)

Principes (audit) :
  • append-only strict : on n'édite/supprime jamais une ligne ; un règlement est un ÉVÉNEMENT à part.
  • idempotence : réserver deux fois la même clé ne double pas l'exposition (dédup sur reservation_id).
  • atomicité inter-passage : check-cumul-puis-append sous verrou exclusif fcntl (deux passages
    concurrents ne peuvent pas dépasser un plafond).
  • restart-safe : aucun état en mémoire entre passages ; tout est relu du journal.

Ce module NE price pas et N'autorise pas une mise : il tient l'état. L'autorisation reste soumise
au gel de promotion d'apex_sync (PROMOTION_FROZEN).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path


class LedgerError(Exception):
    """État du journal illisible/corrompu — toute opération échoue en SÉCURITÉ (fail-closed)."""

try:
    import fcntl  # POSIX uniquement (conteneur Linux) ; verrou inter-processus réel
    _HAS_FCNTL = True
except ImportError:  # pragma: no cover
    _HAS_FCNTL = False

ROOT = Path(__file__).resolve().parent.parent
# Emplacement du journal durable ; surchargeable par APEX_LEDGER_STATE (tests multi-processus).
STATE = Path(os.environ.get("APEX_LEDGER_STATE") or (ROOT / "state" / "ledger"))

# Plafonds d'exposition (fraction de bankroll) — mêmes valeurs que le Risk Manager SYNC.
DEFAULT_CAPS = {"match": 0.01, "league": 0.03, "market": 0.03, "total_day": 0.10}


def _paths(day):
    d = day.isoformat() if hasattr(day, "isoformat") else str(day)
    return {
        "decisions": STATE / f"{d}.decisions.jsonl",
        "reservations": STATE / f"{d}.reservations.jsonl",
        "settlements": STATE / f"{d}.settlements.jsonl",
        "commitments": STATE / f"{d}.commitments.jsonl",
    }


def reservation_id(day, match_id, market) -> str:
    """Clé déterministe d'une réservation : même (jour, match, marché) → même id (idempotence)."""
    d = day.isoformat() if hasattr(day, "isoformat") else str(day)
    raw = f"{d}|{match_id}|{market}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:16]


def _parse_strict(text, where="journal"):
    """Parse un contenu JSONL en REFUSANT toute ligne non vide illisible (fail-closed) : une dernière
    ligne tronquée ne doit pas être silencieusement ignorée (sinon l'exposition reconstruite serait
    sous-évaluée → autorisations erronées). Lève LedgerError."""
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError as e:
            raise LedgerError(f"{where} illisible à la ligne {i} : {e}") from e
    return out


def _read_jsonl(path):
    if not path.exists():
        return []
    return _parse_strict(path.read_text(encoding="utf-8"), where=str(path.name))


def _append_locked(path, record, *, dedup_key=None, dedup_field=None, cap_check=None):
    """Ajoute `record` à `path` sous verrou EXCLUSIF inter-processus (atomique).

    - dedup_key/dedup_field : si une ligne porte déjà cette valeur, on NE ré-écrit PAS et on renvoie
      (False, ligne_existante) — idempotence.
    - cap_check(existing_records) -> (ok, reason) : évalué APRÈS relecture SOUS verrou, juste avant
      l'append, pour que deux passages concurrents ne dépassent pas un plafond.
    Renvoie (written: bool, payload). payload = record écrit, ou la ligne existante, ou {'reason':...}.
    """
    # Indisponibilité du stockage (répertoire non créable, montage absent, droits manquants) : on échoue
    # en SÉCURITÉ (LedgerError) plutôt que de laisser remonter une OSError brute — l'appelant la traite
    # comme un refus d'autorisation (audit 2026-10-05, point 3 : « indisponibilité du stockage »).
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        fh = open(path, "r+", encoding="utf-8")
    except OSError as e:
        raise LedgerError(f"stockage indisponible pour {path} : {e}") from e
    with fh:
        if _HAS_FCNTL:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)   # bloque jusqu'à obtention (inter-processus)
        try:
            raw = fh.read()
            try:
                existing = _parse_strict(raw, where=str(path.name))
            except LedgerError as e:
                return False, {"reason": f"état illisible — opération refusée (fail-closed) : {e}"}
            if dedup_key is not None and dedup_field is not None:
                for r in existing:
                    if r.get(dedup_field) == dedup_key:
                        return False, r            # déjà présent → idempotent
            if cap_check is not None:
                ok, reason = cap_check(existing)
                if not ok:
                    return False, {"reason": reason}
            # Si le journal ne se termine PAS par un saut de ligne (dernier objet complet écrit sans
            # « \n », p. ex. écriture interrompue juste avant), un append naïf concaténerait les deux
            # objets (« }{ ») et rendrait le fichier illisible au passage suivant tout en annonçant un
            # succès (audit 2026-10-05, défaut D5). On RÉPARE la séparation sous verrou avant d'ajouter.
            # `raw` vient d'un fh.read() complet, donc le flux est déjà positionné en fin de fichier.
            if raw and not raw.endswith("\n"):
                fh.write("\n")
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
            return True, record
        finally:
            if _HAS_FCNTL:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


# ───────────────────────── décisions figées avant KO ─────────────────────────

def freeze_decision(day, match_id, kickoff_utc, decision, *, now=None):
    """Fige la décision d'un match AVANT le coup d'envoi. Première écriture gagnante (idempotent).
    Le KO est OBLIGATOIRE et valide ; il est revérifié SOUS VERROU avec l'instant d'écriture (pas un
    horodatage capturé avant l'attente du verrou) — un KO absent/invalide ou dépassé est refusé."""
    ko = _parse_utc(kickoff_utc)
    if ko is None:
        return False, {"reason": "coup d'envoi absent ou invalide — décision non figée"}
    rid = reservation_id(day, match_id, "__decision__")
    rec = {"reservation_id": rid, "match_id": match_id, "kickoff_utc": kickoff_utc,
           "decision": decision}

    def cap_check(_existing):
        t = now or dt.datetime.now(dt.timezone.utc)   # instant RÉEL d'écriture, sous verrou
        if t >= ko:
            return False, "coup d'envoi dépassé au moment de l'écriture — décision non figée"
        rec["frozen_at_utc"] = t.isoformat()
        return True, None

    return _append_locked(_paths(day)["decisions"], rec,
                          dedup_key=rid, dedup_field="reservation_id", cap_check=cap_check)


def get_frozen_decision(day, match_id):
    rid = reservation_id(day, match_id, "__decision__")
    for r in _read_jsonl(_paths(day)["decisions"]):
        if r.get("reservation_id") == rid:
            return r
    return None


# ───────────────────────── réservations d'exposition ─────────────────────────

def current_exposure(day):
    """Reconstruit l'exposition ENGAGÉE (fraction de bankroll) depuis le disque — restart-safe. Fond
    à la fois les réservations primitives (`reserve_exposure`) et l'exposition EFFECTIVE des
    transactions de décision (`commit_decision`) ; sous gel, `stake_effectif` vaut 0, donc une
    décision figée n'engage AUCUNE exposition (recherche seule)."""
    exp = {"matchs": {}, "ligues": {}, "marches": {}, "total": 0.0}

    def _add(mid, lg, mk, s):
        for scope, key in (("matchs", mid), ("ligues", lg), ("marches", mk)):
            if key is not None:
                exp[scope][key] = round(exp[scope].get(key, 0.0) + s, 6)
        exp["total"] = round(exp["total"] + s, 6)

    for r in _read_jsonl(_paths(day)["reservations"]):
        _add(r.get("match_id"), r.get("league"), r.get("market"), float(r.get("stake_pct") or 0.0))
    for r in _read_jsonl(_paths(day)["commitments"]):
        _add(r.get("match_id"), r.get("league"), r.get("market"), float(r.get("stake_effectif") or 0.0))
    return exp


def reserve_exposure(day, sel, stake_pct, caps=None, *, now=None):
    """Réserve `stake_pct` pour `sel` = {match_id, league, market}. Atomique + idempotent :
      • même (jour, match, marché) déjà réservé → renvoie la réservation existante, pas de doublon ;
      • sinon, sous verrou, vérifie les plafonds CUMULÉS (match/ligue/marché/jour) et n'écrit que
        si tous sont respectés — deux passages concurrents ne peuvent pas dépasser un plafond.
    Renvoie (reserved: bool, payload)."""
    caps = {**DEFAULT_CAPS, **(caps or {})}
    now = now or dt.datetime.now(dt.timezone.utc)
    mid, lg, mk = sel.get("match_id"), sel.get("league"), sel.get("market")
    rid = reservation_id(day, mid, mk)
    # Montant VALIDE : fini et strictement positif (refus NaN, ±inf, négatif, zéro — audit 2026-10-05).
    try:
        stake_pct = float(stake_pct)
    except (TypeError, ValueError):
        return False, {"reason": "montant de mise non numérique"}
    if not math.isfinite(stake_pct) or stake_pct <= 0.0:
        return False, {"reason": f"montant de mise invalide ({stake_pct!r}) — fini et > 0 requis"}
    rec = {"reservation_id": rid, "match_id": mid, "league": lg, "market": mk,
           "stake_pct": stake_pct, "reserved_at_utc": now.isoformat()}

    def cap_check(existing):
        # Un seul pari par match (anti-corrélation, au-delà du plafond cumulé) : si une réservation
        # existe déjà sur ce match (quel que soit le marché), on refuse (audit 2026-10-05).
        if any(r.get("match_id") == mid for r in existing):
            return False, "un pari est déjà engagé sur ce match (un pari par match)"
        em = sum(float(r.get("stake_pct") or 0.0) for r in existing if r.get("match_id") == mid)
        el = sum(float(r.get("stake_pct") or 0.0) for r in existing if r.get("league") == lg)
        ek = sum(float(r.get("stake_pct") or 0.0) for r in existing if r.get("market") == mk)
        et = sum(float(r.get("stake_pct") or 0.0) for r in existing)
        if em + stake_pct > caps["match"] + 1e-9:
            return False, f"plafond match dépassé ({em + stake_pct:.4f} > {caps['match']})"
        if el + stake_pct > caps["league"] + 1e-9:
            return False, f"plafond ligue dépassé ({el + stake_pct:.4f} > {caps['league']})"
        if ek + stake_pct > caps["market"] + 1e-9:
            return False, f"plafond marché dépassé ({ek + stake_pct:.4f} > {caps['market']})"
        if et + stake_pct > caps["total_day"] + 1e-9:
            return False, f"plafond journalier dépassé ({et + stake_pct:.4f} > {caps['total_day']})"
        return True, None

    return _append_locked(_paths(day)["reservations"], rec,
                          dedup_key=rid, dedup_field="reservation_id", cap_check=cap_check)


# ───────────────────────── règlements (fichier séparé) ─────────────────────────

def settle(day, match_id, market, score, result, *, now=None):
    """Ajoute un règlement dans un journal SÉPARÉ (append-only). Ne touche jamais la décision figée
    ni la réservation : le bilan se lit en joignant decisions ⋈ settlements, sans réécriture."""
    now = now or dt.datetime.now(dt.timezone.utc)
    rec = {"reservation_id": reservation_id(day, match_id, market), "match_id": match_id,
           "market": market, "score": score, "result": result, "settled_at_utc": now.isoformat()}
    # un règlement par (match, marché) : idempotent sur reservation_id
    return _append_locked(_paths(day)["settlements"], rec,
                          dedup_key=rec["reservation_id"], dedup_field="reservation_id")


def _commit_id(day, match_id) -> str:
    """Clé de transaction : une seule par match (un pari par match)."""
    return reservation_id(day, match_id, "__commit__")


def get_committed_decision(day, match_id):
    cid = _commit_id(day, match_id)
    for r in _read_jsonl(_paths(day)["commitments"]):
        if r.get("commit_id") == cid:
            return r
    return None


def commit_decision(day, sel, kickoff_utc, decision, stake_calcule, *, authorized,
                    caps=None, now=None):
    """TRANSACTION UNIQUE (un seul verrou, un seul append) : vérifie le coup d'envoi, fige la décision
    ET réserve l'exposition d'un même match en une opération atomique — jamais un `freeze_decision()`
    puis un `reserve_exposure()` séparés qui laisseraient un état mi-figé en cas d'interruption (audit
    2026-10-05, point 3).

    AUTORISATION FINALE EXPLICITE : `authorized=False` (gel de promotion) ⇒ `stake_effectif=0` ; le
    montant calculé `stake_calcule` est conservé comme INFORMATION DE RECHERCHE mais n'engage aucune
    exposition. Première écriture gagnante par match (idempotent). Renvoie (committed: bool, payload)."""
    caps = {**DEFAULT_CAPS, **(caps or {})}
    now = now or dt.datetime.now(dt.timezone.utc)
    mid, lg, mk = sel.get("match_id"), sel.get("league"), sel.get("market")
    ko = _parse_utc(kickoff_utc)
    if ko is None:
        return False, {"reason": "coup d'envoi absent ou invalide — rien figé ni réservé"}
    # Mise CALCULÉE : finie et ≥ 0 (une mise non finie ne doit jamais traverser — audit D3).
    try:
        stake_calcule = float(stake_calcule)
    except (TypeError, ValueError):
        return False, {"reason": "mise calculée non numérique — rien figé ni réservé"}
    if not math.isfinite(stake_calcule) or stake_calcule < 0.0:
        return False, {"reason": f"mise calculée invalide ({stake_calcule!r}) — finie et ≥ 0 requise"}
    authorized = bool(authorized)
    stake_effectif = stake_calcule if authorized else 0.0
    if authorized and stake_effectif <= 0.0:
        return False, {"reason": "autorisée mais mise effective nulle — incohérent, rien réservé"}
    cid = _commit_id(day, mid)
    rec = {"commit_id": cid, "match_id": mid, "league": lg, "market": mk, "kickoff_utc": kickoff_utc,
           "decision": decision, "stake_calcule": stake_calcule, "stake_effectif": stake_effectif,
           "authorized": authorized}

    def cap_check(existing):
        t = now   # instant RÉEL d'écriture, sous verrou (pas un horodatage capturé avant l'attente)
        if t >= ko:
            return False, "coup d'envoi dépassé au moment de l'écriture — rien figé ni réservé"
        rec["committed_at_utc"] = t.isoformat()
        # Un seul pari par match (anti-corrélation) — vaut même sous gel pour garder la trace unique.
        if any(r.get("match_id") == mid for r in existing):
            return False, "un pari est déjà engagé sur ce match (un pari par match)"
        # Plafonds cumulés : SEULE l'exposition EFFECTIVE compte (sous gel, 0 → jamais bloquant).
        if stake_effectif > 0.0:
            el = sum(float(r.get("stake_effectif") or 0.0) for r in existing if r.get("league") == lg)
            ek = sum(float(r.get("stake_effectif") or 0.0) for r in existing if r.get("market") == mk)
            et = sum(float(r.get("stake_effectif") or 0.0) for r in existing)
            if stake_effectif > caps["match"] + 1e-9:
                return False, f"plafond match dépassé ({stake_effectif:.4f} > {caps['match']})"
            if el + stake_effectif > caps["league"] + 1e-9:
                return False, f"plafond ligue dépassé ({el + stake_effectif:.4f} > {caps['league']})"
            if ek + stake_effectif > caps["market"] + 1e-9:
                return False, f"plafond marché dépassé ({ek + stake_effectif:.4f} > {caps['market']})"
            if et + stake_effectif > caps["total_day"] + 1e-9:
                return False, f"plafond journalier dépassé ({et + stake_effectif:.4f} > {caps['total_day']})"
        return True, None

    return _append_locked(_paths(day)["commitments"], rec,
                          dedup_key=cid, dedup_field="commit_id", cap_check=cap_check)


def _parse_utc(s):
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None
