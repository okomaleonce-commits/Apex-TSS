#!/usr/bin/env python3
"""
APEX-TURF-WORM — scanner « ver informationnel » hippique.

Equivalent turf de tools/apex_worm.py. Protocole AUTONOME : il ne cherche pas la value,
il cherche des ANOMALIES dans la trajectoire du marche entre deux passages.

Boucle : DISCOVER -> COLLECT -> NORMALIZE -> STORE -> COMPARE -> ANALYZE -> RANK -> REPORT

Difference de fond avec le WORM football, qui n'est pas cosmetique :
le PMU est un PARI MUTUEL. Il n'y a pas de Pinnacle, pas de books multiples, donc
pas de dispersion inter-books, pas de steam multi-books, pas de RLM. Ces moteurs
sortent UNAVAILABLE_STRUCTUREL — jamais estimes. Ce qui les remplace est propre au
turf et bouge l'argent plus fort : les NON-PARTANTS, les changements de driver et
l'argent tardif sur une cote unique.

    python3 tools/apex_turf_worm.py window
    python3 tools/apex_turf_worm.py scan [--date DDMMYYYY] [--max-courses N]
    python3 tools/apex_turf_worm.py report [--date YYYY-MM-DD]
    python3 tools/apex_turf_worm.py bilan  [--date YYYY-MM-DD]

Stdlib uniquement : se depose dans Apex-TSS sans nouvelle dependance.
"""
import argparse, hashlib, json, math, os, sys, time, urllib.request, urllib.error
import datetime as dt
from zoneinfo import ZoneInfo

API = "https://online.turfinfo.api.pmu.fr/rest/client/1/programme"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNAP = os.path.join(ROOT, "data", "turf_worm", "snapshots")
REPO = os.path.join(ROOT, "reports", "turf_worm")
TZ = os.environ.get("APEX_TIMEZONE", "UTC")

# provenance : jamais une valeur sans son origine
OBSERVED, CALCULATED, UNAVAILABLE = "OBSERVED", "CALCULATED", "UNAVAILABLE"
UNAVAILABLE_STRUCTUREL = "UNAVAILABLE_STRUCTUREL"   # le pari mutuel rend la mesure impossible

# q25 mesure de la variation journaliere en trot : en dessous, c'est du bruit.
# 12 % (premiere version) etait SOUS le q25 : la moitie des partants bougent de
# plus de 34,8 % dans une journee. Un seuil a 12 % signalait donc tout le monde.
SEUIL_MOUVEMENT = 18.0

STATUTS_A_VENIR = {"PROGRAMMEE", "ROUGE_AUX_PARTANTS", "DEPART_IMMINENT",
                   "A_PARTIR", "COURSE_ARRETEE_PROVISOIREMENT"}

DISCIPLINES_MOTEUR = {"TROT_ATTELE": "trot", "TROT_MONTE": "trot",
                      "HAIES": "obst", "STEEPLE_CHASE": "obst", "CROSS": "obst"}


try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from apex_turf_lonaci import charger_scope as _lonaci_scope
except ImportError:
    _lonaci_scope = None

QFILE = os.path.join(ROOT, "tools", "params", "turf_worm_quantiles.json")
try:
    QUANT = json.load(open(QFILE, encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    QUANT = {}


def percentile_score(disc, metric, value):
    """
    Un score n'est pas une formule : c'est la place de la valeur dans la distribution
    REELLE de la meme metrique, dans la MEME discipline. 100 veut dire "jamais vu
    au-dela du 99e centile", pas "la formule sature".

    Retourne (score 0-100, provenance, reference) ou (None, UNAVAILABLE, motif) quand
    la discipline n'a pas ses propres quantiles : on ne transpose JAMAIS ceux d'une
    autre discipline.
    """
    q = (QUANT.get(disc) or {}).get(metric)
    if not q:
        return None, UNAVAILABLE, (f"aucun quantile mesure pour {metric} en {disc} — "
                                   f"les quantiles d'une autre discipline ne sont pas transposables")
    pts = sorted((int(k[1:]), v) for k, v in q.items() if k.startswith("q"))
    if value <= pts[0][1]:
        return 0, CALCULATED, f"sous le q{pts[0][0]} ({pts[0][1]})"
    for (pa, va), (pb, vb) in zip(pts, pts[1:]):
        if value <= vb:
            sc = pa + (pb - pa) * ((value - va) / (vb - va) if vb > va else 0)
            return round(sc), CALCULATED, f"entre q{pa} ({va}) et q{pb} ({vb})"
    return 100, CALCULATED, f"au-dela du q{pts[-1][0]} ({pts[-1][1]})"


# --------------------------------------------------------------- fenetre APEX

def apex_day(now=None, tz=None):
    """Journee APEX : 08:00:00 -> 07:59:59 le lendemain, dans APEX_TIMEZONE."""
    z = ZoneInfo(tz or TZ)
    now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(z)
    d = now.date() if now.hour >= 8 else now.date() - dt.timedelta(days=1)
    start = dt.datetime.combine(d, dt.time(8, 0), tzinfo=z)
    return d.isoformat(), start, start + dt.timedelta(days=1) - dt.timedelta(seconds=1)


def cmd_window(a):
    day, s, e = apex_day()
    print(f"fuseau APEX        : {TZ}")
    print(f"journee APEX       : {day}")
    print(f"  debut            : {s.isoformat()}")
    print(f"  fin              : {e.isoformat()}")
    print(f"maintenant         : {dt.datetime.now(ZoneInfo(TZ)).isoformat(timespec='seconds')}")
    return 0


# ------------------------------------------------------------------- collecte

def get(url, tries=3):
    for i in range(tries):
        try:
            r = urllib.request.Request(url, headers={"User-Agent": "APEX-TURF-WORM/1.0"})
            with urllib.request.urlopen(r, timeout=30) as f:
                return json.load(f)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if i == tries - 1:
                return {"_erreur": f"{type(e).__name__}: {e}"}
            time.sleep(2 ** i)


def discover(ddmmyyyy):
    """DISCOVER : toutes les courses du programme du jour."""
    p = get(f"{API}/{ddmmyyyy}")
    if "_erreur" in p:
        return [], p["_erreur"]
    out = []
    for r in (p.get("programme", {}).get("reunions") or []):
        for c in (r.get("courses") or []):
            out.append(dict(
                reunion=r["numOfficiel"], course=c["numExterne"],
                course_id=f"{ddmmyyyy}-R{r['numOfficiel']}C{c['numExterne']}",
                libelle=c.get("libelle"), hippodrome=(r.get("hippodrome") or {}).get("libelleCourt"),
                pays=((r.get("pays") or {}).get("code")),
                discipline=c.get("specialite"), distance=c.get("distance"),
                depart_utc=(dt.datetime.fromtimestamp(c["heureDepart"] / 1000, dt.timezone.utc)
                            .isoformat(timespec="seconds") if c.get("heureDepart") else None),
                statut=c.get("statut"), declares=c.get("nombreDeclaresPartants"),
                categorie=c.get("categorieParticularite")))
    return out, None


def collect(ddmmyyyy, course):
    """COLLECT + NORMALIZE : partants, cotes, non-partants."""
    d = get(f"{API}/{ddmmyyyy}/R{course['reunion']}/C{course['course']}/participants")
    if "_erreur" in d:
        return None, d["_erreur"]
    ps = d.get("participants") or []
    partants, nonpartants, sans_cote = [], [], []
    for p in ps:
        if p.get("statut") != "PARTANT":
            nonpartants.append(dict(num=p["numPmu"], nom=p.get("nom"), statut=p.get("statut")))
            continue
        rap = (p.get("dernierRapportDirect") or {})
        cote = rap.get("rapport")
        if cote is None:
            sans_cote.append(p["numPmu"])
        partants.append(dict(
            num=p["numPmu"], nom=p.get("nom"), cote=cote,
            favori=bool(rap.get("favoris")),
            cote_ts=(dt.datetime.fromtimestamp(rap["dateRapport"] / 1000, dt.timezone.utc)
                     .isoformat(timespec="seconds") if rap.get("dateRapport") else None),
            driver=p.get("driver"), entraineur=p.get("entraineur"),
            deferre=p.get("deferre"), musique=p.get("musique"),
            gains=(p.get("gainsParticipant") or {}).get("gainsCarriere")))
    cotes = [x["cote"] for x in partants if x["cote"]]
    return dict(
        partants=partants, nonpartants=nonpartants, sans_cote=sans_cote,
        n_partants=len(partants),
        overround=(sum(1 / c for c in cotes) if cotes else None),
        overround_prov=CALCULATED if cotes else UNAVAILABLE,
        penetrometre=(d.get("penetrometre") or None)), None


# -------------------------------------------------------------------- moteurs

def _devig(partants):
    cotes = {x["num"]: x["cote"] for x in partants if x["cote"]}
    s = sum(1 / c for c in cotes.values())
    return {n: (1 / c) / s for n, c in cotes.items()} if s else {}


def engine_derive(cur, prev):
    """
    DERIVE : trajectoire de la cote de chaque partant entre deux passages.
    En pari mutuel c'est LE signal : il n'y a qu'une cote, et elle est l'argent.
    """
    if not prev:
        return dict(score=None, provenance=UNAVAILABLE, motif="premier passage du jour")
    pc = {x["num"]: x["cote"] for x in prev["marche"]["partants"] if x["cote"]}
    mv = []
    for x in cur["marche"]["partants"]:
        a, b = pc.get(x["num"]), x["cote"]
        if a and b:
            mv.append(dict(num=x["num"], nom=x["nom"], avant=a, apres=b,
                           variation_pct=round((b - a) / a * 100, 1)))
    if not mv:
        return dict(score=None, provenance=UNAVAILABLE, motif="aucun partant apparie")
    amp = max(abs(m["variation_pct"]) for m in mv)
    disc = DISCIPLINES_MOTEUR.get(cur["course"]["discipline"])
    sc, prov, ref = percentile_score(disc, "derive", amp) if disc else (None, UNAVAILABLE, "hors perimetre")
    # Convention maison : un score None s'accompagne TOUJOURS d'un motif. Sans ce
    # motif, engine_outsider — qui relaie l'indisponibilite de derive — levait un
    # KeyError. Il fallait deux conditions pour le voir : un passage precedent ET
    # une course hors discipline calibree. Le scan planifie filtrant sur le
    # perimetre LONACI, et n'ayant jamais eu deux passages, le bug dormait.
    return dict(score=sc, provenance=prov, reference=ref,
                motif=(ref if sc is None else None),
                echelle=("quantiles mesures sur une JOURNEE entiere (cote du matin -> cote finale), "
                         "alors que ce score compare deux passages HORAIRES : borne superieure "
                         "conservatrice, le signal horaire est SOUS-ESTIME. A re-estimer sur les "
                         "snapshots du WORM des 200 paires de passages."),
                amplitude_max_pct=amp,
                resserrements=sorted([m for m in mv if m["variation_pct"] <= -SEUIL_MOUVEMENT],
                                     key=lambda m: m["variation_pct"])[:5],
                derives=sorted([m for m in mv if m["variation_pct"] >= SEUIL_MOUVEMENT],
                               key=lambda m: -m["variation_pct"])[:5],
                mouvements=mv)


def engine_nonpartants(cur, prev):
    """
    NON-PARTANTS : signal propre au turf, absent du football. Un retrait tardif
    redistribue tout l'argent et change la structure de la course.
    """
    now = {x["num"] for x in cur["marche"]["nonpartants"]}
    before = {x["num"] for x in prev["marche"]["nonpartants"]} if prev else set()
    nouveaux = sorted(now - before)
    if not prev:
        return dict(score=None, provenance=UNAVAILABLE, motif="premier passage",
                    total=sorted(now))
    n = cur["marche"]["n_partants"] or 1
    return dict(score=min(100, round(len(nouveaux) * 45 + (len(now) / (n + len(now))) * 40)),
                provenance=OBSERVED, nouveaux_retraits=nouveaux, total=sorted(now),
                noms=[x["nom"] for x in cur["marche"]["nonpartants"] if x["num"] in nouveaux])


def engine_favori(cur):
    """
    FAVORI DOMINANT — analogue du Blowout. Score = percentile empirique dans la
    discipline, et non une formule : la premiere version notait 100/100 presque
    toutes les courses, ce qui ne porte aucune information.
    """
    p = _devig(cur["marche"]["partants"])
    if len(p) < 3:
        return dict(score=None, provenance=UNAVAILABLE, motif="moins de 3 cotes")
    disc = DISCIPLINES_MOTEUR.get(cur["course"]["discipline"])
    if not disc:
        return dict(score=None, provenance=UNAVAILABLE,
                    motif=f"{cur['course']['discipline']} : aucune distribution mesuree")
    o = sorted(p.items(), key=lambda kv: -kv[1])
    p1, p2 = o[0][1], o[1][1]
    ecart = p1 - p2
    s1, pr1, r1 = percentile_score(disc, "p1", p1)
    s2, pr2, r2 = percentile_score(disc, "ecart", ecart)
    sc = (round((s1 + s2) / 2) if s1 is not None and s2 is not None else (s1 or s2))
    return dict(score=sc, provenance=(pr1 if sc is not None else UNAVAILABLE),
                favori=o[0][0], p_favori=round(p1, 4), p_second=round(p2, 4),
                ecart=round(ecart, 4),
                percentile_p1=s1, percentile_ecart=s2, reference=f"p1 {r1} · ecart {r2}")


def engine_outsider(cur, prev):
    """
    OUTSIDER — analogue de l'Upset : un partant a cote longue vers qui l'argent va.
    Sans mouvement observable il n'y a pas de signal : on ne devine pas un upset.
    """
    d = engine_derive(cur, prev)
    if d["score"] is None:
        return dict(score=None, provenance=UNAVAILABLE,
                    motif=(d.get("motif") or d.get("reference")
                           or "derive indisponible, motif non renseigne"))
    p = _devig(cur["marche"]["partants"])
    cands = []
    for m in d.get("resserrements", []):
        pr = p.get(m["num"])
        if pr is not None and pr < 0.12:          # vrai outsider, pas un 2e favori
            cands.append(dict(num=m["num"], nom=m["nom"], cote=m["apres"],
                              p_marche=round(pr, 4), variation_pct=m["variation_pct"]))
    if not cands:
        return dict(score=0, provenance=OBSERVED, motif="aucun resserrement sur un outsider")
    best = min(cands, key=lambda c: c["variation_pct"])
    sc, prov, ref = percentile_score(DISCIPLINES_MOTEUR.get(cur["course"]["discipline"]),
                                     "derive", abs(best["variation_pct"]))
    return dict(score=sc, provenance=prov, reference=ref,
                candidats=cands[:5], meilleur=best)


def engine_non_terminaison(cur):
    """
    RISQUE DE NON-TERMINAISON — moteur sans equivalent football.
    Taux de base MESURES, pas choisis : 21,2 % en attele, 28,7 % en monte.
    En obstacle le taux depend du champ et de l'hippodrome : le WORM ne price pas,
    il signale seulement la tranche de champ que la calibration a identifiee a risque.
    """
    disc = cur["course"]["discipline"]
    n = cur["marche"]["n_partants"]
    if disc == "TROT_ATTELE":
        base, prov = 0.212, OBSERVED
    elif disc == "TROT_MONTE":
        base, prov = 0.287, OBSERVED
    elif disc in ("HAIES", "STEEPLE_CHASE", "CROSS"):
        base, prov = None, UNAVAILABLE   # depend du champ et du terrain : T4 seul price
    else:
        return dict(score=None, provenance=UNAVAILABLE,
                    motif=f"discipline {disc} : aucun taux de base mesure")
    tranche = ("5-9" if n <= 9 else "10-13" if n <= 13 else "14-17" if n <= 17 else "18-30")
    alerte = n >= 18
    if base is None:
        return dict(score=None, provenance=prov, tranche_champ=tranche,
                    champ_a_risque=alerte,
                    motif="obstacle : taux de base non constant, le pricing revient a la cellule statistique")
    return dict(score=min(100, round(base * 240 + (40 if alerte else 0))), provenance=prov,
                taux_base=base, attendu_non_finissants=round(base * n, 2),
                tranche_champ=tranche, champ_a_risque=alerte)


def engines_structurellement_absents():
    """
    Ce que le WORM football mesure et que le pari mutuel rend IMPOSSIBLE a mesurer.
    Les ecrire explicitement vaut mieux que les omettre : une case vide se remplit
    un jour par une estimation, une case nommee UNAVAILABLE_STRUCTUREL non.
    """
    return {
        "sharp_books": dict(score=None, provenance=UNAVAILABLE_STRUCTUREL,
                            motif="pari mutuel : ni Pinnacle ni book asiatique, une seule cote"),
        "dispersion_inter_books": dict(score=None, provenance=UNAVAILABLE_STRUCTUREL,
                                       motif="une seule cote, aucune dispersion a mesurer"),
        "steam_multi_books": dict(score=None, provenance=UNAVAILABLE_STRUCTUREL,
                                  motif="pas de books multiples a synchroniser"),
        "reverse_line_movement": dict(score=None, provenance=UNAVAILABLE_STRUCTUREL,
                                      motif="le PMU ne publie pas le pourcentage de parieurs par partant"),
        "volume_echange": dict(score=None, provenance=UNAVAILABLE,
                               motif="masse des enjeux non collectee ; disponible en principe, non branchee"),
    }


# -------------------------------------------------------------- rang, decision

def rank_and_decide(sn):
    """
    RANK + decision. Les paliers du WORM football (JOUER / JOUER_PETIT) n'existent PAS
    ici : les deux gates de pari turf sont fermees par le backtest (trot ROI -4,58 %
    sur 118 paris, obstacle -89,05 % sur 21). Le palier maximal est SURVEILLER.
    """
    e = sn["moteurs"]
    scores = [v["score"] for v in e.values() if isinstance(v, dict) and v.get("score") is not None]
    sn["score_global"] = max(scores) if scores else None
    sn["moteurs_actifs"] = len(scores)

    g = sn["score_global"]
    dom = max(((k, v["score"]) for k, v in e.items()
               if isinstance(v, dict) and v.get("score") is not None),
              key=lambda kv: kv[1], default=(None, None))
    sn["signal_dominant"] = dom[0]

    if sn["course"]["discipline"] not in DISCIPLINES_MOTEUR:
        sn["decision"] = "HORS_PERIMETRE"
        sn["motif"] = (f"{sn['course']['discipline']} : aucun moteur calibre "
                       f"(le plat n'en a pas). Aucune lecture chiffree.")
    elif g is None:
        sn["decision"] = "PREMIER_PASSAGE"
        sn["motif"] = "aucune comparaison possible avant un deuxieme relevé"
    elif g >= 70:
        sn["decision"] = "SURVEILLER_FORT"
        sn["motif"] = f"{sn['signal_dominant']} a {g}/100"
    elif g >= 45:
        sn["decision"] = "SURVEILLER"
        sn["motif"] = f"{sn['signal_dominant']} a {g}/100"
    else:
        sn["decision"] = "RIEN_A_SIGNALER"
        sn["motif"] = f"score maximal {g}/100, sous le seuil de 45"
    sn["unites"] = 0.0
    sn["autorite_pari"] = False
    return sn


# ------------------------------------------------------------------ stockage

def read_snapshots(day):
    """Lecture tolerante : un fichier en cours d'ecriture ne doit pas tout casser."""
    f = os.path.join(SNAP, f"{day}.jsonl")
    if not os.path.exists(f):
        return []
    out = []
    for ln in open(f, encoding="utf-8"):
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out


def last_pass(hist, course_id):
    prev = [h for h in hist if h["course"]["course_id"] == course_id]
    return prev[-1] if prev else None


# ------------------------------------------------- signature des signaux du jour

# Le marqueur vit DANS le dossier des snapshots : c'est lui qui part en artefact
# et revient au passage suivant. Ailleurs il serait perdu entre deux runs, et
# --email-si-changement enverrait a chaque fois.
MARQUEUR = os.path.join(SNAP, "dernier_digest.json")


def signature_signaux(day):
    """
    Empreinte de ce qu'un digest ANNONCE, et de rien d'autre.

    Deux passages dont les cotes bougent sans changer un seul palier produisent la
    MEME empreinte : c'est voulu. Ce qui declenche un envoi, c'est qu'une course
    entre, sorte, ou change de palier — pas qu'une cote ait remue de 2 %.

    Entrent dans l'empreinte : course, decision, signal dominant. PAS le score,
    qui bouge en permanence de quelques points sans rien dire de nouveau.
    """
    hist = read_snapshots(day)
    if not hist:
        return dict(n_signaux=0, empreinte=hashlib.sha256(b"vide").hexdigest(), lignes=[])
    npass = max(h["passage"] for h in hist)
    dernier = {h["course"]["course_id"]: h for h in hist if h["passage"] == npass}
    lignes = sorted(
        f"{cid}|{h['decision']}|{h['signal_dominant']}"
        for cid, h in dernier.items() if h["decision"].startswith("SURVEILLER"))
    return dict(n_signaux=len(lignes),
                empreinte=hashlib.sha256("\n".join(lignes).encode("utf-8")).hexdigest(),
                lignes=lignes)


def _lire_signature():
    try:
        return json.load(open(MARQUEUR, encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _ecrire_signature(sig):
    try:
        os.makedirs(SNAP, exist_ok=True)
        json.dump(sig, open(MARQUEUR, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    except OSError as e:
        print(f"marqueur non ecrit ({e}) : le prochain passage reenverra.", file=sys.stderr)


# ----------------------------------------------------------------------- scan

def cmd_scan(a):
    # Le jour du REGISTRE est celui du programme scanne, pas celui du scan.
    # Sans cela, `scan --date 03102026` ecrit dans le registre du 02/10 et
    # `report --date 2026-10-03` ne trouve rien : le registre serait melange.
    if a.date:
        try:
            d = dt.datetime.strptime(a.date, "%d%m%Y").date()
        except ValueError:
            print(f"--date attend DDMMYYYY, recu : {a.date}", file=sys.stderr)
            return 2
        day, ddmmyyyy = d.isoformat(), a.date
    else:
        day = apex_day()[0]
        ddmmyyyy = dt.datetime.now(ZoneInfo(TZ)).strftime("%d%m%Y")
    os.makedirs(SNAP, exist_ok=True)
    os.makedirs(REPO, exist_ok=True)

    courses, err = discover(ddmmyyyy)
    if err:
        print(f"DISCOVER a echoue : {err}", file=sys.stderr)
        return 1
    print(f"DISCOVER : {len(courses)} courses au programme du {ddmmyyyy}")

    hist = read_snapshots(day)
    npass = len({h["passage"] for h in hist}) + 1
    now = dt.datetime.now(dt.timezone.utc)
    # liste BLANCHE : une liste noire laisse passer tout statut non anticipe.
    # Statuts reellement observes le 02/10/2026 : ARRIVEE_DEFINITIVE_COMPLETE,
    # FIN_COURSE, ARRIVEE_PROVISOIRE (termines) ; PROGRAMMEE, ROUGE_AUX_PARTANTS (a venir).
    sel = [c for c in courses if c.get("statut") in STATUTS_A_VENIR]

    # --lonaci : restreindre au programme officiel PMU LONACI (Cote d'Ivoire).
    # LONACI emploie les memes codes R#C# que le PMU francais (verifie sur les
    # 30 courses du 02/10/2026), le perimetre est donc une liste de codes.
    # par_course doit exister meme sans --lonaci : chaque instantane porte le
    # champ lonaci_course, et sans perimetre il vaut simplement None.
    lonaci, par_course = None, {}
    if a.lonaci:
        if _lonaci_scope is None:
            print("apex_turf_lonaci.py introuvable a cote de ce script.", file=sys.stderr)
            return 2
        codes, meta = _lonaci_scope(day)
        if codes is None:
            print(f"AUCUN perimetre LONACI pour {day}. Le construire d'abord :",
                  file=sys.stderr)
            print(f"  python3 tools/apex_turf_lonaci.py scope --date {ddmmyyyy} "
                  f"--from-text <fichier>", file=sys.stderr)
            print("Sans perimetre, --lonaci NE SCANNE RIEN : scanner tout le programme "
                  "francais serait ignorer la demande.", file=sys.stderr)
            return 2
        avant = len(sel)
        sel = [c for c in sel if f"R{c['reunion']}C{c['course']}" in codes]
        par_course = {c["code"]: c for c in (meta.get("courses") or [])}
        lonaci = dict(journee=meta.get("journee"), source=meta.get("source"),
                      releve=meta.get("releve"), n_lonaci=meta.get("n_lonaci"),
                      repartition=meta.get("repartition"),
                      cotes_origine=meta.get("cotes_origine"),
                      avertissement_masse=meta.get("avertissement_masse"))
        print(f"LONACI : perimetre du {meta.get('journee')} "
              f"({meta.get('n_lonaci')} courses au programme, "
              f"{len(codes)} dans le perimetre des moteurs) "
              f"-> {len(sel)} a venir sur {avant}")
        print(f"  cotes : {meta.get('cotes_origine')} — masse d'enjeux non verifiee, "
              f"voir avertissement_masse")
    # --codes : restreindre a des courses NOMMEES. C'est ce qui permet a un passage
    # de ne porter que sur les Nationale, sans toucher au filtre LONACI general.
    if getattr(a, "codes", None):
        voulus = {c.strip().upper() for c in a.codes.split(",") if c.strip()}
        avant = len(sel)
        sel = [c for c in sel if f"R{c['reunion']}C{c['course']}" in voulus]
        manquants = voulus - {f"R{c['reunion']}C{c['course']}" for c in sel}
        print(f"CODES : {len(sel)} retenue(s) sur {avant} "
              f"({len(voulus)} demandee(s))")
        if manquants:
            # Jamais en silence : une course demandee et absente est soit deja
            # partie, soit hors du programme francais, et ca se dit.
            print(f"  absentes des courses a venir : {', '.join(sorted(manquants))}")
    if a.max_courses:
        sel = sel[:a.max_courses]
    print(f"passage {npass} | {len(sel)} courses non terminees retenues")

    out, fh = [], open(os.path.join(SNAP, f"{day}.jsonl"), "a", encoding="utf-8")
    try:
        for c in sel:
            m, err = collect(ddmmyyyy, c)
            if err:
                print(f"  COLLECT {c['course_id']} : {err}", file=sys.stderr)
                continue
            dep = (dt.datetime.fromisoformat(c["depart_utc"]) if c["depart_utc"] else None)
            sn = dict(
                passage=npass, releve_utc=now.isoformat(timespec="seconds"),
                journee_apex=day, course=c, marche=m,
                minutes_avant_depart=(round((dep - now).total_seconds() / 60)
                                      if dep else None),
                moteur_calibre=DISCIPLINES_MOTEUR.get(c["discipline"]),
                lonaci=lonaci,
                lonaci_course=par_course.get(c["course_id"].split("-")[-1]))
            prev = last_pass(hist, c["course_id"])
            # ANOMALIES : seules celles-ci classent la course. Ce sont des ecarts
            # a une distribution, donc informatifs.
            sn["moteurs"] = dict(
                derive=engine_derive(sn, prev),
                non_partants=engine_nonpartants(sn, prev),
                favori_dominant=engine_favori(sn),
                outsider=engine_outsider(sn, prev),
                **engines_structurellement_absents())
            # CONTEXTE STRUCTUREL : constant a discipline et champ donnes, donc sans
            # pouvoir discriminant. Il informe la lecture, il ne classe PAS : le faire
            # entrer dans le rang mettait toutes les courses d'attele a 51/100.
            sn["contexte"] = dict(non_terminaison=engine_non_terminaison(sn))
            sn["passage_precedent"] = (prev["releve_utc"] if prev else None)
            sn = rank_and_decide(sn)
            fh.write(json.dumps(sn, ensure_ascii=False) + "\n")
            out.append(sn)
    finally:
        fh.close()

    print(f"STORE : {len(out)} releves ajoutes (append-only) -> {SNAP}/{day}.jsonl")
    if not out:
        # Un passage qui n'a RIEN releve ne doit pas produire de digest : le
        # rapport serait construit sur le dernier passage non vide du registre et
        # annoncerait des signaux qui ne viennent pas de celui-ci. Mesure a l'appui :
        # un passage a 0 course annoncait « 2 signaux sur 6 courses ».
        print("AUCUN releve sur ce passage : ni rapport, ni digest. "
              "Il n'y a rien a annoncer, et annoncer le passage precedent serait faux.")
        return 0
    write_report(day, out, npass)
    # Regle maison (CLAUDE.md) : tout passage se termine par un digest. Deux
    # amenagements, et ils ne l'affaiblissent pas :
    #
    #   --sans-email          en mode serie, seul le DERNIER passage envoie. Quinze
    #                         digests identiques dans une matinee ne sont pas quinze
    #                         fois l'information, c'est quatorze fois du bruit.
    #   --email-si-changement n'envoie que si l'ensemble des signaux a bouge depuis
    #                         le dernier envoi. Un passage qui ne change rien est un
    #                         passage valide ; il n'a simplement rien a annoncer.
    #
    # Dans les deux cas le passage est COMPLET : le releve est scelle, le rapport
    # ecrit. C'est l'envoi qui est economise, pas la mesure.
    if getattr(a, "sans_email", False):
        print("digest non envoye (--sans-email) : la serie enverra a la fin.")
        return 0
    if getattr(a, "email_si_changement", False):
        sig = signature_signaux(day)
        ancien = _lire_signature()
        if ancien == sig:
            print(f"digest non envoye : signaux inchanges depuis le dernier envoi "
                  f"({sig['n_signaux']} signal(aux), empreinte {sig['empreinte'][:12]}).")
            return 0
        print(f"signaux modifies -> envoi. avant={(ancien or {}).get('empreinte','(aucun)')[:12]} "
              f"apres={sig['empreinte'][:12]}")
    try:
        cmd_email(argparse.Namespace(date=day, to=None,
                                     phase=getattr(a, "phase", None)))
        if getattr(a, "email_si_changement", False):
            _ecrire_signature(signature_signaux(day))
    except Exception as e:
        print(f"digest non construit : {type(e).__name__}: {e}", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------- serie

def _reste_a_venir(ddmmyyyy, codes=None):
    """
    Combien de courses sont encore a venir. Sert d'ARRET a la serie : continuer a
    sonder un programme termine brule du temps de runner pour rien.
    Renvoie None si le DISCOVER echoue — on ne s'arrete pas sur une panne reseau.
    """
    courses, err = discover(ddmmyyyy)
    if err:
        return None
    sel = [c for c in courses if c.get("statut") in STATUTS_A_VENIR]
    if codes is not None:
        sel = [c for c in sel if f"R{c['reunion']}C{c['course']}" in codes]
    return len(sel)


def cmd_serie(a):
    """
    Plusieurs passages dans UN SEUL run, espaces de --intervalle minutes.

    POURQUOI. Le cron de GitHub Actions n'est pas une horloge : les declenchements
    planifies sont rendus au mieux, et la mesure du 08/10/2026 est sans appel —
    douze passages attendus par jour, DEUX executes en deux jours. Or le moteur
    `derive` exige deux passages pour exister, et en pari mutuel la derive est LE
    signal : il n'y a qu'une cote, et elle est l'argent. Un seul passage par jour
    rend la cellule aveugle a ce qui compte le plus.

    Un run qui enchaine ses propres passages ne depend plus que d'UN declenchement
    recu au lieu de douze.

    CE QUE CA COUTE, parce qu'il faut le dire : attendre dans un job consomme du
    temps de runner facturable. C'est pourquoi la serie n'est PAS le mode planifie
    par defaut — le cron redondant l'est, et il ne dort jamais. La serie est l'outil
    du declenchement manuel, quand on veut couvrir correctement une course precise.
    """
    ddmmyyyy = a.date or dt.datetime.now(ZoneInfo(TZ)).strftime("%d%m%Y")
    debut = dt.datetime.now(dt.timezone.utc)
    limite = debut + dt.timedelta(minutes=a.budget_min)
    print(f"SERIE : jusqu'a {a.passages} passage(s), un toutes les {a.intervalle} min, "
          f"budget {a.budget_min} min (fin au plus tard {limite:%H:%M} UTC)")

    codes = None
    if a.lonaci and _lonaci_scope is not None:
        c, _ = _lonaci_scope(dt.datetime.strptime(ddmmyyyy, "%d%m%Y").date().isoformat())
        codes = c

    faits, derniere_sortie = 0, 0
    for i in range(1, a.passages + 1):
        print(f"\n--- passage {i}/{a.passages} de la serie "
              f"({dt.datetime.now(dt.timezone.utc):%H:%M:%S} UTC)")
        # Chaque passage est un cmd_scan complet : meme code, meme registre
        # append-only, meme numerotation. La serie n'est qu'un ordonnanceur.
        derniere_sortie = cmd_scan(argparse.Namespace(
            date=a.date, max_courses=a.max_courses, lonaci=a.lonaci,
            # seul le dernier passage envoie : voir le commentaire de cmd_scan
            sans_email=(i < a.passages), email_si_changement=a.email_si_changement))
        faits += 1

        if i == a.passages:
            break
        reste = _reste_a_venir(ddmmyyyy, codes)
        if reste == 0:
            print(f"ARRET : plus aucune course a venir dans le perimetre apres "
                  f"{faits} passage(s). Le reste du budget n'est pas brule.")
            break
        if reste is not None:
            print(f"  {reste} course(s) encore a venir")
        prochain = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=a.intervalle)
        if prochain >= limite:
            print(f"ARRET : le passage suivant depasserait le budget "
                  f"({prochain:%H:%M} > {limite:%H:%M} UTC). {faits} passage(s) faits.")
            break
        print(f"  attente de {a.intervalle} min -> {prochain:%H:%M} UTC")
        time.sleep(a.intervalle * 60)

    print(f"\nSERIE TERMINEE : {faits} passage(s) en "
          f"{int((dt.datetime.now(dt.timezone.utc) - debut).total_seconds() / 60)} min")
    return derniere_sortie


# -------------------------------------------- les trois passages des Nationale

# Les fenetres sont LARGES et le marquage est « une fois par course et par phase ».
# C'est deliberé : le cron de GitHub abandonne la plupart des declenchements (mesure
# du 08/10 : deux executes sur vingt-quatre attendus). Viser un INSTANT precis, c'est
# viser quelque chose qu'on n'attrapera pas. Une fenetre de trente minutes, elle,
# attrape un declenchement.
PHASES = {
    # phase      : (minutes avant le depart, borne haute -> borne basse)
    "H-60": (75, 45),
    "H-20": (28, 12),
}
OUVERTURE_DEBUT = (8, 0)     # heure APEX (APEX_TIMEZONE), passage d'ouverture
OUVERTURE_FIN = (9, 30)


def _fichier_phases(day):
    # Dans le dossier des snapshots : c'est lui qui part en artefact et revient au
    # run suivant. Ailleurs, l'etat serait perdu et chaque run rejouerait tout.
    return os.path.join(SNAP, f"phases_{day}.json")


def _lire_phases(day):
    try:
        return json.load(open(_fichier_phases(day), encoding="utf-8")).get("faites", {})
    except (OSError, json.JSONDecodeError):
        return {}


def _marquer_phase(day, cle):
    faites = _lire_phases(day)
    faites[cle] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    try:
        os.makedirs(SNAP, exist_ok=True)
        json.dump(dict(jour=day, faites=faites),
                  open(_fichier_phases(day), "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
    except OSError as e:
        # Si l'etat ne s'ecrit pas, le prochain run REFERA la phase. C'est le bon
        # sens de l'erreur : un passage en double vaut mieux qu'un passage manque.
        print(f"etat des phases non ecrit ({e}) : la phase sera refaite.", file=sys.stderr)


def nationales_du_jour(depuis=None):
    """
    Les Nationale du jour, d'apres l'ETIQUETTE OFFICIELLE de LONACI
    (lg_Type_Course_ID contient « NATIONALE »), avec leur heure de depart.

    Les heures de la passerelle sont en UTC : verifie le 07/10/2026, elle donnait
    11:55 pour le Prix des Gobelins que le PMU donne a 13:55 Paris (UTC+2).
    Abidjan etant a UTC+0, le fuseau APEX coincide.

    Renvoie (liste, motif_si_vide). On ne DEVINE jamais trois courses : sans
    etiquette, le choix serait arbitraire et le mauvais choix est pire que rien.
    """
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from apex_turf_lonaci import marches_lonaci
    except ImportError as e:
        return [], f"apex_turf_lonaci introuvable : {e}"
    if depuis:
        try:
            d = json.load(open(depuis, encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            return [], f"dump illisible : {e}"
    else:
        try:
            from apex_turf_lonaci_render import via_passerelle
        except ImportError as e:
            return [], f"apex_turf_lonaci_render introuvable : {e}"
        d, m = via_passerelle()
        if d is None:
            return [], (f"passerelle LONACI injoignable ({m}). Elle ne repond pas "
                        f"depuis un conteneur claude.ai : lancer sur un runner.")
    out = []
    for c in marches_lonaci(d):
        if not c["nationale"]:
            continue
        dep = None
        if c["depart"]:
            try:
                dep = dt.datetime.strptime(c["depart"], "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=dt.timezone.utc)
            except ValueError:
                pass
        out.append(dict(code=c["code"], libelle=c["libelle"], depart=dep,
                        type_lonaci=c["type_lonaci"], paris=c["paris"],
                        hippodrome=c["hippodrome"]))
    if not out:
        return [], ("aucune course ne porte l'etiquette NATIONALE dans la passerelle. "
                    "Ne pas en designer trois au hasard.")
    return out, None


def cmd_nationale(a):
    """
    Les trois passages demandes sur les Nationale, et rien d'autre :

        OUVERTURE   une fois par jour, fenetre 08:00 -> 09:30 (heure APEX),
                    sur les TROIS Nationale ensemble
        H-60        une fois par course, fenetre H-75 -> H-45
        H-20        une fois par course, fenetre H-28 -> H-12

    A lancer SOUVENT (cron toutes les 10-15 min) : la commande decide elle-meme
    si une phase est due, et ne rejoue jamais une phase deja faite. Lancee hors
    de toute fenetre, elle ne fait rien et le dit — c'est une sortie normale.

    Chaque phase produit son propre digest, avec la phase dans le sujet. Les trois
    passages donnent exactement les deux comparaisons qui portent l'information en
    pari mutuel : H-60 contre l'ouverture (comment l'argent de la journee s'est
    forme) et H-20 contre H-60 (l'argent tardif).
    """
    day = (dt.datetime.strptime(a.date, "%d%m%Y").date().isoformat() if a.date
           else apex_day()[0])
    ddmmyyyy = a.date or dt.datetime.now(ZoneInfo(TZ)).strftime("%d%m%Y")
    maintenant = dt.datetime.now(dt.timezone.utc)
    local = maintenant.astimezone(ZoneInfo(TZ))

    courses, motif = nationales_du_jour(a.depuis)
    if motif:
        print(f"REFUS : {motif}", file=sys.stderr)
        return 2
    print(f"NATIONALE du {day} : {len(courses)} course(s)")
    for c in courses:
        h = c["depart"].strftime("%H:%M") if c["depart"] else "heure inconnue"
        reste = (round((c["depart"] - maintenant).total_seconds() / 60)
                 if c["depart"] else None)
        print(f"  {c['code']:<7} {h} UTC  "
              + (f"H{reste:+}min  " if reste is not None else "")
              + f"{str(c['libelle'])[:38]:<38} {len(c['paris'])} pari(s)")

    faites = _lire_phases(day)
    dues = []

    # --- phase d'ouverture : une seule, sur les trois courses
    if a.forcer == "OUVERTURE" or (
            not a.forcer and "OUVERTURE" not in faites
            and (OUVERTURE_DEBUT <= (local.hour, local.minute) <= OUVERTURE_FIN)):
        dues.append(("OUVERTURE", [c["code"] for c in courses]))

    # --- H-60 et H-20 : une fois par course
    for nom, (haut, bas) in PHASES.items():
        for c in courses:
            cle = f"{nom}|{c['code']}"
            if cle in faites and a.forcer != nom:
                continue
            if a.forcer == nom:
                dues.append((nom, [c["code"]]))
                continue
            if not c["depart"]:
                continue
            reste = (c["depart"] - maintenant).total_seconds() / 60
            if bas <= reste <= haut:
                dues.append((nom, [c["code"]]))

    if not dues:
        print()
        print("AUCUNE phase due maintenant. Sortie normale.")
        print(f"  heure APEX : {local:%H:%M} ({TZ})")
        print(f"  deja faites : {', '.join(sorted(faites)) or 'aucune'}")
        return 0

    sortie = 0
    for nom, codes in dues:
        print()
        print(f"=== PHASE {nom} sur {', '.join(codes)}")
        r = cmd_scan(argparse.Namespace(
            date=a.date, max_courses=None, lonaci=a.lonaci,
            codes=",".join(codes), phase=nom,
            sans_email=False, email_si_changement=False))
        sortie = r or sortie
        for code in codes:
            _marquer_phase(day, "OUVERTURE" if nom == "OUVERTURE" else f"{nom}|{code}")
    return sortie


# --------------------------------------------------------------------- rapport

def write_report(day, snaps, npass):
    os.makedirs(REPO, exist_ok=True)
    f = os.path.join(REPO, f"{day}.md")
    ordre = {"SURVEILLER_FORT": 0, "SURVEILLER": 1, "PREMIER_PASSAGE": 2,
             "RIEN_A_SIGNALER": 3, "HORS_PERIMETRE": 4}
    s = sorted(snaps, key=lambda x: (ordre.get(x["decision"], 9), -(x["score_global"] or 0)))
    L = [f"# APEX-TURF-WORM — journee APEX {day}", "",
         f"Passage **{npass}** · relevé {dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}"
         f" · fuseau `{TZ}` · {len(snaps)} courses",
         ""]
    lon = next((x.get("lonaci") for x in snaps if x.get("lonaci")), None)
    if lon:
        r = lon.get("repartition") or {}
        L += [f"> **Périmètre restreint au programme officiel PMU LONACI** du "
              f"{lon.get('journee')} : {lon.get('n_lonaci')} courses au programme, dont "
              f"{r.get('ANALYSABLE', 0)} analysables (trot attelé), "
              f"{r.get('INDICATIF', 0)} en indicatif plafonné, "
              f"{r.get('REFUSE_PLAT', 0)} refusées (plat), "
              f"{r.get('ABSENT_SOURCE', 0)} absentes de la source (Nationale 3 marocaine).",
              f"> Cotes : **{lon.get('cotes_origine')}**. {lon.get('avertissement_masse')}",
              ""]
    L += [
         "> Le palier maximal de ce scanner est **SURVEILLER**. Les deux gates de pari turf",
         "> sont fermées par le backtest (trot ROI −4,58 % sur 118 paris, obstacle −89,05 %",
         "> sur 21). `autorite_pari = false` sur chaque ligne.", ""]
    hp = [x for x in s if x["decision"] == "HORS_PERIMETRE"]
    if hp:
        L += [f"**{len(hp)} courses hors périmètre** (aucun moteur calibré, dont le plat) : "
              + ", ".join(x["course"]["course_id"] for x in hp[:14])
              + (" …" if len(hp) > 14 else ""), ""]
    tops = [x for x in s if x["decision"].startswith("SURVEILLER")]
    L += ["## Signaux", ""]
    if not tops:
        L += ["Aucun signal au-dessus de 45/100 sur ce passage. C'est une sortie valide.", ""]
    for x in tops[:20]:
        c, m = x["course"], x["marche"]
        L += [f"### {c['course_id']} — {c['libelle']} ({c['hippodrome']})",
              f"- {c['discipline']} {c['distance']} m · {m['n_partants']} partants · "
              f"départ {c['depart_utc']} (H{x['minutes_avant_depart']:+} min)" if x['minutes_avant_depart'] is not None
              else f"- {c['discipline']} {c['distance']} m · {m['n_partants']} partants",
              f"- **{x['decision']}** · anomalie dominante `{x['signal_dominant']}` "
              f"{x['score_global']}/100 · {x['motif']}",
              f"- overround {m['overround']:.3f}" if m.get("overround") else "- overround UNAVAILABLE"]
        d = x["moteurs"]["derive"]
        if d.get("resserrements"):
            L.append("- resserrements : " + ", ".join(
                f"#{r['num']} {r['nom']} {r['avant']}→{r['apres']} ({r['variation_pct']:+.1f} %)"
                for r in d["resserrements"][:4]))
        if d.get("derives"):
            L.append("- dérives : " + ", ".join(
                f"#{r['num']} {r['nom']} {r['avant']}→{r['apres']} ({r['variation_pct']:+.1f} %)"
                for r in d["derives"][:4]))
        np_ = x["moteurs"]["non_partants"]
        if np_.get("nouveaux_retraits"):
            L.append(f"- **nouveaux non-partants** : "
                     + ", ".join(f"#{n}" for n in np_["nouveaux_retraits"]))
        nt = x["contexte"]["non_terminaison"]
        if nt.get("attendu_non_finissants") is not None:
            L.append(f"- non-terminaisons attendues {nt['attendu_non_finissants']} "
                     f"(taux de base {nt['taux_base']:.1%}, tranche {nt['tranche_champ']}"
                     + (", **champ à risque**" if nt.get("champ_a_risque") else "") + ")")
        lc = x.get("lonaci_course")
        if lc and lc.get("statut") == "INDICATIF":
            L.append(f"- ⚠ **LONACI {lc['statut']}** — {lc.get('motif','')}")
        elif lc:
            L.append(f"- LONACI : `{lc['statut']}`")
        L.append("")
    L += ["## Moteurs structurellement indisponibles", "",
          "| Moteur | Statut | Motif |", "|---|---|---|"]
    for k, v in engines_structurellement_absents().items():
        L.append(f"| `{k}` | `{v['provenance']}` | {v['motif']} |")
    L += ["", "*Jamais estimés. Une case nommée `UNAVAILABLE_STRUCTUREL` ne se remplit pas "
          "un jour par une approximation ; une case vide, si.*", ""]
    open(f, "w", encoding="utf-8").write("\n".join(L) + "\n")
    print(f"REPORT : {f}")
    return f


def cmd_report(a):
    day = a.date or apex_day()[0]
    hist = read_snapshots(day)
    if not hist:
        print(f"aucun snapshot pour {day}", file=sys.stderr)
        return 1
    npass = max(h["passage"] for h in hist)
    last = {h["course"]["course_id"]: h for h in hist if h["passage"] == npass}
    write_report(day, list(last.values()), npass)
    return 0


def cmd_bilan(a):
    """Bilan de fin de journee : confronte chaque signal a l'arrivee reelle."""
    day = a.date or apex_day()[0]
    hist = read_snapshots(day)
    if not hist:
        print(f"aucun snapshot pour {day}", file=sys.stderr)
        return 1
    ids = {}
    for h in hist:
        ids.setdefault(h["course"]["course_id"], []).append(h)
    print(f"journee {day} : {len(ids)} courses, {len(hist)} relevés, "
          f"{max(h['passage'] for h in hist)} passages")
    sig = [v[-1] for v in ids.values() if v[-1]["decision"].startswith("SURVEILLER")]
    print(f"signaux SURVEILLER au dernier passage : {len(sig)}")
    for s in sig:
        print(f"  {s['course']['course_id']:<22} {s['signal_dominant']:<16} "
              f"{s['score_global']:>3}/100  {s['course']['libelle'][:40]}")
    print("\nLe bilan ne verifie PAS une rentabilite : aucun pari n'est emis "
          "(autorite_pari=false).\nIl sert a mesurer si les anomalies signalees "
          "correspondent a quelque chose, sur un n qui\nreste a construire.")
    return 0


# ------------------------------------------------------------------- email

_CSS = ("font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:14px;"
        "line-height:1.5;color:#1a1a1a")


def build_email_html(day=None, phase=None):
    """
    Construit (sujet, html) pour le digest d'un passage.

    Meme contrat que apex_worm.build_email_html du WORM football, pour que la
    procedure d'envoi du CLAUDE.md s'applique mot pour mot : le SMTP n'etant
    pas configure, l'envoi reel passe par le connecteur Gmail en session.
    """
    day = day or apex_day()[0]
    hist = read_snapshots(day)
    if not hist:
        return (f"APEX-TURF-WORM {day} — aucun releve",
                f"<div style='{_CSS}'><p>Aucun snapshot pour {day}.</p></div>")
    npass = max(h["passage"] for h in hist)
    last = {h["course"]["course_id"]: h for h in hist if h["passage"] == npass}
    snaps = list(last.values())
    ordre = {"SURVEILLER_FORT": 0, "SURVEILLER": 1, "PREMIER_PASSAGE": 2,
             "RIEN_A_SIGNALER": 3, "HORS_PERIMETRE": 4}
    snaps.sort(key=lambda x: (ordre.get(x["decision"], 9), -(x["score_global"] or 0)))
    tops = [x for x in snaps if x["decision"].startswith("SURVEILLER")]
    hp = [x for x in snaps if x["decision"] == "HORS_PERIMETRE"]

    sujet = (f"APEX-TURF-WORM {day} · "
             + (f"{phase} · " if phase else f"passage {npass} · ")
             + f"{len(tops)} signal(aux) sur {len(snaps)} courses")

    h = [f"<div style='{_CSS}'>",
         f"<h2 style='margin:0 0 4px'>APEX-TURF-WORM — journée APEX {day}</h2>",
         f"<p style='color:#555;margin:0 0 14px'>"
         + (f"Phase <b>{phase}</b> · " if phase else "")
         + f"Passage <b>{npass}</b> · "
         f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} · "
         f"fuseau {TZ} · {len(snaps)} courses</p>",
         "<p style='background:#fff7e6;border-left:3px solid #d48806;padding:8px 10px;"
         "margin:0 0 16px'><b>Palier maximal : SURVEILLER.</b> Les deux gates de pari turf "
         "sont fermées par le backtest (trot ROI −4,58 % sur 118 paris, obstacle −89,05 % "
         "sur 21). <code>autorite_pari = false</code> sur chaque ligne.</p>"]

    if tops:
        h.append("<h3>Signaux</h3><table cellpadding='6' cellspacing='0' border='0' "
                 "style='border-collapse:collapse;width:100%'>")
        h.append("<tr style='background:#f0f0f0;text-align:left'><th>Course</th>"
                 "<th>Hippodrome</th><th>H-</th><th>Anomalie</th><th>Score</th>"
                 "<th>Décision</th></tr>")
        for x in tops[:25]:
            c = x["course"]
            m = x.get("minutes_avant_depart")
            h.append(
                f"<tr style='border-bottom:1px solid #e8e8e8'>"
                f"<td><b>{c['course_id']}</b><br><span style='color:#666'>"
                f"{(c['libelle'] or '')[:40]}</span></td>"
                f"<td>{c['hippodrome']}</td>"
                f"<td>{('H-' + str(m)) if m is not None else '—'}</td>"
                f"<td><code>{x['signal_dominant']}</code></td>"
                f"<td><b>{x['score_global']}</b>/100</td>"
                f"<td>{x['decision']}</td></tr>")
        h.append("</table>")
    else:
        h.append("<p>Aucun signal au-dessus de 45/100 sur ce passage. "
                 "<i>C'est une sortie valide.</i></p>")

    if hp:
        h.append(f"<p style='color:#666;margin-top:14px'><b>{len(hp)} course(s) hors "
                 f"périmètre</b> (aucun moteur calibré, dont le plat) : "
                 + ", ".join(x["course"]["course_id"] for x in hp[:12])
                 + (" …" if len(hp) > 12 else "") + "</p>")

    mi = os.path.join(REPO, f"{day}.mi.md")
    if os.path.exists(mi):
        txt = open(mi, encoding="utf-8").read()
        h.append("<h3 style='margin-top:20px'>APEX-TURF-MI — bruit de marché H-30</h3>")
        h.append("<pre style='background:#fafafa;border:1px solid #eee;padding:10px;"
                 "overflow-x:auto;font-size:12px'>"
                 + txt.replace("&", "&amp;").replace("<", "&lt;") + "</pre>")

    h.append("<h3 style='margin-top:20px'>Moteurs structurellement indisponibles</h3>"
             "<table cellpadding='5' cellspacing='0' style='border-collapse:collapse;"
             "font-size:13px'>")
    for k, v in engines_structurellement_absents().items():
        h.append(f"<tr><td><code>{k}</code></td><td style='color:#999'>{v['provenance']}"
                 f"</td><td style='color:#666'>{v['motif']}</td></tr>")
    h.append("</table><p style='color:#888;font-size:12px'><i>Jamais estimés. Une case "
             "nommée <code>UNAVAILABLE_STRUCTUREL</code> ne se remplit pas un jour par une "
             "approximation ; une case vide, si.</i></p></div>")
    return sujet, "\n".join(h)


def fragment_email_turf(day=None):
    """
    Fragment HTML du turf, a joindre au digest du WORM FOOTBALL.

    Volontairement NON INVASIF : ce fichier ne modifie pas tools/apex_worm.py.
    Pour brancher les deux digests, UNE SEULE ligne a ajouter dans
    apex_worm.build_email_html, juste avant la fermeture du corps :

        try:
            from apex_turf_worm import fragment_email_turf
            html += fragment_email_turf(day)
        except Exception:
            pass        # le turf absent ne doit jamais casser le digest football

Le `except` large est deliberé : un digest football qui echoue parce que le
turf manque serait pire que l'absence du turf.

    Retourne "" s'il n'y a rien a dire, pour ne pas polluer le digest d'une
    section vide.
    """
    day = day or apex_day()[0]
    hist = read_snapshots(day)
    if not hist:
        return ""
    npass = max(h["passage"] for h in hist)
    last = {h["course"]["course_id"]: h for h in hist if h["passage"] == npass}
    snaps = list(last.values())
    tops = sorted((x for x in snaps if x["decision"].startswith("SURVEILLER")),
                  key=lambda x: -(x["score_global"] or 0))
    lon = next((x.get("lonaci") for x in snaps if x.get("lonaci")), None)

    h = ["<hr style='margin:22px 0;border:0;border-top:1px solid #ddd'>",
         f"<div style='{_CSS}'>",
         "<h3 style='margin:0 0 4px'>APEX-TURF-WORM — courses</h3>",
         f"<p style='color:#555;margin:0 0 10px'>journée APEX {day} · passage {npass} · "
         f"{len(snaps)} course(s) · {len(tops)} signal(aux)</p>"]
    if lon:
        r = lon.get("repartition") or {}
        h.append(
            "<p style='background:#f6ffed;border-left:3px solid #52c41a;padding:7px 10px;"
            f"margin:0 0 10px;font-size:13px'><b>Périmètre PMU LONACI</b> du "
            f"{lon.get('journee')} : {lon.get('n_lonaci')} courses au programme, dont "
            f"<b>{r.get('ANALYSABLE', 0)}</b> analysables (trot attelé), "
            f"{r.get('INDICATIF', 0)} en indicatif plafonné, "
            f"{r.get('REFUSE_PLAT', 0)} refusées (plat), "
            f"{r.get('ABSENT_SOURCE', 0)} absentes de la source (Nationale 3 marocaine)."
            f"<br>Cotes : <b>{lon.get('cotes_origine')}</b> — masse d'enjeux LONACI non "
            "vérifiée.</p>")
    if tops:
        h.append("<table cellpadding='5' cellspacing='0' style='border-collapse:collapse;"
                 "width:100%;font-size:13px'><tr style='background:#f0f0f0;text-align:left'>"
                 "<th>Course</th><th>Hippodrome</th><th>H-</th><th>Anomalie</th>"
                 "<th>Score</th></tr>")
        for x in tops[:10]:
            c = x["course"]
            m = x.get("minutes_avant_depart")
            h.append(f"<tr style='border-bottom:1px solid #eee'><td><b>{c['course_id']}</b>"
                     f"<br><span style='color:#666'>{(c['libelle'] or '')[:34]}</span></td>"
                     f"<td>{c['hippodrome']}</td>"
                     f"<td>{('H-' + str(m)) if m is not None else '—'}</td>"
                     f"<td><code>{x['signal_dominant']}</code></td>"
                     f"<td><b>{x['score_global']}</b>/100</td></tr>")
        h.append("</table>")
    else:
        h.append("<p>Aucun signal au-dessus de 45/100. <i>Sortie valide.</i></p>")
    h.append("<p style='color:#888;font-size:12px'>Palier maximal <b>SURVEILLER</b> : les "
             "deux gates de pari turf sont fermées par le backtest. "
             "<code>autorite_pari = false</code>.</p></div>")
    return "\n".join(h)


def envoyer_smtp(sujet, html, dest=None):
    """
    Envoi autonome par SMTP, sans modele et sans connecteur.

    C'est la voie que le CLAUDE.md du depot nomme pour un passage declenche par
    cron ou par une Routine a session fraiche : le connecteur Gmail n'y est pas
    disponible, donc seul le SMTP permet de respecter la regle « un passage sans
    email envoye est INCOMPLET ».

    Variables d'environnement (les memes que le WORM football) :
        WORM_SMTP_HOST   obligatoire
        WORM_SMTP_PORT   defaut 587
        WORM_SMTP_USER   obligatoire
        WORM_SMTP_PASS   obligatoire
        WORM_EMAIL_TO    obligatoire si `dest` n'est pas fourni
        WORM_EMAIL_FROM  defaut WORM_SMTP_USER

    Retourne (True, detail) ou (False, motif). Ne leve jamais : un envoi rate
    ne doit pas faire perdre le snapshot du passage.
    """
    host = os.environ.get("WORM_SMTP_HOST")
    user = os.environ.get("WORM_SMTP_USER")
    pwd = os.environ.get("WORM_SMTP_PASS")
    to = dest or os.environ.get("WORM_EMAIL_TO")
    if not (host and user and pwd and to):
        manque = [k for k, v in (("WORM_SMTP_HOST", host), ("WORM_SMTP_USER", user),
                                 ("WORM_SMTP_PASS", pwd), ("WORM_EMAIL_TO", to)) if not v]
        return False, f"non configure : {', '.join(manque)}"
    port = int(os.environ.get("WORM_SMTP_PORT") or 587)
    expediteur = os.environ.get("WORM_EMAIL_FROM") or user
    try:
        import smtplib, ssl
        from email.message import EmailMessage
        m = EmailMessage()
        m["Subject"] = sujet
        m["From"] = expediteur
        m["To"] = to
        m.set_content("Digest APEX-TURF-WORM — version HTML requise.\n")
        m.add_alternative(html, subtype="html")
        ctx = ssl.create_default_context()
        if port == 465:
            with smtplib.SMTP_SSL(host, port, context=ctx, timeout=45) as sv:
                sv.login(user, pwd)
                sv.send_message(m)
        else:
            with smtplib.SMTP(host, port, timeout=45) as sv:
                sv.ehlo()
                sv.starttls(context=ctx)   # jamais en clair
                sv.login(user, pwd)
                sv.send_message(m)
        return True, f"{expediteur} -> {to} via {host}:{port}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def cmd_email(a):
    day = a.date or apex_day()[0]
    sujet, html = build_email_html(day, phase=getattr(a, "phase", None))
    os.makedirs(REPO, exist_ok=True)
    ph = os.path.join(REPO, f"{day}.email.html")
    ps = os.path.join(REPO, f"{day}.email.subject.txt")
    open(ph, "w", encoding="utf-8").write(html)
    open(ps, "w", encoding="utf-8").write(sujet + "\n")
    print(f"SUBJECT: {sujet}")
    print(f"html    -> {ph}")
    print(f"sujet   -> {ps}")
    print()
    ok, detail = envoyer_smtp(sujet, html, dest=getattr(a, "to", None))
    print("=" * 68)
    if ok:
        print(f"EMAIL ENVOYE par SMTP : {detail}")
        print("Passage complet.")
    else:
        print("ENVOI EMAIL OBLIGATOIRE — un passage sans email envoye est INCOMPLET.")
        print(f"SMTP indisponible ({detail}) : ce script n'a RIEN envoye.")
        print("Deux voies pour completer le passage :")
        print("  1. en session, avec le connecteur Gmail :")
        print("       mcp__Gmail__send_message")
        print('       to=["okoma.leonce@gmail.com"]')
        print(f"       subject  = <{os.path.basename(ps)}>")
        print(f"       htmlBody = <{os.path.basename(ph)}>")
        print("     preuve d'envoi = l'id/threadId Gmail renvoye par l'outil.")
        print("  2. sans modele ni connecteur, renseigner dans l'environnement :")
        print("       WORM_SMTP_HOST, WORM_SMTP_USER, WORM_SMTP_PASS, WORM_EMAIL_TO")
        print("       (+ WORM_SMTP_PORT, defaut 587 ; WORM_EMAIL_FROM, defaut l'utilisateur)")
        print("     ce script enverra alors seul a chaque passage.")
    print("=" * 68)
    return 0 if ok else 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="apex_turf_worm", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("window")
    s = sp.add_parser("scan")
    s.add_argument("--date", help="DDMMYYYY (defaut : aujourd'hui dans APEX_TIMEZONE)")
    s.add_argument("--max-courses", type=int)
    s.add_argument("--lonaci", action="store_true",
                   help="restreindre au programme officiel PMU LONACI du jour")
    s.add_argument("--sans-email", action="store_true",
                   help="sceller le releve sans envoyer de digest")
    s.add_argument("--email-si-changement", action="store_true",
                   help="n'envoyer que si l'ensemble des signaux a bouge")

    se = sp.add_parser("serie", help="plusieurs passages dans un seul run")
    se.add_argument("--passages", type=int, default=6)
    se.add_argument("--intervalle", type=int, default=15, help="minutes entre deux passages")
    se.add_argument("--budget-min", type=int, default=110,
                    help="duree maximale de la serie, en minutes")
    se.add_argument("--date", help="DDMMYYYY")
    se.add_argument("--max-courses", type=int)
    se.add_argument("--lonaci", action="store_true")
    se.add_argument("--email-si-changement", action="store_true")

    na = sp.add_parser("nationale",
                       help="les 3 passages (ouverture, H-60, H-20) sur les Nationale")
    na.add_argument("--date", help="DDMMYYYY")
    na.add_argument("--lonaci", action="store_true",
                    help="croiser avec le perimetre LONACI des moteurs")
    na.add_argument("--depuis", help="dump JSON de la passerelle, au lieu de l'appeler")
    na.add_argument("--forcer", choices=["OUVERTURE", "H-60", "H-20"],
                    help="jouer cette phase quoi qu'il arrive (diagnostic)")
    r = sp.add_parser("report"); r.add_argument("--date", help="YYYY-MM-DD")
    b = sp.add_parser("bilan");  b.add_argument("--date", help="YYYY-MM-DD")
    e = sp.add_parser("email")
    e.add_argument("--date", help="YYYY-MM-DD")
    e.add_argument("--to", help="destinataire (defaut : WORM_EMAIL_TO)")
    a = p.parse_args(argv)
    return {"window": cmd_window, "scan": cmd_scan, "serie": cmd_serie,
            "nationale": cmd_nationale,
            "report": cmd_report, "bilan": cmd_bilan,
            "email": cmd_email}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
