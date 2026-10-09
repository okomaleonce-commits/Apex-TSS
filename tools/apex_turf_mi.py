#!/usr/bin/env python3
"""
APEX-TURF-MI — Market & Behavioral Intelligence hippique.

Equivalent turf de tools/apex_mi.py. Capte le BRUIT de marche et le contexte
comportemental observable AVANT le depart. Cette cellule NE PRICE PAS et
N'EMET JAMAIS un pari : bet_authority=false, requires_statistical_convergence=true.

Ce que le pari mutuel retire, et ce qu'il donne a la place
----------------------------------------------------------
Le PMU n'a qu'UNE cote. Disparaissent donc, structurellement :
SHARP_MOVE, STEAM_MOVE, RLM, BOOKMAKER_DIVERGENCE, LIQUIDITY_SPIKE.
Ils sortent UNAVAILABLE_STRUCTUREL — pas estimes, pas omis.

Apparaissent en echange, propres au turf et tous PUBLIES par la source officielle :
NON_PARTANT, DRIVER_CHANGE, DEFERRE_CHANGE, MARKET_FLIP. Ils bougent l'argent
plus fort qu'un sharp move de football, et ils sont OBSERVED, pas inferes.

    python3 tools/apex_turf_mi.py window
    python3 tools/apex_turf_mi.py init --date DDMMYYYY [--within 180]
    python3 tools/apex_turf_mi.py oddsflow --course-dir runs_turf_mi/<run>/<course_id>
    python3 tools/apex_turf_mi.py signal     --course-dir ... --family ... --source-tier ... --url ...
    python3 tools/apex_turf_mi.py behavioral --course-dir ... --index ... --kind ...
    python3 tools/apex_turf_mi.py check  --course-dir ...
    python3 tools/apex_turf_mi.py score  --course-dir ...
    python3 tools/apex_turf_mi.py finalize --run runs_turf_mi/<run>
    python3 tools/apex_turf_mi.py email    --run runs_turf_mi/<run>
    python3 tools/apex_turf_mi.py worm-hook --day YYYY-MM-DD --within 30
        -> trois axes : OUTSIDER_WATCH, FAVORI_WATCH (miroirs du pont football)
           et NON_PARTANT_WATCH (propre au turf)

Stdlib uniquement.
"""
import argparse, json, os, sys, glob
import datetime as dt
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNS = os.path.join(ROOT, "runs_turf_mi")
WORM_SNAP = os.path.join(ROOT, "data", "turf_worm", "snapshots")
WORM_MI = os.path.join(ROOT, "data", "turf_worm", "mi")
TZ = os.environ.get("APEX_TIMEZONE", "UTC")

UNAVAILABLE = "UNAVAILABLE"
UNAVAILABLE_STRUCTUREL = "UNAVAILABLE_STRUCTUREL"

# Hierarchie des sources. Un communique du PMU et un post de forum n'ont jamais
# le meme poids. Mentir sur le tier fausse tout l'essaim.
TIERS = {"pmu_officiel": 100, "officiel_societe": 95, "entourage_identifie": 75,
         "presse_specialisee": 65, "agregateur": 45, "reseau_social": 30,
         "forum": 20, "tipster": 12}

# Familles de signal marche REELLEMENT observables en pari mutuel.
FAMILLES_MARCHE = {
    "PRICE_COMPRESSION": "la cote se resserre entre deux relevés",
    "PRICE_DRIFT": "la cote derive",
    "MARKET_FLIP": "changement de favori",
    "FAVORI_CONTESTE": "deux partants a cote quasi identique en tete",
    "EARLY_MONEY": "mouvement loin du depart",
    "LATE_MONEY": "mouvement dans les 30 dernieres minutes",
    "NON_PARTANT": "retrait d'un partant — redistribue tout l'argent",
    "DRIVER_CHANGE": "changement de driver ou de jockey",
    "DEFERRE_CHANGE": "changement de ferrure (deferre anterieur/posterieur)",
}
FAMILLES_ABSENTES = {
    "SHARP_MOVE": "pari mutuel : ni Pinnacle ni book asiatique, une seule cote",
    "STEAM_MOVE": "pas de books multiples a synchroniser",
    "RLM": "le PMU ne publie pas le pourcentage de parieurs par partant",
    "BOOKMAKER_DIVERGENCE": "une seule cote, aucune divergence a mesurer",
    "LIQUIDITY_SPIKE": "masse des enjeux par partant non publiee en direct",
}

# Indices comportementaux turf. Jamais un etat mental : un fait source, ou rien.
INDICES_COMPORTEMENTAUX = {
    "FRAICHEUR_INDEX": "cheval — jours depuis la derniere course, nombre de courses recentes",
    "DRIVER_HABITUEL": "driver — pilote habituel du cheval ou remplacant",
    "ECURIE_INTENT": "entraineur — plusieurs partants de la meme ecurie dans la course",
    "ENGAGEMENT_INDEX": "proprietaire — supplement paye, engagement tardif",
    "PISTE_CONTEXT": "hippodrome — corde, autostart, penetrometre",
    "NARRATIVE_STRENGTH": "marche/medias — recit deja price",
}

# Vagues temporelles. Plus courtes qu'au football : en pari mutuel l'argent
# decisif arrive dans le dernier quart d'heure.
VAGUES = [("EARLY", 24 * 60, 180), ("INFORMATION", 180, 30), ("LATE", 30, 0)]

# Pertinence de timing par famille et par vague. Une LINEUP_SHOCK football en LATE
# vaut 100 ; ici c'est NON_PARTANT et DRIVER_CHANGE qui valent 100 en LATE.
TIMING = {
    "NON_PARTANT":      {"EARLY": 55, "INFORMATION": 85, "LATE": 100},
    "DRIVER_CHANGE":    {"EARLY": 50, "INFORMATION": 80, "LATE": 100},
    "DEFERRE_CHANGE":   {"EARLY": 45, "INFORMATION": 75, "LATE": 90},
    "LATE_MONEY":       {"EARLY": 10, "INFORMATION": 45, "LATE": 95},
    "EARLY_MONEY":      {"EARLY": 80, "INFORMATION": 40, "LATE": 15},
    "PRICE_COMPRESSION":{"EARLY": 55, "INFORMATION": 75, "LATE": 85},
    "PRICE_DRIFT":      {"EARLY": 55, "INFORMATION": 70, "LATE": 80},
    "MARKET_FLIP":      {"EARLY": 40, "INFORMATION": 70, "LATE": 90},
    "FAVORI_CONTESTE":  {"EARLY": 60, "INFORMATION": 65, "LATE": 70},
}
BANDES = [(90, "anomalie majeure"), (75, "signal fort"), (60, "signal credible"),
          (40, "information a surveiller"), (0, "bruit faible / non exploitable")]


def vague(minutes_avant):
    if minutes_avant is None:
        return None
    for nom, hi, lo in VAGUES:
        if lo <= minutes_avant <= hi:
            return nom
    return "EARLY" if minutes_avant > 24 * 60 else "LATE"


def bande(score):
    return next(lab for seuil, lab in BANDES if score >= seuil)


def get(url, tries=3):
    import urllib.request, urllib.error, time
    for i in range(tries):
        try:
            r = urllib.request.Request(url, headers={"User-Agent": "APEX-TURF-MI/1.0"})
            with urllib.request.urlopen(r, timeout=30) as f:
                return json.load(f)
        except Exception as e:
            if i == tries - 1:
                return {"_erreur": f"{type(e).__name__}: {e}"}
            time.sleep(2 ** i)


def jload(p, d=None):
    try:
        return json.load(open(p, encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return d


def jdump(p, o):
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(o, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def append(p, o):
    """Append-only : plusieurs agents ecrivent le meme dossier, aucun n'ecrase."""
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(o, ensure_ascii=False) + "\n")


# ------------------------------------------------------------------- commandes

def cmd_window(a):
    now = dt.datetime.now(ZoneInfo(TZ))
    print(f"fuseau APEX : {TZ}\nmaintenant  : {now.isoformat(timespec='seconds')}\n")
    print("vagues temporelles (plus courtes qu'au football : en pari mutuel")
    print("l'argent decisif arrive dans le dernier quart d'heure)\n")
    for nom, hi, lo in VAGUES:
        print(f"  {nom:<12} T-{hi:>4} min  ->  T-{lo:<4} min")
    print("\nfamilles de signal disponibles :")
    for k, v in FAMILLES_MARCHE.items():
        print(f"  {k:<20} {v}")
    print("\nfamilles STRUCTURELLEMENT indisponibles (jamais estimees) :")
    for k, v in FAMILLES_ABSENTES.items():
        print(f"  {k:<20} {v}")
    return 0


def cmd_init(a):
    """Resout les courses de la fenetre et cree les dossiers de run."""
    ddmmyyyy = a.date or dt.datetime.now(ZoneInfo(TZ)).strftime("%d%m%Y")
    p = get(f"https://online.turfinfo.api.pmu.fr/rest/client/1/programme/{ddmmyyyy}")
    if "_erreur" in p:
        print(f"init a echoue : {p['_erreur']}", file=sys.stderr)
        return 1
    now = dt.datetime.now(dt.timezone.utc)
    run = os.path.join(RUNS, now.strftime("%Y%m%d-%H%M"))
    sel = []
    for r in (p.get("programme", {}).get("reunions") or []):
        for c in (r.get("courses") or []):
            if not c.get("heureDepart"):
                continue
            dep = dt.datetime.fromtimestamp(c["heureDepart"] / 1000, dt.timezone.utc)
            mins = round((dep - now).total_seconds() / 60)
            if not (0 <= mins <= (a.within or 180)):
                continue
            cid = f"{ddmmyyyy}-R{r['numOfficiel']}C{c['numExterne']}"
            sel.append(dict(course_id=cid, libelle=c.get("libelle"),
                            hippodrome=(r.get("hippodrome") or {}).get("libelleCourt"),
                            discipline=c.get("specialite"), distance=c.get("distance"),
                            depart_utc=dep.isoformat(timespec="seconds"),
                            minutes_avant_depart=mins, vague=vague(mins),
                            declares=c.get("nombreDeclaresPartants")))
    if not sel:
        print(f"EMPTY : aucune course dans les {a.within or 180} prochaines minutes.")
        print("STOP — ne devine pas une course. Demande une course precise.")
        return 2
    for c in sel:
        jdump(os.path.join(run, c["course_id"], "00_course.json"), c)
    jdump(os.path.join(run, "run.json"),
          dict(run=os.path.basename(run), cree=now.isoformat(timespec="seconds"),
               fenetre_min=a.within or 180, courses=[c["course_id"] for c in sel]))
    print(f"run {os.path.basename(run)} — {len(sel)} courses")
    for c in sel:
        print(f"  {c['course_id']:<22} {c['vague']:<12} H-{c['minutes_avant_depart']:<4} "
              f"{(c['libelle'] or '')[:36]}")
    print(f"\n-> {run}")
    return 0


def cmd_oddsflow(a):
    """
    Metriques mecaniques, lues sur les snapshots APEX-TURF-WORM.
    Les agents marche lisent CE fichier avant d'enregistrer un signal : ils ne
    recalculent pas et n'inventent pas une cote.
    """
    d = a.course_dir
    c = jload(os.path.join(d, "00_course.json"))
    if not c:
        print("00_course.json absent — lancer init d'abord", file=sys.stderr)
        return 1
    day = None
    for f in sorted(glob.glob(os.path.join(WORM_SNAP, "*.jsonl")), reverse=True):
        for ln in open(f, encoding="utf-8"):
            ln = ln.strip()
            if ln and c["course_id"] in ln:
                day = f
                break
        if day:
            break
    if not day:
        out = dict(provenance=UNAVAILABLE,
                   motif=("aucun snapshot APEX-TURF-WORM ne couvre cette course. "
                          "Lancer `apex_turf_worm.py scan` au moins deux fois : une "
                          "trajectoire exige deux relevés."),
                   trajectoire=None, familles_absentes=FAMILLES_ABSENTES)
        jdump(os.path.join(d, "01_oddsflow.json"), out)
        print(out["motif"])
        return 0
    passes = []
    for ln in open(day, encoding="utf-8"):
        ln = ln.strip()
        if not ln:
            continue
        try:
            s = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if s["course"]["course_id"] == c["course_id"]:
            passes.append(s)
    if len(passes) < 2:
        out = dict(provenance=UNAVAILABLE, n_passages=len(passes),
                   motif="un seul relevé : aucune trajectoire calculable",
                   familles_absentes=FAMILLES_ABSENTES)
        jdump(os.path.join(d, "01_oddsflow.json"), out)
        print(out["motif"])
        return 0

    p0, p1 = passes[0], passes[-1]
    av = {x["num"]: x["cote"] for x in p0["marche"]["partants"] if x["cote"]}
    ap = {x["num"]: x["cote"] for x in p1["marche"]["partants"] if x["cote"]}
    noms = {x["num"]: x["nom"] for x in p1["marche"]["partants"]}
    traj = [dict(num=n, nom=noms.get(n), avant=av[n], apres=ap[n],
                 variation_pct=round((ap[n] - av[n]) / av[n] * 100, 1))
            for n in sorted(set(av) & set(ap))]
    fav_av = min(av, key=av.get) if av else None
    fav_ap = min(ap, key=ap.get) if ap else None
    familles = []
    if fav_av != fav_ap:
        familles.append("MARKET_FLIP")
    if traj and min(t["variation_pct"] for t in traj) <= -18:
        familles.append("PRICE_COMPRESSION")
    if traj and max(t["variation_pct"] for t in traj) >= 18:
        familles.append("PRICE_DRIFT")
    nouveaux = (set(x["num"] for x in p1["marche"]["nonpartants"])
                - set(x["num"] for x in p0["marche"]["nonpartants"]))
    if nouveaux:
        familles.append("NON_PARTANT")
    if c.get("vague") == "LATE" and familles:
        familles.append("LATE_MONEY")
    elif c.get("vague") == "EARLY" and familles:
        familles.append("EARLY_MONEY")
    o = sorted(ap.items(), key=lambda kv: kv[1])
    if len(o) >= 2 and o[1][1] / o[0][1] < 1.12:
        familles.append("FAVORI_CONTESTE")

    out = dict(provenance="OBSERVED", source="data/turf_worm/snapshots",
               n_passages=len(passes),
               premier_releve=p0["releve_utc"], dernier_releve=p1["releve_utc"],
               favori_avant=fav_av, favori_apres=fav_ap,
               overround=p1["marche"].get("overround"),
               nouveaux_non_partants=sorted(nouveaux),
               familles_detectees=sorted(set(familles)),
               trajectoire=sorted(traj, key=lambda t: t["variation_pct"]),
               familles_absentes=FAMILLES_ABSENTES)
    jdump(os.path.join(d, "01_oddsflow.json"), out)
    print(f"{c['course_id']} — {len(passes)} relevés, familles : "
          f"{', '.join(out['familles_detectees']) or 'aucune'}")
    for t in out["trajectoire"][:6]:
        print(f"  #{t['num']:<3} {str(t['nom'])[:20]:<20} {t['avant']:>6} -> {t['apres']:>6} "
              f"{t['variation_pct']:+.1f} %")
    return 0


def _score4(family, tier, vg, confirmations, impact):
    """
    Quatre dimensions 0-100. Le MOTEUR score ; l'agent n'apporte qu'une observation.
    Un agent qui choisirait lui-meme son score ferait de l'essaim un vote d'opinion.
    """
    src = TIERS.get(tier, 0)
    tim = TIMING.get(family, {}).get(vg or "INFORMATION", 50)
    conf = min(100, 25 * max(0, confirmations))
    sig = round(0.35 * src + 0.25 * tim + 0.20 * conf + 0.20 * impact)
    return dict(SOURCE_RELIABILITY=src, TIMING_RELEVANCE=tim,
                CROSS_SOURCE_CONFIRMATION=conf, MARKET_IMPACT=impact,
                MARKET_SIGNAL_SCORE=sig, bande=bande(sig))


def cmd_signal(a):
    if a.family in FAMILLES_ABSENTES:
        print(f"REFUS : {a.family} est structurellement indisponible en pari mutuel.\n"
              f"  motif : {FAMILLES_ABSENTES[a.family]}\n"
              f"  Enregistrer un tel signal serait l'inventer.", file=sys.stderr)
        return 1
    if a.family not in FAMILLES_MARCHE:
        print(f"famille inconnue : {a.family}\ndisponibles : "
              f"{', '.join(FAMILLES_MARCHE)}", file=sys.stderr)
        return 1
    if a.source_tier not in TIERS:
        print(f"tier inconnu : {a.source_tier}\ndisponibles : {', '.join(TIERS)}",
              file=sys.stderr)
        return 1
    if not a.url:
        print("REFUS : un signal de marche exige une --url datee. "
              "Sans source verifiable, c'est une rumeur.", file=sys.stderr)
        return 1
    c = jload(os.path.join(a.course_dir, "00_course.json")) or {}
    rec = dict(type="signal", agent=a.agent, family=a.family,
               source_tier=a.source_tier, url=a.url, note=a.note,
               confirmations=a.confirmations, market_impact=a.impact,
               vague=c.get("vague"), horodatage=dt.datetime.now(dt.timezone.utc)
               .isoformat(timespec="seconds"),
               scores=_score4(a.family, a.source_tier, c.get("vague"),
                              a.confirmations, a.impact))
    append(os.path.join(a.course_dir, "signaux.jsonl"), rec)
    s = rec["scores"]
    print(f"{a.family} enregistre — MARKET_SIGNAL_SCORE {s['MARKET_SIGNAL_SCORE']}/100 "
          f"({s['bande']})")
    return 0


def cmd_behavioral(a):
    if a.index not in INDICES_COMPORTEMENTAUX:
        print(f"indice inconnu : {a.index}\ndisponibles : "
              f"{', '.join(INDICES_COMPORTEMENTAUX)}", file=sys.stderr)
        return 1
    if a.kind in ("fact", "observation") and not a.url:
        print(f"REFUS : un --kind {a.kind} exige une --url datee.", file=sys.stderr)
        return 1
    c = jload(os.path.join(a.course_dir, "00_course.json")) or {}
    # une interpretation est plafonnee : elle ne peut pas peser comme un fait
    plafond = {"fact": 100, "observation": 75, "interpretation": 40}[a.kind]
    src = min(TIERS.get(a.source_tier, 0), plafond)
    rec = dict(type="behavioral", agent=a.agent, index=a.index, kind=a.kind,
               source_tier=a.source_tier, url=a.url, note=a.note,
               vague=c.get("vague"), poids=src, plafond_kind=plafond,
               horodatage=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))
    append(os.path.join(a.course_dir, "comportemental.jsonl"), rec)
    print(f"{a.index} ({a.kind}) enregistre — poids {src}/100 "
          f"(plafond du kind : {plafond})")
    return 0


def _read(p):
    if not os.path.exists(p):
        return []
    out = []
    for ln in open(p, encoding="utf-8"):
        ln = ln.strip()
        if ln:
            try:
                out.append(json.loads(ln))
            except json.JSONDecodeError:
                continue
    return out


def cmd_check(a):
    d = a.course_dir
    c = jload(os.path.join(d, "00_course.json")) or {}
    sg, bh = _read(os.path.join(d, "signaux.jsonl")), _read(os.path.join(d, "comportemental.jsonl"))
    of = jload(os.path.join(d, "01_oddsflow.json"))
    print(f"{c.get('course_id')} — {c.get('libelle')}")
    print(f"  vague           : {c.get('vague')} (H-{c.get('minutes_avant_depart')} min)")
    print(f"  oddsflow        : {'present' if of else 'ABSENT -> lancer oddsflow'}")
    print(f"  signaux marche  : {len(sg)} ({len({s['agent'] for s in sg})} agents)")
    print(f"  indices compo.  : {len(bh)} ({len({s['agent'] for s in bh})} agents)")
    if not of:
        print("  prochaine etape : oddsflow")
    elif not sg and not bh:
        print("  prochaine etape : lancer l'essaim (agents marche puis comportementaux)")
    else:
        print("  prochaine etape : score")
    return 0


def cmd_score(a):
    d = a.course_dir
    c = jload(os.path.join(d, "00_course.json")) or {}
    sg, bh = _read(os.path.join(d, "signaux.jsonl")), _read(os.path.join(d, "comportemental.jsonl"))
    of = jload(os.path.join(d, "01_oddsflow.json")) or {}

    # La valeur vient de la CONVERGENCE entre agents, jamais d'un signal isole.
    familles = {}
    for s in sg:
        familles.setdefault(s["family"], []).append(s)
    conv = {f: len({s["agent"] for s in v}) for f, v in familles.items()}
    sig = max((s["scores"]["MARKET_SIGNAL_SCORE"] for s in sg), default=0)
    dom = max(sg, key=lambda s: s["scores"]["MARKET_SIGNAL_SCORE"])["family"] if sg else None
    multi = max(conv.values(), default=0)
    sig_conv = min(100, round(sig * (1 + 0.08 * max(0, multi - 1))))

    etat = ("DISLOCATED" if of.get("nouveaux_non_partants") or "MARKET_FLIP" in
            (of.get("familles_detectees") or [])
            else "ACTIVE" if of.get("familles_detectees") else "CALM")
    jdump(os.path.join(d, "90_market_synthesis.json"), dict(
        course=c.get("course_id"), etat_marche=etat, signal_dominant=dom,
        MARKET_SIGNAL_SCORE=sig_conv, bande=bande(sig_conv),
        convergence_par_famille=conv, n_signaux=len(sg),
        oddsflow_familles=of.get("familles_detectees"),
        familles_absentes=FAMILLES_ABSENTES))

    bsc = max((x["poids"] for x in bh), default=0)
    faits = sum(1 for x in bh if x["kind"] == "fact")
    jdump(os.path.join(d, "91_behavioral.json"), dict(
        course=c.get("course_id"), BEHAVIORAL_SIGNAL=bsc, bande=bande(bsc),
        n_indices=len(bh), n_faits=faits,
        indices=[dict(index=x["index"], kind=x["kind"], poids=x["poids"]) for x in bh]))

    # Regle d'integration NON NEGOCIABLE
    has_m, has_b = bool(sg), bool(bh)
    statut = ("CANDIDATE" if has_m and has_b else "WATCH" if has_b or has_m else "RIEN")
    jdump(os.path.join(d, "92_integration.json"), dict(
        course=c.get("course_id"), statut=statut,
        bet_authority=False, requires_statistical_convergence=True,
        regle=("BEHAVIORAL seul -> WATCH ; BEHAVIORAL + MARKET -> CANDIDATE ; "
               "+ DATA (cellule statistique, hors de cette cellule) -> CONFIRMED"),
        brique_data="ABSENTE de cette cellule — fournie par apex-turf-team",
        MARKET_SIGNAL_SCORE=sig_conv, BEHAVIORAL_SIGNAL=bsc))
    print(f"{c.get('course_id')} — marche {etat} · MARKET_SIGNAL_SCORE {sig_conv}/100 "
          f"({bande(sig_conv)}) · comportemental {bsc}/100 · integration {statut} "
          f"· bet_authority=False")
    return 0


def cmd_finalize(a):
    run = a.run
    cs = sorted(d for d in glob.glob(os.path.join(run, "*")) if os.path.isdir(d))
    L = [f"# APEX-TURF-MI — synthese du run {os.path.basename(run)}", "",
         f"{len(cs)} courses · "
         f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} · fuseau `{TZ}`", "",
         "> Cette cellule **ne price pas** et **n'emet jamais un pari** : "
         "`bet_authority = false`,", "> `requires_statistical_convergence = true`. "
         "Ses signaux alimentent `apex-turf-team`.", ""]
    n_cand = 0
    for d in cs:
        c = jload(os.path.join(d, "00_course.json"))
        if not c:
            continue
        m = jload(os.path.join(d, "90_market_synthesis.json")) or {}
        b = jload(os.path.join(d, "91_behavioral.json")) or {}
        i = jload(os.path.join(d, "92_integration.json")) or {}
        if i.get("statut") == "CANDIDATE":
            n_cand += 1
        L += [f"## {c['course_id']} — {c['libelle']} ({c['hippodrome']})",
              f"- {c['discipline']} {c['distance']} m · vague **{c['vague']}** "
              f"· H-{c['minutes_avant_depart']} min",
              f"- etat du marche : **{m.get('etat_marche','?')}** · signal dominant "
              f"`{m.get('signal_dominant')}` · MARKET_SIGNAL_SCORE "
              f"{m.get('MARKET_SIGNAL_SCORE',0)}/100 ({m.get('bande','-')})",
              f"- comportemental : {b.get('BEHAVIORAL_SIGNAL',0)}/100 "
              f"({b.get('n_indices',0)} indices, {b.get('n_faits',0)} faits sourcés)",
              f"- integration : **{i.get('statut','RIEN')}** · bet_authority=False"]
        if m.get("oddsflow_familles"):
            L.append(f"- familles détectées mécaniquement : "
                     + ", ".join(f"`{x}`" for x in m["oddsflow_familles"]))
        L.append("")
    L += ["## Familles structurellement indisponibles", "",
          "| Famille | Motif |", "|---|---|"]
    for k, v in FAMILLES_ABSENTES.items():
        L.append(f"| `{k}` | {v} |")
    L += ["", "*Le pari mutuel les rend impossibles à mesurer. Elles ne sont ni estimées "
          "ni omises : une case nommée se défend, une case vide se remplit un jour par "
          "une approximation.*", ""]
    p = os.path.join(run, "SYNTHESE.md")
    open(p, "w", encoding="utf-8").write("\n".join(L) + "\n")
    jrn = os.path.join(ROOT, "journal", "apex_turf_mi_journal.csv")
    os.makedirs(os.path.dirname(jrn), exist_ok=True)
    neuf = not os.path.exists(jrn)
    with open(jrn, "a", encoding="utf-8") as f:
        if neuf:
            f.write("run,course_id,vague,etat_marche,signal_dominant,market_score,"
                    "behavioral,integration,bet_authority\n")
        for d in cs:
            c = jload(os.path.join(d, "00_course.json"))
            if not c:
                continue
            m = jload(os.path.join(d, "90_market_synthesis.json")) or {}
            b = jload(os.path.join(d, "91_behavioral.json")) or {}
            i = jload(os.path.join(d, "92_integration.json")) or {}
            f.write(f"{os.path.basename(run)},{c['course_id']},{c['vague']},"
                    f"{m.get('etat_marche','')},{m.get('signal_dominant','')},"
                    f"{m.get('MARKET_SIGNAL_SCORE','')},{b.get('BEHAVIORAL_SIGNAL','')},"
                    f"{i.get('statut','')},false\n")
    print(f"SYNTHESE  : {p}\njournal   : {jrn} (append-only)\nCANDIDATE : {n_cand}/{len(cs)}")
    # Regle maison (CLAUDE.md) : finalize construit TOUJOURS le digest.
    cmd_email(argparse.Namespace(run=run))
    return 0


def _confirmation(mouvements, num):
    """
    Confirmation de mouvement : la cote de ce partant se raccourcit-elle ?
    Seul un RACCOURCISSEMENT compte comme confirmation — une derive en sens
    inverse n'est pas une confirmation faible, c'est une infirmation.
    Retourne (score 0-100, variation_pct) ou (None, None) si non apparie.
    """
    for m in (mouvements or []):
        if m["num"] == num:
            v = m["variation_pct"]
            return (min(100, round(abs(v) * 2.8)) if v < 0 else 0), v
    return None, None


def cmd_email(a):
    """
    Construit le digest du run. Meme contrat qu'apex_mi.py email : le script
    n'envoie rien (pas de SMTP configure), l'envoi reel passe par le
    connecteur Gmail en session, comme l'impose le CLAUDE.md du depot.
    """
    run = a.run
    cs = sorted(d for d in glob.glob(os.path.join(run, "*")) if os.path.isdir(d))
    rows, n_cand = [], 0
    for d in cs:
        c = jload(os.path.join(d, "00_course.json"))
        if not c:
            continue
        m = jload(os.path.join(d, "90_market_synthesis.json")) or {}
        b = jload(os.path.join(d, "91_behavioral.json")) or {}
        i = jload(os.path.join(d, "92_integration.json")) or {}
        if i.get("statut") == "CANDIDATE":
            n_cand += 1
        rows.append((c, m, b, i))
    sujet = (f"APEX-TURF-MI {os.path.basename(run)} · {len(rows)} course(s) · "
             f"{n_cand} CANDIDATE")
    css = ("font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:14px;"
           "line-height:1.5;color:#1a1a1a")
    h = [f"<div style='{css}'>",
         f"<h2 style='margin:0 0 4px'>APEX-TURF-MI — run {os.path.basename(run)}</h2>",
         f"<p style='color:#555;margin:0 0 14px'>{len(rows)} course(s) · "
         f"{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')} · "
         f"fuseau {TZ}</p>",
         "<p style='background:#e6f4ff;border-left:3px solid #1677ff;padding:8px 10px;"
         "margin:0 0 16px'>Cette cellule <b>ne price pas</b> et <b>n'émet jamais un "
         "pari</b> : <code>bet_authority = false</code>, "
         "<code>requires_statistical_convergence = true</code>. Le maximum atteignable "
         "ici est <b>CANDIDATE</b> — la brique DATA appartient à "
         "<code>apex-turf-team</code>.</p>"]
    if rows:
        h.append("<table cellpadding='6' cellspacing='0' style='border-collapse:collapse;"
                 "width:100%'><tr style='background:#f0f0f0;text-align:left'>"
                 "<th>Course</th><th>Vague</th><th>Marché</th><th>Signal dominant</th>"
                 "<th>MKT</th><th>COMP</th><th>Intégration</th></tr>")
        for c, m, b, i in rows:
            h.append(
                f"<tr style='border-bottom:1px solid #e8e8e8'>"
                f"<td><b>{c['course_id']}</b><br><span style='color:#666'>"
                f"{(c['libelle'] or '')[:36]} · {c['hippodrome']}</span></td>"
                f"<td>{c['vague']}<br><span style='color:#666'>H-"
                f"{c['minutes_avant_depart']}</span></td>"
                f"<td>{m.get('etat_marche','?')}</td>"
                f"<td><code>{m.get('signal_dominant') or '—'}</code></td>"
                f"<td><b>{m.get('MARKET_SIGNAL_SCORE',0)}</b></td>"
                f"<td>{b.get('BEHAVIORAL_SIGNAL',0)}<br><span style='color:#666'>"
                f"{b.get('n_faits',0)} fait(s)</span></td>"
                f"<td><b>{i.get('statut','RIEN')}</b></td></tr>")
        h.append("</table>")
    else:
        h.append("<p>Aucune course dans ce run.</p>")
    h.append("<h3 style='margin-top:20px'>Familles structurellement indisponibles</h3>"
             "<table cellpadding='5' cellspacing='0' style='border-collapse:collapse;"
             "font-size:13px'>")
    for k, v in FAMILLES_ABSENTES.items():
        h.append(f"<tr><td><code>{k}</code></td><td style='color:#666'>{v}</td></tr>")
    h.append("</table><p style='color:#888;font-size:12px'><i>Le pari mutuel les rend "
             "impossibles à mesurer. Ni estimées, ni omises.</i></p></div>")
    html = "\n".join(h)
    for nom, contenu in (("email.html", html),
                         ("email.subject.txt", sujet + "\n"),
                         ("email.txt", f"{sujet}\n\nVoir SYNTHESE.md du run "
                                       f"{os.path.basename(run)}.\n"
                                       f"bet_authority = false.\n")):
        open(os.path.join(run, nom), "w", encoding="utf-8").write(contenu)
    print(f"SUBJECT: {sujet}")
    print(f"-> {run}/email.html · email.txt · email.subject.txt")
    print()
    print("=" * 68)
    print("ENVOI EMAIL OBLIGATOIRE — un passage sans email envoye est INCOMPLET.")
    print("Enchainer en session avec mcp__Gmail__send_message :")
    print('  to=["okoma.leonce@gmail.com"]')
    print("  subject = email.subject.txt | htmlBody = email.html | body = email.txt")
    print("Preuve d'envoi = l'id/threadId Gmail renvoye par l'outil.")
    print("=" * 68)
    return 0


def cmd_worm_hook(a):
    """
    Activation H-30 depuis un scan APEX-TURF-WORM.

    Miroir du pont football, qui croise deux anomalies structurelles avec le
    mouvement du marche dans le meme sens :

        OUTSIDER_WATCH   = 0,55 · outsider structurel (WORM)
                         + 0,45 · confirmation (l'outsider se raccourcit)
        FAVORI_WATCH     = 0,55 · favori dominant (WORM)
                         + 0,45 · confirmation (le favori se raccourcit)

    Plus un TROISIEME axe, sans equivalent football, parce que le turf le
    publie et que le football n'a rien de comparable :

        NON_PARTANT_WATCH = 0,60 · retraits tardifs
                          + 0,40 · recomposition du marche

    Un retrait a H-30 redistribue TOUT l'argent de la course. C'est le seul
    evenement du turf dont l'effet sur les cotes est certain avant le depart.

    Tri par le maximum des trois, comme le pont football trie par le max de
    ses deux axes.
    """
    day = a.day or dt.datetime.now(ZoneInfo(TZ)).date().isoformat()
    f = os.path.join(WORM_SNAP, f"{day}.jsonl")
    if not os.path.exists(f):
        print(f"aucun snapshot WORM pour {day}", file=sys.stderr)
        return 1
    par = {}
    for ln in open(f, encoding="utf-8"):
        ln = ln.strip()
        if not ln:
            continue
        try:
            s = json.loads(ln)
        except json.JSONDecodeError:
            continue
        par.setdefault(s["course"]["course_id"], []).append(s)

    out, within = [], a.within or 30
    for cid, ps in par.items():
        last = ps[-1]
        m = last.get("minutes_avant_depart")
        if m is None or not (0 <= m <= within):
            continue
        mot = last["moteurs"]
        npm, der = mot["non_partants"], mot["derive"]
        fav, outs = mot["favori_dominant"], mot["outsider"]
        mouv = der.get("mouvements")

        # --- axe 1 : outsider (miroir de UPSET_WATCH)
        o_watch = o_statut = o_num = o_var = None
        best = outs.get("meilleur")
        if outs.get("score") is not None and best:
            o_num = best["num"]
            cs, o_var = _confirmation(mouv, o_num)
            if cs is not None:
                o_watch = round(0.55 * outs["score"] + 0.45 * cs)
                o_statut = ("LIVE_OUTSIDER_WATCH" if cs > 0 else "OUTSIDER_FADING")
            else:
                o_watch, o_statut = outs["score"], "WATCH"

        # --- axe 2 : favori dominant (miroir de BLOWOUT_WATCH)
        f_watch = f_statut = f_num = f_var = None
        if fav.get("score") is not None and fav.get("favori") is not None:
            f_num = fav["favori"]
            cs, f_var = _confirmation(mouv, f_num)
            if cs is not None:
                f_watch = round(0.55 * fav["score"] + 0.45 * cs)
                f_statut = ("LIVE_FAVORI_WATCH" if cs > 0 else "FAVORI_FADING")
            else:
                f_watch, f_statut = fav["score"], "WATCH"

        # --- axe 3 : non-partants (propre au turf)
        n_watch = n_statut = None
        if npm.get("score") is not None or der.get("score") is not None:
            n_watch = round(0.60 * (npm.get("score") or 0) + 0.40 * (der.get("score") or 0))
            n_statut = ("NON_PARTANT_WATCH" if npm.get("nouveaux_retraits")
                        else "MARCHE_RECOMPOSE" if (der.get("score") or 0) >= 50 else "WATCH")

        scores = [x for x in (o_watch, f_watch, n_watch) if x is not None]
        dominant = max(
            ((s, st) for s, st in ((o_watch, o_statut), (f_watch, f_statut),
                                   (n_watch, n_statut)) if s is not None),
            key=lambda t: t[0], default=(None, "WATCH"))
        out.append(dict(
            course_id=cid, libelle=last["course"]["libelle"],
            hippodrome=last["course"]["hippodrome"],
            discipline=last["course"]["discipline"],
            minutes_avant_depart=m, vague=vague(m),
            OUTSIDER_WATCH=o_watch, outsider_statut=o_statut,
            outsider_num=o_num, outsider_variation_pct=o_var,
            FAVORI_WATCH=f_watch, favori_statut=f_statut,
            favori_num=f_num, favori_variation_pct=f_var,
            NON_PARTANT_WATCH=n_watch, non_partant_statut=n_statut,
            nouveaux_retraits=npm.get("nouveaux_retraits"),
            amplitude_derive_pct=der.get("amplitude_max_pct"),
            score_max=(max(scores) if scores else None),
            statut=dominant[1], bet_authority=False))

    out.sort(key=lambda x: -(x["score_max"] or 0))
    jdump(os.path.join(WORM_MI, f"{day}.json"), dict(
        journee=day, fenetre_min=within, n=len(out),
        focus="NON_PARTANT+OUTSIDER+FAVORI", courses=out,
        formules=dict(
            OUTSIDER_WATCH="0,55 · outsider structurel (WORM) + 0,45 · confirmation (l'outsider se raccourcit)",
            FAVORI_WATCH="0,55 · favori dominant (WORM) + 0,45 · confirmation (le favori se raccourcit)",
            NON_PARTANT_WATCH="0,60 · retraits tardifs + 0,40 · recomposition du marche"),
        garde_fous=[
            "seul un raccourcissement compte comme confirmation ; une derive est une infirmation",
            "RLM non calculable : le PMU ne publie pas le % de parieurs par partant",
            "masse des enjeux non collectee",
            "bet_authority=false — cette cellule ne price pas"],
        bet_authority=False))

    L = [f"## APEX-TURF-MI — bruit de marche H-{within} "
         f"(focus NON-PARTANTS + OUTSIDER + FAVORI) · {day}", "",
         f"{len(out)} course(s) dans la fenetre", ""]
    if out:
        L += ["| Dans | Course | Hippodrome | OUTSIDER (n°, Δ) | OUTSIDER WATCH | "
              "FAVORI (n°, Δ) | FAVORI WATCH | NON-PARTANTS | NP WATCH | Statut |",
              "|---|---|---|---|---|---|---|---|---|---|"]
        for x in out:
            od = (f"#{x['outsider_num']}, {x['outsider_variation_pct']:+.1f} %"
                  if x["outsider_num"] is not None and x["outsider_variation_pct"] is not None
                  else "—")
            fd = (f"#{x['favori_num']}, {x['favori_variation_pct']:+.1f} %"
                  if x["favori_num"] is not None and x["favori_variation_pct"] is not None
                  else "—")
            n = lambda v: (v if v is not None else "—")
            L.append(f"| H-{x['minutes_avant_depart']} | {x['course_id']} | {x['hippodrome']} | "
                     f"{od} | {n(x['OUTSIDER_WATCH'])} | {fd} | {n(x['FAVORI_WATCH'])} | "
                     f"{', '.join('#'+str(k) for k in (x['nouveaux_retraits'] or [])) or '—'} | "
                     f"{n(x['NON_PARTANT_WATCH'])} | **{x['statut']}** |")
    else:
        L.append("Aucune course dans la fenetre. Sortie valide.")
    L += ["",
          "OUTSIDER WATCH = 0,55 · outsider structurel (WORM) + 0,45 · confirmation de "
          "mouvement vers l'outsider. ",
          "FAVORI WATCH = 0,55 · favori dominant (WORM) + 0,45 · confirmation de mouvement "
          "vers le favori. ",
          "NON-PARTANT WATCH = 0,60 · retraits tardifs + 0,40 · recomposition du marche — "
          "axe propre au turf, un retrait redistribue tout l'argent de la course. ",
          "`LIVE_OUTSIDER_WATCH` / `LIVE_FAVORI_WATCH` = anomalie structurelle **et** argent "
          "qui va dans le meme sens. `*_FADING` = le marche s'en eloigne.", "",
          "Garde-fous : seul un raccourcissement compte comme confirmation ; RLM non "
          "calculable sans % public ; masse des enjeux non collectee ; "
          "`bet_authority = false`.", ""]
    p = os.path.join(ROOT, "reports", "turf_worm", f"{day}.mi.md")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    open(p, "w", encoding="utf-8").write("\n".join(L) + "\n")
    print(f"APEX-TURF-MI worm-hook {day} : {len(out)} course(s) H-{within} "
          f"(focus NON_PARTANT + OUTSIDER + FAVORI)\n-> {p}")
    for x in out[:8]:
        nn = lambda v: (v if v is not None else "—")
        print(f"  {x['course_id']:<22} OUTSIDER {nn(x['OUTSIDER_WATCH'])} "
              f"[{nn(x['outsider_statut'])}] · FAVORI {nn(x['FAVORI_WATCH'])} "
              f"[{nn(x['favori_statut'])}] · NP {nn(x['NON_PARTANT_WATCH'])} "
              f"[{nn(x['non_partant_statut'])}] -> {x['statut']}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="apex_turf_mi", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("window")
    pi = sp.add_parser("init"); pi.add_argument("--date"); pi.add_argument("--within", type=int)
    po = sp.add_parser("oddsflow"); po.add_argument("--course-dir", required=True)
    ps = sp.add_parser("signal")
    ps.add_argument("--course-dir", required=True); ps.add_argument("--agent", required=True)
    ps.add_argument("--family", required=True); ps.add_argument("--source-tier", required=True)
    ps.add_argument("--url"); ps.add_argument("--note", default="")
    ps.add_argument("--confirmations", type=int, default=1)
    ps.add_argument("--impact", type=int, default=50)
    pb = sp.add_parser("behavioral")
    pb.add_argument("--course-dir", required=True); pb.add_argument("--agent", required=True)
    pb.add_argument("--index", required=True)
    pb.add_argument("--kind", required=True, choices=["fact", "observation", "interpretation"])
    pb.add_argument("--source-tier", required=True); pb.add_argument("--url")
    pb.add_argument("--note", default="")
    pc = sp.add_parser("check"); pc.add_argument("--course-dir", required=True)
    pz = sp.add_parser("score"); pz.add_argument("--course-dir", required=True)
    pf = sp.add_parser("finalize"); pf.add_argument("--run", required=True)
    pe = sp.add_parser("email"); pe.add_argument("--run", required=True)
    pw = sp.add_parser("worm-hook"); pw.add_argument("--day"); pw.add_argument("--within", type=int)
    a = p.parse_args(argv)
    return {"window": cmd_window, "init": cmd_init, "oddsflow": cmd_oddsflow,
            "signal": cmd_signal, "behavioral": cmd_behavioral, "check": cmd_check,
            "score": cmd_score, "finalize": cmd_finalize, "email": cmd_email,
            "worm-hook": cmd_worm_hook}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
