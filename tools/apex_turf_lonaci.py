#!/usr/bin/env python3
"""
APEX-TURF-LONACI — perimetre officiel du programme PMU LONACI (Cote d'Ivoire).

Restreint le scan APEX-TURF-WORM aux courses REELLEMENT inscrites au programme
LONACI, et non a tout le programme francais.

Fait etabli le 02/10/2026 par confrontation des deux programmes
--------------------------------------------------------------
LONACI emploie LES MEMES codes R#C# que le PMU francais. Verifie sur les
30 courses du 02/10 : chaque code francais tombe sur le bon hippodrome et le
bon nom de course (R1C4 = Vincennes Prix Ludovica, R3C5 = Borely Prix des
Camelias, etc.). Le perimetre se reduit donc a une LISTE DE CODES.

Trois consequences, mesurees et non supposees :

1. L'HEURE N'EST PAS UNE CLE DE VALIDATION. Sur 30 courses : 9 heures
   identiques, 13 ecarts de 1 a 5 min, aucun au-dela. LONACI publie une heure
   programmee, le PMU une heureDepart. On valide sur le NOM de la course et on
   tolere l'ecart d'heure jusqu'a 6 min.

2. LA NATIONALE 3 EST MAROCAINE et N'EXISTE PAS dans l'API francaise. Les 8
   courses R9 d'Anfa du 02/10 sont absentes de la source, pas seulement du
   perimetre des moteurs. Elles sortent ABSENT_SOURCE : ni cote, ni partant,
   ni rien. Aucune analyse possible, et le dire est la seule reponse honnete.

3. LE PLAT DOMINE le reste du programme LONACI. Sur les 22 courses francaises
   du 02/10 : 11 trot attele, 1 trot monte, 1 obstacle, 9 plat. Soit, avec les
   8 marocaines, 11 courses analysables sur 30.

Question ouverte, non tranchee : DE QUI SONT LES COTES ?
--------------------------------------------------------
LONACI sert ses propres rapports par sa propre passerelle
(api.lonacionline.flexbet-software.com, endpoint ws_web_mobile_rapport.jsp).
Rien ne prouve que sa masse d'enjeux soit celle du PMU francais. Or les
moteurs APEX sont calibres sur les cotes FRANCAISES, qu'ils utilisent comme
offset de marche. Si les deux masses sont distinctes, l'offset est celui d'un
autre marche que celui ou l'on joue.

Cette question n'est PAS resolue ici : la passerelle LONACI est injoignable
depuis le reseau de cette session (tunnel ouvert puis coupe par le serveur).
Le champ `cotes_origine` de chaque sortie vaut donc "PMU_FRANCE" et le champ
`avertissement_masse` le dit. Ne jamais presenter une analyse LONACI comme
fondee sur les cotes LONACI tant que la comparaison n'a pas ete faite.

Pour la trancher : relever, sur une vingtaine de courses, le rapport LONACI
d'un Simple Gagnant et le rapport francais du meme cheval. Identiques = masse
commune, l'offset est bon. Differents = il faut les cotes LONACI.

    python3 tools/apex_turf_lonaci.py scope --date 02102026 --from-text <fichier>
    python3 tools/apex_turf_lonaci.py scope --date 02102026 --from-codes R1C1,R1C2
    python3 tools/apex_turf_lonaci.py show  --date 02102026

Stdlib uniquement.
"""
import argparse, json, os, re, sys, time, unicodedata, urllib.request
import datetime as dt

API = "https://online.turfinfo.api.pmu.fr/rest/client/1/programme"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCOPE = os.path.join(ROOT, "data", "turf_worm", "lonaci")

PRICEABLE = {"TROT_ATTELE"}
INDICATIF = {"TROT_MONTE", "HAIES", "STEEPLE_CHASE", "CROSS", "OBSTACLE"}
TOLERANCE_MIN = 6


def norm(s):
    """Comparaison de noms de course : accents, ponctuation et casse neutralises."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def get(url, tries=3):
    for i in range(tries):
        try:
            r = urllib.request.Request(url, headers={"User-Agent": "APEX-TURF-LONACI/1.0"})
            with urllib.request.urlopen(r, timeout=30) as f:
                return json.load(f)
        except Exception as e:
            if i == tries - 1:
                return {"_erreur": f"{type(e).__name__}: {e}"}
            time.sleep(2 ** i)


def programme_francais(ddmmyyyy):
    p = get(f"{API}/{ddmmyyyy}")
    if "_erreur" in p:
        return None, p["_erreur"]
    out = {}
    for r in (p.get("programme", {}).get("reunions") or []):
        for c in (r.get("courses") or []):
            t = (dt.datetime.fromtimestamp(c["heureDepart"] / 1000, dt.timezone.utc)
                 if c.get("heureDepart") else None)
            out[f"R{r['numOfficiel']}C{c['numExterne']}"] = dict(
                hippodrome=(r.get("hippodrome") or {}).get("libelleCourt"),
                pays=(r.get("pays") or {}).get("code"),
                libelle=c.get("libelle"), discipline=c.get("specialite"),
                distance=c.get("distance"), statut=c.get("statut"),
                declares=c.get("nombreDeclaresPartants"),
                depart_utc=t.isoformat(timespec="seconds") if t else None,
                heure=t.strftime("%Hh%M") if t else None)
    return out, None


# --------------------------------------------------- lecture du programme LONACI

LIGNE = re.compile(r"^\s*(R\d+C\d+)\s*$")
HEURE = re.compile(r"^\s*(\d{1,2})h(\d{2})\s*$")


def parse_texte_lonaci(txt):
    """
    Lit le texte RENDU de la page publique https://pmu.lonacionline.ci/mobile/.

    La page est une application Angular : un `curl` ne rend rien. Le texte doit
    venir d'un moteur de rendu (navigateur, ou outil de recuperation de page en
    session). Format attendu, tel que la page le produit :

        R1C4
        PARIS-VINCENNES
        PRIX LUDOVICA
        18h15

    Le parseur est volontairement tolerant sur ce qui separe ces blocs, et
    strict sur le code : une ligne qui n'est pas exactement R#C# n'ouvre pas une
    course.
    """
    lignes = [l.rstrip() for l in txt.splitlines()]
    out, i = [], 0
    while i < len(lignes):
        m = LIGNE.match(lignes[i])
        if not m:
            i += 1
            continue
        code = m.group(1)
        suite = [l.strip() for l in lignes[i + 1:i + 7] if l.strip()]
        hippo = suite[0] if suite else None
        libelle = suite[1] if len(suite) > 1 else None
        heure = next((h.group(0).strip() for h in (HEURE.match(s) for s in suite) if h), None)
        out.append(dict(code=code, hippodrome_lonaci=hippo,
                        libelle_lonaci=libelle, heure_lonaci=heure))
        i += 1
    # dedoublonnage : la page liste deux fois les courses mises en avant
    # (bloc "Nationale N" puis bloc "Toutes les Courses"). On garde la
    # premiere occurrence, en notant la mise en avant.
    vu, uniq = {}, []
    for c in out:
        if c["code"] in vu:
            vu[c["code"]]["double_annonce"] = True
            continue
        c["double_annonce"] = False
        vu[c["code"]] = c
        uniq.append(c)
    return uniq


# ------------------------------------------------------------- classement final

def classer(code, lon, fr):
    """
    Dit ce qu'on peut faire de cette course, et pourquoi. Jamais d'a-peu-pres :
    une course non analysable est nommee comme telle, avec son motif.
    """
    f = fr.get(code)
    if not f:
        return dict(statut="ABSENT_SOURCE", moteur=None,
                    motif=("absente du programme PMU francais : ni partants, ni cotes. "
                           "La Nationale 3 LONACI est marocaine et n'est pas servie par "
                           "cette source."))
    if f["pays"] != "FRA":
        return dict(statut="HORS_FRANCE", moteur=None,
                    motif=f"reunion {f['pays']} : les moteurs ne sont calibres que sur "
                          f"le programme francais.")
    d = f["discipline"]
    if d == "PLAT":
        return dict(statut="REFUSE_PLAT", moteur=None,
                    motif=("aucun moteur calibre pour le plat. Les coefficients du trot "
                           "et de l'obstacle ne sont pas transposables : cf vaut 0,0 en "
                           "trot contre 0,4 en obstacle. Hors discipline le signe est faux."))
    if d in PRICEABLE:
        return dict(statut="ANALYSABLE", moteur="trot",
                    motif="trot attele francais : DCS 82 en walk-forward.")
    if d in INDICATIF:
        m = "trot" if d.startswith("TROT") else "obst"
        why = ("trot monte : G-TROT-0, ECE 1,164 contre 0,923 pour le marche sur 693 "
               "courses. INDICATIF obligatoire." if d == "TROT_MONTE" else
               "obstacle : DCS 63, sous le seuil de 65. Gate de pari fermee par "
               "arithmetique. INDICATIF.")
        return dict(statut="INDICATIF", moteur=m, motif=why)
    return dict(statut="INCONNU", moteur=None, motif=f"discipline {d} non prevue.")


def cmd_scope(a):
    ddmmyyyy = a.date or dt.datetime.now(dt.timezone.utc).strftime("%d%m%Y")
    try:
        jour = dt.datetime.strptime(ddmmyyyy, "%d%m%Y").date().isoformat()
    except ValueError:
        print(f"--date attend DDMMYYYY, recu : {ddmmyyyy}", file=sys.stderr)
        return 2

    if a.from_text:
        if not os.path.exists(a.from_text):
            print(f"fichier introuvable : {a.from_text}", file=sys.stderr)
            return 2
        lon = parse_texte_lonaci(open(a.from_text, encoding="utf-8").read())
        source = f"texte rendu de pmu.lonacionline.ci ({os.path.basename(a.from_text)})"
    elif a.from_codes:
        lon = [dict(code=c.strip().upper(), hippodrome_lonaci=None,
                    libelle_lonaci=None, heure_lonaci=None, double_annonce=False)
               for c in a.from_codes.split(",") if c.strip()]
        source = "codes fournis a la main"
    else:
        print("fournir --from-text <fichier> ou --from-codes R1C1,R1C2", file=sys.stderr)
        print("\nLa passerelle LONACI (api.lonacionline.flexbet-software.com:14443) est",
              file=sys.stderr)
        print("injoignable depuis ce reseau : le tunnel s'ouvre puis le serveur coupe.",
              file=sys.stderr)
        print("Le programme doit donc venir du texte RENDU de la page publique, la page",
              file=sys.stderr)
        print("etant une application Angular qu'un curl ne rend pas.", file=sys.stderr)
        return 2

    if not lon:
        print("AUCUN code R#C# trouve dans la source. STOP — ne pas deviner un perimetre.",
              file=sys.stderr)
        return 2

    fr, err = programme_francais(ddmmyyyy)
    if err:
        print(f"programme francais illisible : {err}", file=sys.stderr)
        return 1

    courses, ecarts = [], []
    for c in lon:
        cl = classer(c["code"], c, fr)
        f = fr.get(c["code"], {})
        # Validation sur le NOM, pas sur l'heure : l'heure derive de 1 a 5 min.
        nom_ok = None
        if c["libelle_lonaci"] and f.get("libelle"):
            a_, b_ = norm(c["libelle_lonaci"]), norm(f["libelle"])
            nom_ok = a_ in b_ or b_ in a_ or a_ == b_
        dh = None
        if c["heure_lonaci"] and f.get("heure"):
            hm = lambda s: int(s.split("h")[0]) * 60 + int(s.split("h")[1])
            dh = abs(hm(c["heure_lonaci"]) - hm(f["heure"]))
            if dh > TOLERANCE_MIN:
                ecarts.append((c["code"], c["heure_lonaci"], f["heure"], dh))
        courses.append(dict(**c, **cl, pmu=f or None,
                            nom_concordant=nom_ok, ecart_heure_min=dh))

    par = {}
    for c in courses:
        par[c["statut"]] = par.get(c["statut"], 0) + 1
    out = dict(
        journee=jour, date_pmu=ddmmyyyy, source=source,
        releve=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        n_lonaci=len(courses), repartition=par,
        codes_analysables=[c["code"] for c in courses if c["statut"] == "ANALYSABLE"],
        codes_indicatifs=[c["code"] for c in courses if c["statut"] == "INDICATIF"],
        codes_scannables=[c["code"] for c in courses
                          if c["statut"] in ("ANALYSABLE", "INDICATIF")],
        cotes_origine="PMU_FRANCE",
        avertissement_masse=(
            "Les cotes viennent du PMU FRANCAIS. Rien ne prouve que la masse d'enjeux "
            "LONACI soit la meme : LONACI sert ses propres rapports par sa propre "
            "passerelle, injoignable depuis ce reseau. Les moteurs utilisant la cote "
            "comme offset de marche, un offset d'un autre marche que celui ou l'on joue "
            "est un defaut non quantifie. A trancher en comparant, sur une vingtaine de "
            "courses, le rapport LONACI et le rapport francais du meme cheval."),
        validation=dict(cle="nom de course", tolerance_heure_min=TOLERANCE_MIN,
                        ecarts_hors_tolerance=ecarts),
        courses=courses)
    os.makedirs(SCOPE, exist_ok=True)
    p = os.path.join(SCOPE, f"{jour}.json")
    json.dump(out, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"PERIMETRE LONACI du {jour} — {len(courses)} courses au programme")
    print(f"  source : {source}\n")
    lab = {"ANALYSABLE": "analysables (trot attele)",
           "INDICATIF": "indicatif plafonne (monte, obstacle)",
           "REFUSE_PLAT": "refusees — plat, aucun moteur",
           "ABSENT_SOURCE": "absentes de la source (Nationale 3 marocaine)",
           "HORS_FRANCE": "hors France", "INCONNU": "discipline inconnue"}
    for k in ("ANALYSABLE", "INDICATIF", "REFUSE_PLAT", "ABSENT_SOURCE",
              "HORS_FRANCE", "INCONNU"):
        if par.get(k):
            print(f"  {par[k]:>3}  {lab[k]}")
    tot = par.get("ANALYSABLE", 0) + par.get("INDICATIF", 0)
    print(f"\n  => {tot} course(s) sur {len(courses)} entrent dans le perimetre des moteurs")
    nn = [c for c in courses if c["nom_concordant"] is False]
    if nn:
        print(f"\n  ATTENTION — {len(nn)} nom(s) de course discordant(s) : perimetre "
              f"peut-etre perime")
        for c in nn[:6]:
            print(f"    {c['code']} : LONACI « {c['libelle_lonaci']} » "
                  f"vs PMU « {(c['pmu'] or {}).get('libelle')} »")
    if ecarts:
        print(f"\n  ecart d'heure au-dela de {TOLERANCE_MIN} min : {len(ecarts)}")
        for code, hl, hf, d in ecarts[:6]:
            print(f"    {code} : LONACI {hl} vs PMU {hf} ({d} min)")
    print(f"\n  cotes : PMU_FRANCE — voir avertissement_masse dans le fichier")
    print(f"-> {p}")
    return 0


def cmd_show(a):
    jour = (dt.datetime.strptime(a.date, "%d%m%Y").date().isoformat() if a.date
            else dt.datetime.now(dt.timezone.utc).date().isoformat())
    p = os.path.join(SCOPE, f"{jour}.json")
    d = None
    if os.path.exists(p):
        try:
            d = json.load(open(p, encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    if not d:
        print(f"aucun perimetre LONACI pour {jour}. Le construire avec "
              f"`scope --date {a.date or ''} --from-text <fichier>`.", file=sys.stderr)
        return 1
    print(f"PERIMETRE LONACI {d['journee']} · {d['n_lonaci']} courses · "
          f"relevé {d['releve']}")
    print(f"source : {d['source']}\n")
    print(f"{'code':<7}{'statut':<15}{'moteur':<7}{'disc':<14}{'hippodrome':<16}libelle")
    for c in d["courses"]:
        f = c.get("pmu") or {}
        print(f"{c['code']:<7}{c['statut']:<15}{str(c['moteur'] or '—'):<7}"
              f"{str(f.get('discipline') or '—')[:13]:<14}"
              f"{str(f.get('hippodrome') or c['hippodrome_lonaci'] or '—')[:15]:<16}"
              f"{str(f.get('libelle') or c['libelle_lonaci'] or '—')[:34]}")
    print(f"\nscannables : {' '.join(d['codes_scannables']) or '(aucune)'}")
    print(f"\ncotes : {d['cotes_origine']}\n{d['avertissement_masse']}")
    return 0


def charger_scope(jour):
    """Utilise par apex_turf_worm.py --lonaci. Retourne (codes, meta) ou (None, None)."""
    p = os.path.join(SCOPE, f"{jour}.json")
    if not os.path.exists(p):
        return None, None
    try:
        d = json.load(open(p, encoding="utf-8"))
    except json.JSONDecodeError:
        return None, None
    return set(d.get("codes_scannables") or []), d


def main(argv=None):
    p = argparse.ArgumentParser(prog="apex_turf_lonaci", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    s = sp.add_parser("scope")
    s.add_argument("--date", help="DDMMYYYY")
    s.add_argument("--from-text", help="fichier contenant le texte RENDU de la page LONACI")
    s.add_argument("--from-codes", help="R1C1,R1C2,… fournis a la main")
    sh = sp.add_parser("show"); sh.add_argument("--date", help="DDMMYYYY")
    a = p.parse_args(argv)
    return {"scope": cmd_scope, "show": cmd_show}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
