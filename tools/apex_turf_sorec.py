#!/usr/bin/env python3
"""
APEX-TURF-SOREC — sonde des courses marocaines (SOREC), pour le perimetre LONACI.

POURQUOI CE FICHIER EXISTE
Le programme LONACI comporte des reunions marocaines — la Nationale 3, courue a
Khemisset, Anfa, Rabat, Casablanca, El Jadida, Marrakech, Meknes. L'API PMU
turfinfo NE LES DIFFUSE PAS : le 07/10/2026 elle annoncait sept reunions, aucune
marocaine, alors que LONACI affichait Khemisset en R9. Ces courses sortent donc
ABSENT_SOURCE du perimetre, et aucune lecture de marche n'est possible.

CE QUI A DEJA ETE MESURE (07/10/2026, depuis un conteneur claude.ai)
  www.e-sorec.ma        Recv failure: Connection reset by peer (~13 s), 3 essais
  www.sorec.ma          200, mais site institutionnel : aucun programme
  www.sorec-galop.ma    200. Le document « Programme reunion du 07/10/2026
                        Khemisset » EST liste. Le bouton Telecharger est un
                        postback JSF : le ViewState avance (donc l'action
                        s'execute) mais la reponse est la page re-rendue, pas le
                        PDF.
  canalturf             derniere reunion PMU a Khemisset : 28/09/2022
  zone-turf, zeturf, equidia, paris-turf, oneturf : programme francais seulement

Le reset d'e-sorec.ma a EXACTEMENT la signature de la passerelle LONACI, qui est
injoignable depuis un conteneur claude.ai et REPOND depuis un runner GitHub, ou
il n'y a pas de proxy MITM. D'ou cette sonde : la faire tourner la-bas.

CE QUE CE SCRIPT NE FAIT PAS
Il ne devine rien. Si aucun chemin n'aboutit il sort en erreur sans rien ecrire :
un fichier vide ou partiel qui deviendrait un programme serait pire que l'absence.
Et il ne touche AUCUN endpoint de pari ni de compte — lecture seule, pages
publiques.

    python3 tools/apex_turf_sorec.py probe
    python3 tools/apex_turf_sorec.py programme --date 07102026 --out /tmp/khem.pdf

Stdlib uniquement.
"""
import argparse, json, re, sys, urllib.parse, urllib.request, http.cookiejar
import datetime as dt

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0.0.0 Safari/537.36")

GALOP = "https://www.sorec-galop.ma"
PROG_PAGE = GALOP + "/pages/programmeReunion/programmeReunion.jsf"

# Hotes a sonder. e-sorec est le site de PARIS : c'est le seul qui porterait des
# cotes, donc le seul qui permettrait une lecture de marche. sorec-galop porte le
# programme officiel (partants, jockeys, poids) mais JAMAIS de cote.
CIBLES = [
    ("e-sorec",        "https://www.e-sorec.ma/"),
    ("e-sorec (nu)",   "https://e-sorec.ma/"),
    ("sorec-galop",    PROG_PAGE),
    ("sorec-galop/res", GALOP + "/pages/resultat/resultat_course.jsf?fctID=1396"),
    ("sorec-galop/ven", GALOP + "/pages/course_a_venir/course_a_venir.jsf?fctID=1404"),
    ("sorec",          "https://www.sorec.ma/"),
]


def log(m):
    print(m, file=sys.stderr, flush=True)


def _opener():
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def _get(op, url, timeout=30, headers=None):
    h = {"User-Agent": UA}
    h.update(headers or {})
    with op.open(urllib.request.Request(url, headers=h), timeout=timeout) as f:
        return f.read(), dict(f.headers), f.status


def _post(op, url, champs, timeout=60, headers=None):
    h = {"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded"}
    h.update(headers or {})
    data = urllib.parse.urlencode(champs).encode("utf-8")
    with op.open(urllib.request.Request(url, data=data, headers=h), timeout=timeout) as f:
        return f.read(), dict(f.headers), f.status


# ----------------------------------------------------------------------- probe

def cmd_probe(a):
    """
    Dit, pour chaque cible, si elle repond DEPUIS ICI. C'est la mesure qui
    decide : si e-sorec repond sur le runner alors qu'il coupe en session,
    le chemin existe et il faut le prendre la-bas.
    """
    op = _opener()
    res = []
    for nom, url in CIBLES:
        t0 = dt.datetime.now()
        try:
            corps, hdr, st = _get(op, url, timeout=a.timeout)
            ms = int((dt.datetime.now() - t0).total_seconds() * 1000)
            ct = hdr.get("Content-Type", "?")
            tete = corps[:300].decode("utf-8", "replace").replace("\n", " ")
            tete = re.sub(r"\s+", " ", tete).strip()
            res.append(dict(cible=nom, url=url, ok=True, statut=st, ms=ms,
                            octets=len(corps), content_type=ct, tete=tete[:220]))
            log(f"  OK   {nom:<18} {st} {len(corps):>8} o  {ms:>6} ms  {ct}")
        except Exception as e:
            ms = int((dt.datetime.now() - t0).total_seconds() * 1000)
            res.append(dict(cible=nom, url=url, ok=False, ms=ms,
                            erreur=f"{type(e).__name__}: {e}"))
            log(f"  ECHEC {nom:<18} {ms:>6} ms  {type(e).__name__}: {e}")
    joignables = [r for r in res if r["ok"]]
    log("")
    log(f"{len(joignables)}/{len(res)} cible(s) joignable(s) depuis cet hote.")
    if a.json:
        open(a.json, "w", encoding="utf-8").write(
            json.dumps(dict(releve=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                            resultats=res), ensure_ascii=False, indent=1) + "\n")
        log(f"-> {a.json}")
    # Le code de sortie porte l'information utile au workflow : e-sorec est le
    # seul qui changerait quelque chose, puisque c'est le seul a porter des cotes.
    esorec = any(r["ok"] for r in res if r["cible"].startswith("e-sorec"))
    print(f"ESOREC_JOIGNABLE={'oui' if esorec else 'non'}")
    print(f"JOIGNABLES={len(joignables)}")
    return 0


# ------------------------------------------------------- programme sorec-galop

def _etat_jsf(html_txt, id_form):
    """ViewState + __ncforminfo + action : tout ce qu'un postback JSF exige."""
    vs = re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', html_txt)
    nc = re.search(r'name="__ncforminfo"[^>]*value="([^"]*)"', html_txt)
    act = re.search(rf'id="{re.escape(id_form)}"[^>]*action="([^"]+)"', html_txt)
    return (vs.group(1) if vs else None,
            nc.group(1) if nc else None,
            act.group(1) if act else None)


def _ligne_du_jour(html_txt, jjmmaaaa):
    """
    Numero de ligne du tableau pour la date demandee, et le libelle trouve.
    La date s'affiche JJ/MM/AAAA ; on recoit DDMMYYYY.
    """
    d = f"{jjmmaaaa[0:2]}/{jjmmaaaa[2:4]}/{jjmmaaaa[4:8]}"
    for m in re.finditer(r'data-ri="(\d+)"(.*?)</tr>', html_txt, re.S):
        if d in m.group(2):
            cells = [re.sub(r"<[^>]+>", "", c).strip()
                     for c in re.findall(r"<td[^>]*>(.*?)</td>", m.group(2), re.S)]
            return m.group(1), " · ".join(cells[:3])
    return None, None


def cmd_programme(a):
    """
    Telecharge le programme officiel de la reunion marocaine du jour.

    ATTENTION a ce que ce fichier EST et n'est PAS : c'est le programme, donc
    les partants, les jockeys, les poids et les conditions. Il ne porte AUCUNE
    cote. Il permet de connaitre le champ, pas de lire un marche.
    """
    op = _opener()
    try:
        corps, _, _ = _get(op, PROG_PAGE, timeout=a.timeout)
    except Exception as e:
        log(f"page programme injoignable : {type(e).__name__}: {e}")
        return 1
    html_txt = corps.decode("utf-8", "replace")
    ri, libelle = _ligne_du_jour(html_txt, a.date)
    if ri is None:
        log(f"aucune reunion listee pour le {a.date} sur la premiere page.")
        log("Les 10 dernieres publiees :")
        for m in list(re.finditer(r'data-ri="(\d+)"(.*?)</tr>', html_txt, re.S))[:10]:
            c = [re.sub(r"<[^>]+>", "", x).strip()
                 for x in re.findall(r"<td[^>]*>(.*?)</td>", m.group(2), re.S)]
            log("   " + " · ".join(c[:3]))
        return 1
    log(f"ligne {ri} : {libelle}")

    vs, nc, act = _etat_jsf(html_txt, "form:formDocu")
    if not (vs and act):
        log("ViewState ou action introuvable : la page a change de structure.")
        return 1
    champs = [("form:formDocu", "form:formDocu"),
              (f"form:formDocu:datatableC:{ri}:j_idt65", ""),
              ("javax.faces.ViewState", vs)]
    if nc is not None:
        champs.append(("__ncforminfo", nc))

    try:
        corps2, hdr2, _ = _post(op, GALOP + act, champs, timeout=a.timeout,
                                headers={"Referer": PROG_PAGE})
    except Exception as e:
        log(f"postback echoue : {type(e).__name__}: {e}")
        return 1

    ct = (hdr2.get("Content-Type") or "").lower()
    cd = hdr2.get("Content-Disposition") or ""
    est_pdf = corps2[:5] == b"%PDF-" or "pdf" in ct or "pdf" in cd.lower()
    if est_pdf:
        open(a.out, "wb").write(corps2)
        log(f"OK — {len(corps2)} octets -> {a.out}")
        print(f"SOURCE=sorec-galop\nFICHIER={a.out}\nOCTETS={len(corps2)}")
        return 0

    # Mesure du 07/10 : le postback renvoie la page re-rendue. On le DIT, avec
    # de quoi ecrire la suite, au lieu de pretendre avoir telecharge.
    log("")
    log("Le postback n'a PAS renvoye de fichier.")
    log(f"  content-type        : {ct or '(absent)'}")
    log(f"  content-disposition : {cd or '(absent)'}")
    log(f"  octets              : {len(corps2)}")
    t2 = corps2.decode("utf-8", "replace")
    vs2, _, _ = _etat_jsf(t2, "form:formDocu")
    log(f"  ViewState avant     : {vs}")
    log(f"  ViewState apres     : {vs2}")
    log("  (un ViewState qui avance = l'action S'EST executee cote serveur ;")
    log("   le fichier ne revient simplement pas par ce chemin)")
    if a.dump:
        open(a.dump, "wb").write(corps2)
        log(f"  reponse brute -> {a.dump}")
    print("SOURCE=aucune\nFICHIER=\nOCTETS=0")
    return 2


def main(argv=None):
    p = argparse.ArgumentParser(prog="apex_turf_sorec", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    pr = sp.add_parser("probe", help="quelles sources marocaines repondent d'ici")
    pr.add_argument("--timeout", type=int, default=25)
    pr.add_argument("--json", help="ecrire le releve dans ce fichier")
    pg = sp.add_parser("programme", help="telecharger le programme officiel du jour")
    pg.add_argument("--date", required=True, help="DDMMYYYY")
    pg.add_argument("--out", required=True)
    pg.add_argument("--dump", help="ecrire la reponse brute si ce n'est pas un PDF")
    pg.add_argument("--timeout", type=int, default=60)
    a = p.parse_args(argv)
    return {"probe": cmd_probe, "programme": cmd_programme}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
