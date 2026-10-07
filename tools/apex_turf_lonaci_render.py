#!/usr/bin/env python3
"""
APEX-TURF-LONACI — rendu du programme officiel, sans connecteur.

Produit le fichier texte que `apex_turf_lonaci.py scope --from-text` consomme :
un champ par ligne, dans l'ordre de la page.

    python3 tools/apex_turf_lonaci_render.py --out /tmp/lonaci.txt

Deux chemins, essayes dans cet ordre, et la sortie dit TOUJOURS lequel a servi :

  1. PASSERELLE JSON (gratuite, sans navigateur)
     api.lonacionline.flexbet-software.com:14443, lue dans assets/config/config.json
     de l'application. Depuis un conteneur claude.ai elle est injoignable : le tunnel
     s'ouvre puis le serveur coupe au ClientHello (mesure du 02 et du 07/10/2026).
     Depuis un runner GitHub, sans proxy MITM, elle peut repondre — on essaie donc.

  2. NAVIGATEUR (Playwright + Chromium)
     La page est une application Angular : curl et WebFetch ne rendent que la coquille
     vide (verifie). Seule l'execution du JavaScript donne le programme.
     Depuis un conteneur claude.ai, Chromium refuse la CA du proxy
     (ERR_CERT_AUTHORITY_INVALID) et ce chemin echoue. Depuis un runner GitHub, il
     n'y a pas de proxy MITM : c'est la qu'il est prevu pour tourner.

Si les deux echouent, le script SORT EN ERREUR sans rien ecrire. Il ne faut surtout
pas qu'un fichier vide ou partiel devienne un perimetre : `scope` refuserait de
toute facon, mais mieux vaut echouer ici, avec le motif.

Stdlib + playwright (optionnel). Sans playwright installe, seul le chemin 1 est tente.
"""
import argparse, json, os, re, sys, urllib.request

PAGE = "https://pmu.lonacionline.ci/mobile/"
CONFIG = PAGE + "assets/config/config.json"
UA = ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/120.0.0.0 Mobile Safari/537.36")
CODE = re.compile(r"^R\d+C\d+$")


def log(m):
    print(m, file=sys.stderr, flush=True)


# ------------------------------------------------------- chemin 1 : passerelle

def via_passerelle(timeout=25):
    try:
        r = urllib.request.Request(CONFIG, headers={"User-Agent": UA})
        with urllib.request.urlopen(r, timeout=timeout) as f:
            cfg = json.load(f)
    except Exception as e:
        return None, f"config.json illisible : {type(e).__name__}: {e}"
    base = (cfg.get("gateways") or {}).get("turf_gateway")
    if not base:
        return None, "config.json ne porte pas gateways.turf_gateway"
    for ep in ("/ser_hippique_json",):
        url = base.rstrip("/") + ep
        try:
            r = urllib.request.Request(url, headers={
                "User-Agent": UA, "Accept": "application/json",
                "Origin": "https://pmu.lonacionline.ci",
                "Referer": PAGE})
            with urllib.request.urlopen(r, timeout=timeout) as f:
                d = json.load(f)
            return d, f"passerelle {url}"
        except Exception as e:
            dernier = f"{type(e).__name__}: {e}"
    return None, f"passerelle injoignable ({dernier})"


def passerelle_vers_texte(d):
    """
    Met la reponse de la passerelle au format attendu par le parseur.

    Structure REELLE, observee le 07/10/2026 sur le runner (c'est la seule
    facon dont elle pouvait l'etre : la passerelle est injoignable depuis un
    conteneur claude.ai) :

        $ list[4]                              <- les reunions du programme
          [0] int_Numero  '1'                  <- numero de REUNION
              str_Name    'ENGHIEN'
              Course list[8]
                [0] int_Numero            '1'  <- numero de COURSE
                    Condition             'PRIX DES GOBELINS'
                    str_City              'ENGHIEN'
                    dt_Course_Date        '2026-10-07 11:55:00'
                    by_Participant_Number '18'
                    str_Status            'cloturer'
                    Int_Distance          '2875'

    LE CODE R#C# N'EST PAS UN CHAMP : il se construit en recollant le numero
    de reunion et celui de la course. C'est precisement ce que la premiere
    version cherchait en vain comme une chaine deja formee.
    """
    reunions = d if isinstance(d, list) else [d]
    lignes, n = ["Courses du jour", ""], 0
    for r in reunions:
        if not isinstance(r, dict):
            continue
        rn = str(r.get("int_Numero") or "").strip()
        hippo_r = (r.get("str_Name") or "").strip()
        courses = r.get("Course") or []
        if isinstance(courses, dict):          # une seule course : pas de liste
            courses = [courses]
        for c in courses:
            if not isinstance(c, dict):
                continue
            cn = str(c.get("int_Numero") or "").strip()
            if not (rn.isdigit() and cn.isdigit()):
                continue
            hippo = (c.get("str_City") or hippo_r or "").strip()
            lib = (c.get("Condition") or "").strip()
            dt = (c.get("dt_Course_Date") or "").strip()
            heure = ""
            if len(dt) >= 16 and " " in dt:
                hh, mm = dt.split(" ")[1].split(":")[:2]
                heure = f"{hh}h{mm}"
            # 4e ligne : l'etat, comme la page l'affiche. Le parseur de
            # perimetre ne s'en sert pas, mais il rend le fichier lisible.
            etat = (c.get("str_Status") or "").strip()
            part = str(c.get("by_Participant_Number") or "").strip()
            dist = str(c.get("Int_Distance") or "").strip()
            detail = (f"{dist}m-{part} Partants" if dist and part else etat)
            lignes += [f"R{rn}C{cn}", hippo, lib, heure, detail, ""]
            n += 1
    return ("\n".join(lignes) + "\n", n) if n else (None, 0)


# ------------------------------------------------------- chemin 2 : navigateur

RN = re.compile(r"^R\d+$")
CN = re.compile(r"^C\d+$")


def _recoller_codes(txt):
    """« R1 » puis « C1 » sur deux lignes deviennent « R1C1 »."""
    L = [l.strip() for l in txt.splitlines()]
    out, i = [], 0
    while i < len(L):
        if i + 1 < len(L) and RN.match(L[i]) and CN.match(L[i + 1]):
            out.append(L[i] + L[i + 1])
            i += 2
        else:
            out.append(L[i])
            i += 1
    return "\n".join(out)


def via_navigateur(timeout_ms=120000, attente_ms=20000):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None, "playwright non installe"
    chemins = [os.environ.get("CHROMIUM_PATH"),
               "/opt/pw-browsers/chromium-1194/chrome-linux/chrome"]
    exe = next((p for p in chemins if p and os.path.exists(p)), None)
    try:
        with sync_playwright() as p:
            lancer = dict(args=["--no-sandbox", "--disable-dev-shm-usage"])
            if exe:
                lancer["executable_path"] = exe
            b = p.chromium.launch(**lancer)
            pg = b.new_page(user_agent=UA, viewport={"width": 420, "height": 1400})
            pg.goto(PAGE, wait_until="networkidle", timeout=timeout_ms)
            pg.wait_for_timeout(attente_ms)
            txt = pg.inner_text("body")
            b.close()
    except Exception as e:
        return None, f"navigateur : {type(e).__name__}: {str(e).splitlines()[0][:140]}"
    # La page affiche le code en DEUX elements : une ligne « R1 », une ligne « C1 ».
    # Observe le 07/10 sur le runner. On les recolle avant de chercher R#C#.
    txt = _recoller_codes(txt)
    n = sum(1 for l in txt.splitlines() if CODE.match(l.strip()))
    if not n:
        apercu = " / ".join(l.strip() for l in txt.splitlines() if l.strip())[:300]
        return None, (f"navigateur : {len(txt)} caracteres rendus, aucun code R#C#. "
                      f"Debut du texte : {apercu!r}")
    return txt, f"navigateur, {n} code(s) R#C#"


# ----------------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="fichier texte a ecrire")
    ap.add_argument("--only", choices=["passerelle", "navigateur"],
                    help="n'essayer qu'un seul chemin (diagnostic)")
    a = ap.parse_args(argv)

    motifs = []

    if a.only != "navigateur":
        log("chemin 1 — passerelle JSON…")
        d, m = via_passerelle()
        if d is not None:
            txt, n = passerelle_vers_texte(d)
            if txt:
                open(a.out, "w", encoding="utf-8").write(txt)
                log(f"OK via {m} — {n} course(s) -> {a.out}")
                print(f"SOURCE=passerelle\nCOURSES={n}\nFICHIER={a.out}")
                return 0
            m = f"{m} : repondu, mais aucun code R#C# reconnu dans le JSON"
            log("")
            log("  --- carte de structure du JSON recu (pour ecrire le parseur) ---")
            for l in carte_structure(d):
                log("  " + l)
            log("  --- fin de la carte ---")
            log("")
        motifs.append(m)
        log(f"  echec : {m}")

    if a.only != "passerelle":
        log("chemin 2 — navigateur Playwright…")
        txt, m = via_navigateur()
        if txt:
            open(a.out, "w", encoding="utf-8").write(txt)
            n = sum(1 for l in txt.splitlines() if CODE.match(l.strip()))
            log(f"OK via {m} -> {a.out}")
            print(f"SOURCE=navigateur\nCOURSES={n}\nFICHIER={a.out}")
            return 0
        motifs.append(m)
        log(f"  echec : {m}")

    log("")
    log("AUCUN CHEMIN N'A ABOUTI — rien n'a ete ecrit.")
    for i, m in enumerate(motifs, 1):
        log(f"  {i}. {m}")
    log("")
    log("Le perimetre LONACI ne peut pas etre lu. Ne pas scanner le programme")
    log("francais entier a la place : ce serait ignorer la demande.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
