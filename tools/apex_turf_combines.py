#!/usr/bin/env python3
"""
APEX-TURF-COMBINES — probabilites de combines sur les paris REELLEMENT proposes
par LONACI, calculees sur des ARRIVEES SIMULEES.

REGLE DE FOND, et c'est la seule qui compte ici : un combine ne se calcule JAMAIS
en multipliant des probabilites marginales. P(trio 3-7-11) n'est pas
p3 · p7 · p11 : les places ne sont pas independantes, un cheval qui gagne ne peut
pas etre deuxieme. On simule des arrivees completes par Plackett-Luce, puis on
COMPTE les combinaisons gagnantes. C'est le meme mecanisme que
apex_turf_pricing.simulate_order, qui ne garde que le top-k et ne permet donc pas
d'en tirer un tierce ou un quinte.

D'OU VIENNENT LES PROBABILITES D'ENTREE — a lire avant d'interpreter une sortie :

  MOTEUR_TROT     p_win du logit conditionnel calibre (trot attele francais).
                  C'est un avis de modele, distinct du marche.
  MARCHE_DEVIG    cotes du marche, overround retire. Ce n'est PAS un avis :
                  c'est le marche lui-meme, remis en forme. Les combines qui en
                  sortent disent comment le marche voit la course, et ne portent
                  AUCUN avantage sur lui. Utile pour la structure, inutile pour
                  chercher une value.

PARIS COUVERTS, tels que la passerelle LONACI les enumere sur une Nationale :
  ordre impose  : Tierce, Quarte, Quinte
  sans ordre    : Trio, Multi, Pick 5, 2 sur 4, Couple Place, Jumele Gagnant,
                  Jumele Place
  simples       : Simple Gagnant, Simple Place

    python3 tools/apex_turf_combines.py course --date 08102026 --course R4C6
    python3 tools/apex_turf_combines.py course --date 08102026 --course R4C6 --coupon

Depend de numpy, deja requis par apex_turf_pricing.
"""
import argparse, itertools, json, sys, urllib.request
import datetime as dt
from collections import Counter

try:
    import numpy as np
except ImportError:
    np = None

API = "https://online.turfinfo.api.pmu.fr/rest/client/1/programme"
SIMS = 60_000
GRAINE = 20263

# Taux de non-terminaison MESURES (meme source que le WORM). Un cheval qui ne
# termine pas ne peut etre ni gagnant ni place : il doit sortir du tirage, sans
# quoi tous les combines sont surestimes.
NON_TERMINAISON = {"TROT_ATTELE": 0.212, "TROT_MONTE": 0.287}
NON_TERMINAISON_DEFAUT = 0.05      # galop : les non-partants sont deja retires

MOTEUR_CALIBRE = {"TROT_ATTELE"}   # seule discipline ou p_win vient d'un modele


def _get(url, tries=3):
    import time
    for i in range(tries):
        try:
            r = urllib.request.Request(url, headers={"User-Agent": "APEX-TURF/1.0"})
            with urllib.request.urlopen(r, timeout=30) as f:
                return json.load(f)
        except Exception as e:
            if i == tries - 1:
                return {"_erreur": f"{type(e).__name__}: {e}"}
            time.sleep(2 ** i)


def charger_course(ddmmyyyy, code):
    """Partants, cotes et discipline d'une course du programme francais."""
    rn, cn = code.upper().lstrip("R").split("C")
    prog = _get(f"{API}/{ddmmyyyy}")
    if "_erreur" in prog:
        return None, prog["_erreur"]
    info = None
    for u in (prog.get("programme", {}).get("reunions") or []):
        if str(u.get("numOfficiel")) != rn:
            continue
        for c in (u.get("courses") or []):
            if str(c.get("numExterne")) == cn:
                info = dict(libelle=c.get("libelle"), discipline=c.get("specialite"),
                            distance=c.get("distance"), statut=c.get("statut"),
                            hippodrome=(u.get("hippodrome") or {}).get("libelleCourt"),
                            pays=(u.get("pays") or {}).get("code"),
                            depart=c.get("heureDepart"))
    if not info:
        return None, f"{code} absente du programme du {ddmmyyyy}"
    d = _get(f"{API}/{ddmmyyyy}/R{rn}/C{cn}/participants")
    if "_erreur" in d:
        return None, d["_erreur"]
    partants = []
    for p in (d.get("participants") or []):
        if p.get("statut") != "PARTANT":
            continue
        cote = (p.get("dernierRapportDirect") or {}).get("rapport")
        if not cote:
            continue
        partants.append(dict(num=p["numPmu"], nom=p.get("nom"), cote=float(cote),
                             driver=p.get("driver")))
    if len(partants) < 3:
        return None, f"{len(partants)} partant(s) avec cote : trop peu pour un combine"
    info["partants"] = partants
    return info, None


def probas_marche(partants):
    """Cotes -> probabilites, overround retire. Le marche, pas un avis sur lui."""
    s = sum(1 / p["cote"] for p in partants)
    return [(1 / p["cote"]) / s for p in partants]


def simuler_arrivees(p_win, non_finish, sims=SIMS, profondeur=5, graine=GRAINE):
    """
    Arrivees completes par Plackett-Luce, non-terminaison comprise.

    Renvoie un tableau (sims, profondeur) d'INDICES de partants ; -1 quand la
    position n'a pas pu etre remplie (tout le champ non classe).
    """
    if np is None:
        raise RuntimeError("numpy requis")
    rng = np.random.default_rng(graine)
    w = np.asarray(p_win, dtype=float)
    n = len(w)
    log_w = np.log(np.maximum(w, 1e-12))
    dispo = ~(rng.random((sims, n)) < np.asarray(non_finish, dtype=float)[None, :])
    k = min(profondeur, n)
    arr = np.full((sims, k), -1, dtype=int)
    lignes = np.arange(sims)
    for pos in range(k):
        cles = log_w[None, :] + rng.gumbel(size=(sims, n))
        cles = np.where(dispo, cles, -np.inf)
        choisi = np.argmax(cles, axis=1)
        vide = ~dispo.any(axis=1)
        arr[:, pos] = np.where(vide, -1, choisi)
        arr_ok = ~vide
        dispo[lignes[arr_ok], choisi[arr_ok]] = False
    return arr


def _erreur_mc(p, sims):
    """Erreur-type Monte-Carlo. Distincte de l'incertitude du MODELE, qui est
    plus grande et que cette fonction ne pretend pas mesurer."""
    return (p * (1 - p) / sims) ** 0.5


def combines(arr, nums, sims, top=8):
    """
    Compte les combinaisons gagnantes sur les arrivees simulees, pari par pari.
    Les numeros rendus sont ceux des partants, pas des indices.
    """
    def nm(i):
        return nums[i]

    res = {}

    def ajoute(cle, compteur, libelle, ordre):
        lignes = []
        for comb, c in compteur.most_common(top):
            p = c / sims
            lignes.append(dict(combinaison=comb, p=round(p, 5),
                               erreur_mc=round(_erreur_mc(p, sims), 5),
                               cote_juste=(round(1 / p, 1) if p > 0 else None)))
        res[cle] = dict(libelle=libelle, ordre_impose=ordre, combinaisons=lignes)

    valides = arr[(arr[:, 0] >= 0)]
    n_ok = len(valides)
    if n_ok == 0:
        return res

    # --- ordre impose
    for k, (cle, lib) in enumerate([("tierce", "Tiercé"), ("quarte", "Quarté"),
                                    ("quinte", "Quinté")], start=3):
        if arr.shape[1] < k:
            continue
        sous = valides[:, :k]
        sous = sous[(sous >= 0).all(axis=1)]
        ajoute(cle, Counter("-".join(nm(i) for i in r) for r in sous), lib, True)

    # --- sans ordre
    for k, (cle, lib) in enumerate([("trio", "Trio"), ("_q4", None),
                                    ("pick5", "Pick 5")], start=3):
        if lib is None or arr.shape[1] < k:
            continue
        sous = valides[:, :k]
        sous = sous[(sous >= 0).all(axis=1)]
        ajoute(cle, Counter("-".join(sorted((nm(i) for i in r), key=int))
                            for r in sous), lib, False)

    # Jumele Gagnant : les deux premiers, sans ordre
    sous = valides[:, :2]
    sous = sous[(sous >= 0).all(axis=1)]
    ajoute("jumele_gagnant",
           Counter("-".join(sorted((nm(i) for i in r), key=int)) for r in sous),
           "Jumelé Gagnant", False)

    # Couple Place / Jumele Place : deux chevaux parmi les TROIS premiers.
    # Chaque arrivee contient trois paires gagnantes, pas une : on les compte
    # toutes, sinon la probabilite d'une paire donnee serait divisee par trois.
    if arr.shape[1] >= 3:
        sous = valides[:, :3]
        sous = sous[(sous >= 0).all(axis=1)]
        cpt = Counter()
        for r in sous:
            for a, b in itertools.combinations(sorted((nm(i) for i in r), key=int), 2):
                cpt[f"{a}-{b}"] += 1
        ajoute("couple_place", cpt, "Couplé Placé / Jumelé Placé", False)

    # Multi en 4 : les quatre premiers, sans ordre. LONACI paie aussi le Multi
    # en 5, 6 et 7 a des rapports differents ; seul le 4 est calcule ici, et c'est
    # dit plutot que suppose equivalent.
    if arr.shape[1] >= 4:
        sous = valides[:, :4]
        sous = sous[(sous >= 0).all(axis=1)]
        ajoute("multi4", Counter("-".join(sorted((nm(i) for i in r), key=int))
                                 for r in sous), "Multi (en 4)", False)

    # 2 sur 4 : deux chevaux parmi les QUATRE premiers
    if arr.shape[1] >= 4:
        sous = valides[:, :4]
        sous = sous[(sous >= 0).all(axis=1)]
        cpt = Counter()
        for r in sous:
            for a, b in itertools.combinations(sorted((nm(i) for i in r), key=int), 2):
                cpt[f"{a}-{b}"] += 1
        ajoute("deux_sur_quatre", cpt, "2 sur 4", False)

    return res


def simples(arr, nums, sims):
    """Simple Gagnant et Simple Place (3 premiers), sur les memes arrivees."""
    g = Counter(nums[i] for i in arr[:, 0] if i >= 0)
    p = Counter()
    k = min(3, arr.shape[1])
    for r in arr[:, :k]:
        for i in set(r):
            if i >= 0:
                p[nums[i]] += 1
    return (
        [dict(num=n, p=round(c / sims, 5), cote_juste=round(sims / c, 1))
         for n, c in g.most_common()],
        [dict(num=n, p=round(c / sims, 5), cote_juste=round(sims / c, 1))
         for n, c in p.most_common()])


def analyser(info, sims=SIMS):
    disc = info["discipline"]
    partants = info["partants"]
    nums = [str(p["num"]) for p in partants]
    pw = probas_marche(partants)
    source = "MARCHE_DEVIG"
    nt = [NON_TERMINAISON.get(disc, NON_TERMINAISON_DEFAUT)] * len(partants)
    arr = simuler_arrivees(pw, nt, sims=sims)
    sg, sp = simples(arr, nums, sims)
    return dict(
        course=dict(libelle=info["libelle"], hippodrome=info["hippodrome"],
                    discipline=disc, distance=info["distance"],
                    pays=info["pays"], statut=info["statut"],
                    n_partants=len(partants)),
        source_probabilites=source,
        moteur_calibre=(disc in MOTEUR_CALIBRE),
        # Dire « moteur calibre : OUI » a cote de probabilites de MARCHE etait
        # trompeur : le moteur existe pour cette discipline, il n'est pas
        # employe ici. Le faire tourner demanderait les variables de forme que
        # la chaine T1-T4 construit, et que ce chemin ne porte pas.
        etat_moteur=("existe pour cette discipline, NON UTILISE ici "
                     "(probabilites de marche)" if disc in MOTEUR_CALIBRE
                     else f"aucun moteur calibre pour {disc}"),
        taux_non_terminaison=nt[0],
        simulations=sims,
        partants=[dict(num=str(p["num"]), nom=p["nom"], cote=p["cote"],
                       p_marche=round(q, 4), driver=p.get("driver"))
                  for p, q in zip(partants, pw)],
        simple_gagnant=sg, simple_place=sp,
        combines=combines(arr, nums, sims))


# --------------------------------------------------------------------- rendu

AVERTISSEMENT_INDICATIF = (
    "INDICATIF. Probabilites calculees sur des arrivees simulees, jamais par "
    "multiplication de marginales. `autorite_pari = false` : aucune mise n'est "
    "recommandee ici.")

AVERTISSEMENT_MARCHE = (
    "Les probabilites d'entree sont les COTES DU MARCHE, overround retire. Ce "
    "n'est pas un avis distinct du marche : c'est le marche remis en forme. Ces "
    "combines disent comment le marche voit la course. Ils ne portent AUCUN "
    "avantage sur lui, et il ne faut pas les lire comme une value.")

AVERTISSEMENT_COUPON = (
    "COUPON ASSUME — ce n'est pas une sortie validee. Les deux gates de pari turf "
    "sont FERMEES par le backtest : trot ROI -4,58 % sur 118 paris, obstacle "
    "-89,05 % sur 21. Ces combinaisons sont les plus PROBABLES selon la "
    "simulation, ce qui n'est pas la meme chose que les plus RENTABLES : la plus "
    "probable est aussi la moins payee. Aucune mesure ne soutient que jouer ceci "
    "soit profitable. Vous l'avez demande en connaissance de cause.")

# Mises de base LONACI relevees sur la passerelle (Nationale, 08/10/2026), en FCFA.
MISES_LONACI = {"tierce": 300, "quarte": 300, "quinte": 300, "multi4": 350,
                "pick5": 400, "trio": 400, "deux_sur_quatre": 500,
                "couple_place": 500, "jumele_gagnant": 500}


def rendre(a, coupon=False, top=6):
    c = a["course"]
    L = [f"=== {c['libelle']} · {c['hippodrome']} ({c['pays']})",
         f"    {c['discipline']} {c['distance']}m · {c['n_partants']} partants · "
         f"statut {c['statut']}",
         f"    probabilites : {a['source_probabilites']} · "
         f"moteur : {a['etat_moteur']} · "
         f"{a['simulations']:,} simulations · "
         f"non-terminaison {a['taux_non_terminaison']:.1%}".replace(",", " "),
         ""]
    if a["source_probabilites"] == "MARCHE_DEVIG":
        L += ["    " + AVERTISSEMENT_MARCHE, ""]

    L.append("--- SIMPLE GAGNANT (p simulee, cote juste)")
    for x in a["simple_gagnant"][:6]:
        L.append(f"      {x['num']:>3}  p={x['p']:6.1%}  juste {x['cote_juste']}")
    L.append("--- SIMPLE PLACE (3 premiers)")
    for x in a["simple_place"][:6]:
        L.append(f"      {x['num']:>3}  p={x['p']:6.1%}  juste {x['cote_juste']}")

    for cle, bloc in a["combines"].items():
        L += ["", f"--- {bloc['libelle']}"
                  + ("  (ordre impose)" if bloc["ordre_impose"] else "  (sans ordre)")]
        for x in bloc["combinaisons"][:top]:
            L.append(f"      {x['combinaison']:<22} p={x['p']:7.2%} "
                     f"± {x['erreur_mc']:.2%}   juste {x['cote_juste']}")
    L += ["", "    " + AVERTISSEMENT_INDICATIF]

    if coupon:
        L += ["", "=" * 72, "COUPON", "=" * 72, AVERTISSEMENT_COUPON, ""]
        for cle, bloc in a["combines"].items():
            mise = MISES_LONACI.get(cle)
            if not mise or not bloc["combinaisons"]:
                continue
            n = 3 if bloc["ordre_impose"] else 2
            choix = bloc["combinaisons"][:n]
            total = mise * len(choix)
            L.append(f"  {bloc['libelle']} — mise de base {mise} FCFA")
            for x in choix:
                L.append(f"      {x['combinaison']:<22} p={x['p']:7.2%}  "
                         f"juste {x['cote_juste']}")
            L.append(f"      -> {len(choix)} combinaison(s), {total} FCFA")
            L.append("")
        tot = sum(MISES_LONACI.get(k, 0) * (3 if b["ordre_impose"] else 2)
                  for k, b in a["combines"].items()
                  if MISES_LONACI.get(k) and b["combinaisons"])
        L.append(f"  TOTAL DU COUPON : {tot} FCFA")
        L.append("  Rappel : la combinaison la plus probable est la moins payee.")
    return "\n".join(L)


def cmd_course(a):
    if np is None:
        print("numpy requis.", file=sys.stderr)
        return 1
    ddmmyyyy = a.date or dt.datetime.now(dt.timezone.utc).strftime("%d%m%Y")
    info, err = charger_course(ddmmyyyy, a.course)
    if err:
        print(f"REFUS : {err}", file=sys.stderr)
        return 1
    res = analyser(info, sims=a.sims)
    print(rendre(res, coupon=a.coupon, top=a.top))
    if a.json:
        json.dump(res, open(a.json, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        print(f"\n-> {a.json}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="apex_turf_combines", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = p.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("course", help="combines d'une course")
    c.add_argument("--date", help="DDMMYYYY")
    c.add_argument("--course", required=True, help="R4C6")
    c.add_argument("--sims", type=int, default=SIMS)
    c.add_argument("--top", type=int, default=6)
    c.add_argument("--coupon", action="store_true",
                   help="ajouter un coupon assume (voir l'avertissement)")
    c.add_argument("--json", help="ecrire le resultat structure ici")
    a = p.parse_args(argv)
    return {"course": cmd_course}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())


def fragment_html(ddmmyyyy, codes, coupon=False, top=5, sims=30_000):
    """
    Section HTML des combines, a joindre au digest d'une phase Nationale.

    Ne leve JAMAIS : un combine qui echoue ne doit pas faire perdre le digest,
    qui porte le releve scelle. L'echec est ecrit dans la section, pas avale.
    """
    if np is None:
        return ("<p style='color:#a00'>Combines indisponibles : numpy absent.</p>")
    out = []
    for code in codes:
        try:
            info, err = charger_course(ddmmyyyy, code)
            if err:
                out.append(f"<p style='color:#a00'><b>{code}</b> — combines "
                           f"impossibles : {err}</p>")
                continue
            a = analyser(info, sims=sims)
        except Exception as e:
            out.append(f"<p style='color:#a00'><b>{code}</b> — combines "
                       f"impossibles : {type(e).__name__}: {e}</p>")
            continue
        c = a["course"]
        out.append(
            f"<h3 style='margin:18px 0 4px'>{code} — {c['libelle']} "
            f"<span style='font-weight:400;color:#666'>({c['hippodrome']})</span></h3>"
            f"<p style='margin:0 0 8px;color:#555;font-size:13px'>"
            f"{c['discipline']} {c['distance']}m · {c['n_partants']} partants · "
            f"{a['simulations']:,} simulations · non-terminaison "
            f"{a['taux_non_terminaison']:.1%} · probabilites "
            f"<b>{a['source_probabilites']}</b> · moteur : {a['etat_moteur']}"
            "</p>".replace(",", " "))
        if a["source_probabilites"] == "MARCHE_DEVIG":
            out.append("<p style='background:#fff7e6;border-left:3px solid #d48806;"
                       f"padding:7px 10px;margin:0 0 10px;font-size:13px'>"
                       f"{AVERTISSEMENT_MARCHE}</p>")
        out.append("<table cellpadding='5' cellspacing='0' style='border-collapse:"
                   "collapse;font-size:13px;width:100%'><tr style='background:#f0f0f0;"
                   "text-align:left'><th>Pari</th><th>Combinaison</th><th>p</th>"
                   "<th>± MC</th><th>Cote juste</th></tr>")
        for cle, bloc in a["combines"].items():
            for i, x in enumerate(bloc["combinaisons"][:top]):
                nom = (f"<b>{bloc['libelle']}</b>"
                       + ("<br><span style='color:#888'>ordre imposé</span>"
                          if bloc["ordre_impose"] else "")) if i == 0 else ""
                out.append(
                    f"<tr style='border-bottom:1px solid #eee'><td>{nom}</td>"
                    f"<td><code>{x['combinaison']}</code></td>"
                    f"<td>{x['p']:.2%}</td><td style='color:#888'>±{x['erreur_mc']:.2%}</td>"
                    f"<td>{x['cote_juste']}</td></tr>")
        out.append("</table>")
        if coupon:
            tot = 0
            lignes = []
            for cle, bloc in a["combines"].items():
                mise = MISES_LONACI.get(cle)
                if not mise or not bloc["combinaisons"]:
                    continue
                n = 3 if bloc["ordre_impose"] else 2
                ch = bloc["combinaisons"][:n]
                tot += mise * len(ch)
                lignes.append(
                    f"<tr style='border-bottom:1px solid #eee'><td><b>{bloc['libelle']}"
                    f"</b></td><td>" + "<br>".join(f"<code>{x['combinaison']}</code> "
                                                   f"<span style='color:#888'>{x['p']:.2%}</span>"
                                                   for x in ch)
                    + f"</td><td>{mise} FCFA × {len(ch)}</td>"
                      f"<td><b>{mise * len(ch)}</b></td></tr>")
            out.append("<h4 style='margin:16px 0 4px'>Coupon</h4>")
            out.append("<p style='background:#fff1f0;border-left:3px solid #cf1322;"
                       f"padding:8px 10px;margin:0 0 8px;font-size:13px'>"
                       f"{AVERTISSEMENT_COUPON}</p>")
            out.append("<table cellpadding='5' cellspacing='0' style='border-collapse:"
                       "collapse;font-size:13px;width:100%'>" + "".join(lignes)
                       + f"<tr><td colspan='3' style='text-align:right'><b>TOTAL</b>"
                         f"</td><td><b>{tot} FCFA</b></td></tr></table>")
    if not out:
        return ""
    return ("<hr style='margin:22px 0;border:0;border-top:1px solid #ddd'>"
            "<h2 style='margin:0 0 4px'>Combinés — paris proposés par LONACI</h2>"
            "<p style='color:#666;font-size:13px;margin:0 0 10px'>"
            "Calculés sur des arrivées simulées, jamais par multiplication de "
            "probabilités marginales : les places ne sont pas indépendantes.</p>"
            + "".join(out))
