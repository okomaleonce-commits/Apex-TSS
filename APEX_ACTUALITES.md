# APEX-ACTUALITÉS — sources & convention de fraîcheur

## Convention de temps (NON négociable)

Le « **H-** » compte **en minutes/heures AVANT le coup d'envoi**, jamais en jours.

- **H-1 = 1 heure avant** le coup d'envoi.
- **H-60 = 60 minutes avant** le coup d'envoi.
- Donc **H-1 ≡ H-60** (même instant : 1 h = 60 min).
- Les passages **lointains** se notent en **jours** : **J-3** (jeudi pour un match du week-end), **J-2** (vendredi), **J-1** (veille). Ne jamais écrire « H-36 » ou « H-60 » pour parler de jours.

## Règle de fraîcheur

**Toute actualité = les DERNIÈRES nouvelles.**

- Chaque donnée est **horodatée** ; on garde **la plus récente**. Une info périmée est **écartée**, jamais recyclée.
- Les **compositions** sortent ~**H-1 / H-60** (≈ 1 h avant). C'est **à cet instant** que l'actu compos/blessures devient réellement fiable → les passages proches du coup d'envoi priment.
- Donnée pas encore disponible = écrite **ABSENT** (anti-invention), puis **rafraîchie** dès qu'elle sort.
- Aucun diagnostic mental ; uniquement des **proxys observables**.

## Cadence des passages

| Moment | Rôle |
|---|---|
| **J-3 / J-2** (jeu 20h, ven 15h) | Cadre : calendrier réel, contexte disponible à ce stade (enjeu, forme, blessures longues). Compos **ABSENT**. |
| **J-1 / matin J** (sam 09h, dim 09h) | Rafraîchissement : blessures/suspensions confirmées, probables. |
| **H-60 (60 min avant)** | **Compos officielles + dernières blessures.** C'est l'actu qui compte vraiment. Capté par le hook APEX-MI H-60 dès qu'un scan tourne dans la dernière heure du match. |

## Sources (par fiabilité décroissante)

1. **Officiel** — API-Football (`tools/apex_apifootball.py` : compos, blessures, suspensions, cotes horodatées), sites officiels des clubs/ligues. **Tier le plus haut.**
2. **Presse établie** — BBC Sport, L'Équipe, Sportsmole, previews de ligue (via WebSearch / Firecrawl).
3. **Agrégateurs / previews**.
4. **Social / tipster** — tier le plus bas, jamais décisif. Un déplacement Pinnacle ≠ un post Telegram.

Essaims d'observation (ne pricent pas, n'inventent pas) : `apex-mi-*` / `apex-bi-*` (football), `apex-tmi-*` / `apex-tbi-*` (turf). Outils : `tools/apex_mi.py`, `tools/apex_turf_mi.py`.

**Pas des sources d'actu** : Gmail/connecteur (= canal d'envoi), SharpAPI/Infersports (= cotes/marché). **Passes décisives** : aucune source disponible → toujours ABSENT.
