---
name: apex-turf-worm-scanner
description: Scanner hippique « ver informationnel » APEX-TURF-WORM, indépendant des autres protocoles APEX (chaîne S1-S8 football, hockey, apex-turf-team). DÉCLENCHER pour un scan de journée orienté anomalies sur les courses — trajectoire de cote entre deux passages, non-partants, favori dominant, outsider confirmé par le mouvement — sur la fenêtre APEX 08:00→07:59, en historisant chaque passage et en le comparant au précédent. Outil mécanique tools/apex_turf_worm.py (window, scan, report, bilan). Le palier maximal est SURVEILLER : les deux gates de pari turf sont fermées par le backtest. Ne remplace pas apex-turf-team pour un pronostic calibré sur une course précise.
---

# APEX-TURF-WORM — scanner « ver informationnel » hippique

Protocole **autonome et séparé**. Il ne cherche pas la value : il cherche des **anomalies**
dans la trajectoire du marché entre deux passages.

## Boucle

```
DISCOVER → COLLECT → NORMALIZE → STORE → COMPARE → ANALYZE → RANK → REPORT
```

```bash
python3 tools/apex_turf_worm.py window
python3 tools/apex_turf_worm.py scan
python3 tools/apex_turf_worm.py scan --lonaci          # perimetre officiel LONACI
python3 tools/apex_turf_worm.py scan --date 02102026 --max-courses 20
python3 tools/apex_turf_worm.py report
python3 tools/apex_turf_worm.py bilan
python3 tools/apex_turf_mi.py worm-hook --within 30
```

## Fenêtre APEX

Journée de **08:00:00 à 07:59:59** le lendemain, dans `APEX_TIMEZONE` (variable
d'environnement, jamais codée en dur ; défaut UTC). Avant 08:00 on est encore la veille.

## Cycle permanent

Un passage par heure. Chaque passage **compare** au relevé précédent du même jour et
**n'écrase jamais** : `data/turf_worm/snapshots/<jour>.jsonl` est append-only.

**En pari mutuel la trajectoire est *la* donnée**, pas une donnée parmi d'autres : il n'y
a qu'une cote, et elle est le reflet direct des enjeux déjà misés. Un seul relevé ne dit
rien ; deux relevés disent où va l'argent.

Le premier passage de la journée sort `PREMIER_PASSAGE` sur toutes les courses. C'est
correct, pas un échec.

## Moteurs d'anomalie — ce qui classe la course

| Moteur | Mesure | Scoring |
|---|---|---|
| `derive` | variation de chaque cote entre deux passages | percentile empirique |
| `non_partants` | retraits, nouveaux depuis le passage précédent | 45 par retrait + part du champ |
| `favori_dominant` | `p1` démarginée et écart au deuxième | moyenne de deux percentiles |
| `outsider` | partant sous 12 % vers qui l'argent va | percentile de son resserrement |

### Le score est un percentile, pas une formule

La première version de `favori_dominant` notait `p1 · 180 + ecart · 220` et sortait
**100/100 sur presque toutes les courses**. Un signal qui s'allume partout ne porte
aucune information. Le score est désormais la place de la valeur dans la distribution
réelle de la même métrique **dans la même discipline** :
`tools/params/turf_worm_quantiles.json`, mesuré sur **14 861 courses de trot** et
**2 952 d'obstacle**.

`percentile_score()` sort `UNAVAILABLE` quand la discipline n'a pas ses propres
quantiles. **Les quantiles d'une discipline ne sont jamais transposés à une autre**, même
quand ils se ressemblent.

### Contexte structurel — ce qui ne classe pas

`contexte.non_terminaison` donne le taux de base mesuré (**21,2 %** attelé, **28,7 %**
monté) et l'attendu de non-finissants. Il est constant à discipline et champ donnés, donc
sans pouvoir discriminant : le faire entrer dans le rang mettait toutes les courses
d'attelé à 51/100. Il informe la lecture, il ne classe pas.

En obstacle il sort `UNAVAILABLE` : le taux y dépend du champ et du terrain, et le
pricing revient à la cellule statistique.

## Moteurs structurellement indisponibles

| Moteur | Statut | Motif |
|---|---|---|
| `sharp_books` | `UNAVAILABLE_STRUCTUREL` | pari mutuel : ni Pinnacle ni book asiatique, une seule cote |
| `dispersion_inter_books` | `UNAVAILABLE_STRUCTUREL` | une seule cote, rien à disperser |
| `steam_multi_books` | `UNAVAILABLE_STRUCTUREL` | pas de books à synchroniser |
| `reverse_line_movement` | `UNAVAILABLE_STRUCTUREL` | le PMU ne publie pas le % de parieurs par partant |
| `volume_echange` | `UNAVAILABLE` | masse des enjeux non collectée — disponible en principe, non branchée |

**Jamais estimés, jamais omis.** Une case nommée `UNAVAILABLE_STRUCTUREL` se défend ; une
case vide se remplit un jour par une approximation.

## Provenance obligatoire

Toute valeur porte `OBSERVED`, `CALCULATED`, `UNAVAILABLE` ou `UNAVAILABLE_STRUCTUREL`.
**Ne jamais transformer une absence en fait.**

## Décision — palier maximal SURVEILLER

Le WORM football a `JOUER` et `JOUER_PETIT`. **Pas ici.** Les deux gates de pari turf sont
fermées par le backtest : trot **ROI −4,58 %** sur 118 paris IC95 [−44,04 % ; +42,06 %],
obstacle **−89,05 %** sur 21. Chaque ligne porte `autorite_pari = false` et `unites = 0`.

| Condition | Décision |
|---|---|
| score ≥ 70 | `SURVEILLER_FORT` |
| score ≥ 45 | `SURVEILLER` |
| score < 45 | `RIEN_A_SIGNALER` |
| premier passage | `PREMIER_PASSAGE` |
| discipline sans moteur calibré | `HORS_PERIMETRE` |

**Le plat sort toujours `HORS_PERIMETRE`** : aucun moteur n'est calibré pour lui, et les
coefficients du trot ou de l'obstacle ne sont pas transposables — `cf` vaut 0,0 en trot et
0,4 en obstacle, la règle du top 3 change de camp. Hors discipline, le résultat n'est pas
moins précis, il est de signe faux.

## Statuts de course — liste blanche

`PROGRAMMEE`, `ROUGE_AUX_PARTANTS`, `DEPART_IMMINENT`, `A_PARTIR`,
`COURSE_ARRETEE_PROVISOIREMENT`.

Une liste **noire** laisserait passer tout statut non anticipé : la première version
excluait `ARRIVEE_DEFINITIVE` et `FIN_COURSE`, et scannait donc des courses marquées
`ARRIVEE_DEFINITIVE_COMPLETE` et `ARRIVEE_PROVISOIRE`, déjà terminées.

## Le piège d'échelle de la dérive

Les quantiles de dérive sont mesurés de la **cote de référence du matin à la cote finale**
— une journée entière. Le WORM compare deux passages **horaires**. L'échelle de la mesure
est donc plus grande que celle du signal, et le score de dérive **sous-estime** le
mouvement horaire. Le champ `echelle` du moteur le porte ; le citer dans toute lecture.

À re-estimer sur les snapshots du WORM lui-même dès **200 paires de passages
consécutifs**. Jusque-là, ne jamais présenter un score de dérive comme calibré.

Ordre de grandeur utile : en trot, la variation médiane d'un partant sur une journée est de
**34,8 %**, le q75 de 69 %, le q90 de 132 %. Une variation de 12 % est sous le premier
quartile — c'est du bruit. Le seuil de signalement est à 18 %.

## Voie entièrement autonome : GitHub Actions

`.github/workflows/apex-turf-worm.yml` — passage horaire **sans connecteur et sans
modèle**, toutes les heures de 08h à 19h (minute 17).

```
rendu du programme LONACI → périmètre → scan --lonaci → pont MI → email SMTP
```

C'est le seul endroit où la chaîne peut tourner seule, et pour deux raisons mesurées le
07/10/2026 : depuis un conteneur claude.ai les ports SMTP 587, 465 et 25 **expirent tous**
(la sortie passe par un proxy HTTPS, pas par du TCP brut) et Chromium **refuse la CA de ce
proxy** (`ERR_CERT_AUTHORITY_INVALID`). Sur un runner GitHub, ni l'un ni l'autre.

### Le rendu du programme : `tools/apex_turf_lonaci_render.py`

Deux chemins, essayés dans cet ordre, et la sortie dit **toujours** lequel a servi :

| Chemin | Conteneur claude.ai | Runner GitHub |
|---|---|---|
| **1. passerelle JSON** (`turf_gateway` lu dans `assets/config/config.json`) | injoignable — reset au ClientHello | ✅ **répond en ~6 s** |
| **2. navigateur** (Playwright + Chromium) | `ERR_CERT_AUTHORITY_INVALID` | rend la page en ~25 s |

**C'est la passerelle qui sert en production** : structurée, six fois plus rapide, et elle
donne plus que la page (hippodrome, libellé, heure, partants, distance). Le navigateur
reste en repli.

### La structure de la passerelle, observée le 07/10/2026

Elle n'avait jamais pu l'être depuis un conteneur. Le premier passage sur le runner l'a
donnée :

```
$ list[4]                                    ← les réunions du programme
  [0] int_Numero '1'   str_Name 'ENGHIEN'   Course list[8]
        [0] int_Numero            '1'
            Condition             'PRIX DES GOBELINS'
            str_City              'ENGHIEN'
            dt_Course_Date        '2026-10-07 11:55:00'
            by_Participant_Number '18'
            Int_Distance          '2875'
```

**Le code `R#C#` n'est pas un champ : il se construit**, `R{réunion.int_Numero}C{course.int_Numero}`.
La première version le cherchait comme une chaîne déjà formée, d'où le « répondu, mais
aucun code reconnu ».

Et la **page** affiche « R1 » et « C1 » sur **deux lignes séparées**, jamais « R1C1 » :
`_recoller_codes()` les rejoint. Le rendu marchait depuis le début.

**Si les deux échouent, le script sort en erreur sans rien écrire.** Il ne faut surtout pas
qu'un fichier vide devienne un périmètre : `scope` refuserait de toute façon, mais mieux
vaut échouer là, avec le motif.

Réserve honnête : la forme du JSON de la passerelle **n'a jamais pu être observée**. Le
convertisseur cherche les champs de façon défensive et rend `None` s'il ne trouve aucun
code `R#C#`, pour basculer sur le navigateur plutôt que de produire un périmètre devine.

### Secrets à déclarer

`Settings → Secrets and variables → Actions` du dépôt :

| Secret | Pour Gmail |
|---|---|
| `WORM_SMTP_HOST` | `smtp.gmail.com` |
| `WORM_SMTP_PORT` | `587` |
| `WORM_SMTP_USER` | l'adresse d'envoi |
| `WORM_SMTP_PASS` | un **mot de passe d'application** à 16 caractères, pas celui du compte (exige la validation en deux étapes) |
| `WORM_EMAIL_TO` | le destinataire |
| `WORM_EMAIL_FROM` | optionnel, défaut `WORM_SMTP_USER` |

Variable facultative : `APEX_TIMEZONE`, défaut `Africa/Abidjan`.

Sans ces secrets, le workflow tourne quand même et le résumé affiche
« **secrets `WORM_SMTP_*` absents** — digest construit, non envoyé ». Jamais d'envoi
silencieusement raté.

### Snapshots entre passages

Ils partent en **artefacts**, pas en commits : douze commits par jour rendraient
l'historique illisible. L'étape *Reprendre les snapshots du passage précédent* les
retélécharge — sans quoi il n'y aurait pas de COMPARE et chaque passage repartirait de
zéro, c'est-à-dire sans trajectoire, c'est-à-dire sans signal.

### Ce qui est éprouvé, et ce qui ne l'est pas

Éprouvé avec les commandes exactes du workflow (07/10/2026) : périmètre 24 → 8 courses,
scan 57 → 35 à venir → **5 retenues**, pont H-30, résumé GitHub, et le message d'envoi
dans les deux cas (secrets absents, puis hôte invalide).

**Éprouvé sur le runner le 07/10/2026** (passage `37636418885`, conclusion `success`) :
rendu par la **passerelle**, 24 courses au programme LONACI → 8 dans le périmètre → scan
57 découvertes → 3 à venir retenues → 1 signal → **`EMAIL ENVOYE par SMTP`**, artefacts
publiés. La chaîne tourne seule, sans connecteur ni modèle.

Il a fallu trois passages pour y arriver, et les deux premiers ont servi : le premier a
révélé que la passerelle répondait, le second a imprimé sa structure. Aucune des deux
corrections n'est une supposition.

## Équipe d'agents

`apex-turf-worm-conductor` lance le scan, puis délègue : `apex-turf-worm-market`
(trajectoire, retraits), `apex-turf-worm-anomaly` (structure du marché),
`apex-turf-worm-live` (courses à H-30 et moins). Chaque agent **lit** le JSONL ou le
rapport — il ne recalcule pas et n'invente rien.

## Activation APEX-TURF-MI en fin de scan

```bash
python3 tools/apex_turf_mi.py worm-hook --within 30
```

Au football le croisement porte sur l'UPSET. Ici il porte sur ce que le pari mutuel rend
visible et qui n'a pas d'équivalent :

```
OUTSIDER_WATCH    = 0,55 · outsider structurel (WORM) + 0,45 · confirmation (l'outsider se raccourcit)
FAVORI_WATCH      = 0,55 · favori dominant (WORM)     + 0,45 · confirmation (le favori se raccourcit)
NON_PARTANT_WATCH = 0,60 · retraits tardifs           + 0,40 · recomposition du marché
```

Les deux premiers sont les **miroirs exacts** du pont football (`UPSET_WATCH` et
`BLOWOUT_WATCH`, 0,55 / 0,45). Le troisième n'a **aucun équivalent football** : un retrait
à H-30 redistribue *tout* l'argent de la course, et le PMU le publie. Tri par le maximum
des trois, comme le pont football trie par le max de ses deux axes.

**Seul un raccourcissement compte comme confirmation.** Une dérive en sens inverse n'est
pas une confirmation faible, c'est une infirmation — d'où les statuts `*_FADING`.

Sorties : `data/turf_worm/mi/<jour>.json` et `reports/turf_worm/<jour>.mi.md`.

## Collecte

Source unique : l'API publique turfinfo du PMU. Pas de clé, pas de login, rien à
contourner. User-Agent identifiable, trois tentatives avec attente exponentielle.

## Périmètre LONACI — restreindre aux courses réellement au programme

```bash
python3 tools/apex_turf_lonaci.py scope --date DDMMYYYY --from-text <fichier>
python3 tools/apex_turf_lonaci.py show  --date DDMMYYYY
python3 tools/apex_turf_worm.py   scan  --lonaci
```

### Le fait qui rend la restriction simple

**LONACI emploie les mêmes codes `R#C#` que le PMU français.** Vérifié sur les 30 courses
du 02/10/2026 : chaque code français tombe sur le bon hippodrome et le bon nom de course
(`R1C4` = Vincennes Prix Ludovica, `R3C5` = Borély Prix des Camélias). Le périmètre se
réduit donc à une **liste de codes**.

### Trois conséquences mesurées, pas supposées

**1. L'heure n'est pas une clé de validation.** Sur 30 courses : 9 heures identiques,
13 écarts de 1 à 5 min, aucun au-delà. LONACI publie une heure programmée, le PMU une
`heureDepart`. La validation porte donc sur le **nom de la course**, avec une tolérance
d'heure de 6 min. Un nom discordant est signalé : le périmètre est peut-être périmé.

**2. La Nationale 3 est marocaine et absente de l'API française.** Les 8 courses `R9`
d'Anfa du 02/10 n'existent pas dans la source : ni partant, ni cote, ni rien. Elles
sortent `ABSENT_SOURCE`. Ce n'est pas un refus de périmètre, c'est une absence de données,
et le dire est la seule réponse honnête.

**3. Le plat domine le reste du programme.** Sur les 22 courses françaises du 02/10 :
**11 trot attelé, 1 trot monté, 1 obstacle, 9 plat**. Soit, avec les 8 marocaines,
**13 courses analysables sur 30** — dont 11 seulement en trot attelé, la seule discipline
dont le DCS atteint 82.

C'est la lecture la plus utile de ce module : **deux tiers du programme LONACI sont hors
de ce que les moteurs peuvent chiffrer.**

### Question ouverte, non tranchée : de qui sont les cotes ?

LONACI sert ses propres rapports par sa propre passerelle
(`api.lonacionline.flexbet-software.com`, endpoint `ws_web_mobile_rapport.jsp`). **Rien ne
prouve que sa masse d'enjeux soit celle du PMU français.** Or les moteurs sont calibrés sur
les cotes françaises, qu'ils utilisent comme **offset de marché**. Si les deux masses sont
distinctes, l'offset est celui d'un autre marché que celui où l'on joue.

Cette question n'est **pas** résolue : la passerelle LONACI est injoignable depuis le
réseau de cette session (le tunnel s'ouvre, le serveur coupe). Le champ `cotes_origine`
vaut donc `PMU_FRANCE` et `avertissement_masse` le dit, dans le fichier de périmètre comme
dans le rapport et dans l'email.

**Ne jamais présenter une analyse LONACI comme fondée sur les cotes LONACI** tant que la
comparaison n'a pas été faite. Pour la trancher : relever, sur une vingtaine de courses, le
rapport LONACI d'un Simple Gagnant et le rapport français du même cheval. Identiques =
masse commune, l'offset est bon. Différents = il faut les cotes LONACI, et ce module est
alors incomplet.

### D'où vient le programme LONACI

`https://pmu.lonacionline.ci/mobile/` — page **publique**, sans login. C'est une
application Angular : **un `curl` ne rend rien**, le texte doit venir d'un moteur de rendu
(session avec un outil de récupération de page, ou navigateur). La passerelle JSON serait
plus propre mais est injoignable d'ici.

`pmu.lonaci.ci`, que l'on cite parfois, **ne résout pas en DNS** — le domaine servant le
programme est `pmu.lonacionline.ci`.

Collecte : page publique, aucun `robots.txt` interdisant quoi que ce soit, User-Agent
identifiable, deux requêtes par passage. **Aucun contournement de login ni de CAPTCHA**, et
aucun appel aux points d'entrée de pari ou de compte de la passerelle — seulement la
lecture du programme.

### Sans périmètre, `--lonaci` ne scanne rien

Le scan **refuse** plutôt que de retomber sur le programme français entier : scanner tout
alors qu'on a demandé LONACI serait ignorer la demande silencieusement.


## Email — obligatoire à chaque passage

**Règle du dépôt (`CLAUDE.md`) : un passage sans email envoyé est INCOMPLET.** Elle vaut
ici comme pour le WORM football.

Deux voies, et le script dit toujours laquelle a servi :

**1. SMTP — autonome, sans modèle ni connecteur.** `envoyer_smtp()` envoie seul dès que
l'environnement porte `WORM_SMTP_HOST`, `WORM_SMTP_USER`, `WORM_SMTP_PASS` et
`WORM_EMAIL_TO` (plus `WORM_SMTP_PORT`, défaut 587, et `WORM_EMAIL_FROM`, défaut
l'utilisateur). STARTTLS obligatoire, jamais en clair. C'est **la seule voie pour une
Routine à session fraîche ou un cron**, où le connecteur Gmail n'existe pas.

> **Mesuré le 07/10/2026 : l'envoi SMTP ne fonctionne PAS depuis un conteneur
> claude.ai.** Les ports 587, 465 et 25 expirent tous vers `smtp.gmail.com` (en IPv4 ;
> l'IPv6 n'est pas routée). La sortie réseau de l'environnement passe par un proxy HTTPS,
> pas par du TCP brut. Renseigner les `WORM_SMTP_*` seuls ne suffit donc pas : il faudrait
> aussi élargir *Network access* dans les réglages de l'environnement, et rien ne garantit
> que le port 587 s'ouvre, le chemin de sortie étant orienté HTTPS.
>
> **Là où cette voie fonctionne vraiment : GitHub Actions.** Le workflow
> `.github/workflows/apex-turf-worm.yml` tourne sur un runner GitHub, où la sortie SMTP est
> ouverte. Y déclarer `WORM_SMTP_*` en *Actions secrets* rend l'envoi autonome sans aucun
> connecteur. En session claude.ai, c'est le connecteur Gmail qui reste la voie fiable.

**2. Connecteur Gmail — en session interactive.** Si le SMTP n'est pas configuré, le script
construit le digest, **n'envoie rien**, et le dit : il affiche les deux voies et les
chemins des fichiers à passer à `mcp__Gmail__send_message`.

Un envoi raté ne fait jamais perdre le snapshot du passage : `envoyer_smtp()` ne lève
jamais, elle retourne le motif.

```bash
python3 tools/apex_turf_worm.py email          # ecrit reports/turf_worm/<jour>.email.html
#   (scan l'appelle deja automatiquement en fin de passage)
```

Puis, en session :

```
mcp__Gmail__send_message
  to       = ["okoma.leonce@gmail.com"]
  subject  = contenu de reports/turf_worm/<jour>.email.subject.txt
  htmlBody = contenu de reports/turf_worm/<jour>.email.html
```

`build_email_html(day)` est aussi importable et renvoie `(sujet, html)`, comme
`apex_worm.build_email_html`. Le digest joint automatiquement le fragment
`reports/turf_worm/<jour>.mi.md` quand il existe.

**Preuve d'envoi = l'`id` / `threadId` Gmail** renvoyé par l'outil. Si le connecteur
s'est déconnecté, le recharger par `ToolSearch` avant d'envoyer. Pour un envoi 100 %
autonome par le cron, il faudrait renseigner `WORM_SMTP_*`.

## Sorties

- `data/turf_worm/snapshots/<jour>.jsonl` — historique horodaté append-only
- `reports/turf_worm/<jour>.md` — signaux, moteurs absents
- `reports/turf_worm/<jour>.mi.md` — activation H-30
- `reports/turf_worm/<jour>.email.html` + `.email.subject.txt` — digest à envoyer

## Limites connues, non corrigées

1. **Dérive non calibrée à l'échelle horaire** (voir ci-dessus).
2. **Pas de quantiles de dérive en obstacle** : aucune cote de référence du matin
   collectée. Le moteur sort `UNAVAILABLE`.
3. **Masse des enjeux non branchée** : `volume_echange` reste `UNAVAILABLE`.
4. **`bilan` ne mesure aucune rentabilité**, et ne peut pas : aucun pari n'est émis. Il
   sert à voir si les anomalies signalées correspondent à quelque chose, sur un n qui
   reste à construire.
