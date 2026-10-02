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

## Email — obligatoire à chaque passage

**Règle du dépôt (`CLAUDE.md`) : un passage sans email envoyé est INCOMPLET.** Elle vaut
ici comme pour le WORM football.

Les secrets `WORM_SMTP_*` ne sont pas configurés : le script **construit** le digest et
**n'envoie rien**. L'envoi réel se fait en session par le connecteur Gmail.

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
