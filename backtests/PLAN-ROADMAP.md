# Feuille de route « faire performer APEX » — état dans le dépôt

Réponse point par point à la revue externe (5 chantiers). Pour chaque étape : ce qui existe
déjà dans le dépôt, l'outil, et ce qu'il reste à faire. Aucun chantier n'invente de donnée :
les conclusions négatives sont conservées telles quelles.

## Étape 1 — Base pré-match réellement horodatée, une compétition bien couverte, deux instants

**Fait — `tools/apex_tsbase.py`.** Collecteur sur une seule ligue (Premier League, id API-Football 39
par défaut). À chaque exécution, pour chaque match non commencé, il enregistre `hours_before_ko` et
le range dans un `bucket` :
- `T24`  : 30 h → 12 h avant le coup d'envoi (la veille) ;
- `T045` : 90 min → 20 min avant (après publication probable des compositions) ;
- `autre` : hors de ces deux fenêtres.

Chaque capture porte son heure réelle (`captured_at_utc`), les cotes 1X2/O-U 2.5/BTTS démarginées
(Pinnacle en priorité), l'état des compositions (`lineups_published`, XI si dispo) et les blessures.
Append-only, un fichier par ligue+saison : `data/tsbase/<league>_<saison>.jsonl` (non versionné,
`.gitignore`, car c'est de la donnée qui s'accumule dans le temps, pas du code).

**Règle d'or (revue externe) appliquée dans le code :** une prévision qui utilise les compositions
ne peut être comparée qu'aux cotes du bucket `T045` (prises après publication). Toute donnée dont
l'heure de disponibilité serait inconnue est marquée `availability_incertaine`.

**Restant :** laisser les deux passages quotidiens accumuler T24 puis T045 sur plusieurs semaines.
Le collecteur est désormais appelé dans `tools/apex_daily.py` (donc à chaque passage automatique).
`report` compte les matchs disposant des DEUX instants → seuls ceux-là serviront à l'expérience.

## Étape 2 — Partir du marché et apprendre ses écarts (poids apprenable, pas imposé)

**Fait — `tools/apex_market.py`.** Modèle ancré sur le marché : p ∝ p_marché^(1−w) × p_modèle^w,
avec **w appris sur validation**, jamais fixé. Résultat du backtest : **w = 0** — sur données
historiques, le modèle Dixon-Coles n'ajoute rien au marché démarginé. C'est le comportement voulu
par la revue (le poids doit pouvoir tomber à zéro si le modèle n'apporte pas d'information), et ici
il tombe effectivement à zéro. Conclusion conservée : ne pas parier sur le seul écart modèle↔marché.

## Étape 3 — Améliorer les buts attendus avec quelques variables testées (xG)

**Outil prêt, en attente de la clé — `tools/apex_footystats.py`.** Connecteur xG (xG réalisé ET
xG avant-match `team_x_xg_prematch`, seul ce dernier étant admissible pour une prévision). Discipline
inscrite dans le code : le xG réalisé ne sert jamais à une prévision d'avant-match.

**Bloqué sur :** `FOOTYSTATS_KEY` à définir comme **variable d'environnement** (FootyStats authentifie
par `?key=`, donc pas un identifiant API en en-tête), puis ouvrir une **nouvelle session** pour que la
variable soit injectée. Testé et fonctionnel sur la clé de démonstration (EPL, couverture xG 100 %).

## Étape 4 — Tester chaque amélioration séparément, split temporel + calibration

**Fait — `tools/apex_bsm.py` (walk-forward).** Backtest chronologique strict, sans info future :
train / validation / test isolé. Métriques : log-loss, Brier, RPS, calibration, IC95 bootstrap
groupés par match. Toute variante (xG, mouvement de cote…) se branche sur ce même cadre et se juge
sur le test isolé uniquement. Verdict actuel du modèle de base : **non supérieur au marché**.

## Étape 5 — Vérifier l'edge en conditions réelles (paris fictifs, centaines de matchs)

**Fait — `ledger/` (append-only) + `tools/apex_market.py forward` + `tools/apex_linemove.py forward`.**
Chaque prévision est enregistrée AVANT le coup d'envoi (`--record`) puis réglée après (`settle`).
L'évaluation en avant lit le journal et mesure le rendement réel sur les paris fictifs accumulés.
Le CLV (valeur à la clôture) est le juge retenu, conformément à la revue : battre la clôture avant
de parler de rendement.

---

## Synthèse

| Chantier revue | Outil | État |
|---|---|---|
| 1. Base horodatée 2 instants | `apex_tsbase.py` | **fait**, accumulation en cours (branché sur le quotidien) |
| 2. Ancrage marché, poids appris | `apex_market.py` | **fait** — w=0 (le modèle n'ajoute rien : conclusion conservée) |
| 3. xG dans les buts attendus | `apex_footystats.py` | outil prêt, **bloqué** sur `FOOTYSTATS_KEY` (env + nouvelle session) |
| 4. Split temporel + calibration | `apex_bsm.py` | **fait** (walk-forward, log-loss/Brier/calibration/IC95) |
| 5. Edge en conditions réelles | `ledger/` + `forward` | **fait**, se remplit match après match |

**Le seul chemin non encore réfuté** reste l'information que le marché price lentement — compositions
et mouvement ouverture→avant-match — mesurable uniquement EN AVANT sur 2026-27, ce que la base
horodatée (étape 1) et le journal (étape 5) sont désormais faits pour capturer proprement.
