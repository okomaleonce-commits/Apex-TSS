# JURISPRUDENCE APEX — analyse de paris sportifs

*Droit jurisprudentiel des moteurs APEX : chaque cas réel d'**échec** ou
d'**abstention** est transformé en règle réutilisable pour les prochaines
analyses. Établi le 2026-10-26 par audit de `backtests/`, `ledger/`
(234 prévisions, 214 règlements) et des gates des skills/outils.*

**Comment l'utiliser.** Avant toute analyse (foot, hockey, turf, fusion),
charger `tools/params/jurisprudence.json` et appliquer les règles. Une règle
`bloquante` ferme la décision ; une règle `plafond` limite le palier
atteignable (ex. SURVEILLER). Append-only : on n'efface pas une règle, on en
ajoute une plus précise. **Principe cardinal : donnée absente = écrite absente,
jamais inventée.**

Deux familles de raisons se dégagent de l'audit :
- **Statistique** — le modèle ou les données disent non (EV, backtest, Brier,
  volatilité, échantillon, ROI, voix indépendantes).
- **Non statistique** — structure du marché, périmètre, données, ou process
  (cote absente, hors périmètre, convention mal lue, veto, confirmations, gel).

---

## I. Raisons STATISTIQUES

### J01 — EV < 3 % ou EV instable → ABSTENTION
Un marché officiel ne passe que si **EV ≥ 3 %** *et* l'EV reste positive aux
quatre chocs de sensibilité (λ dom/ext ±10 %). Sinon, abstention.
> *Cas : 43 prévisions « ABSTENTION — aucune EV ≥ 3 % stable » (Belgium-France,
> Türkiye-Italy, Northern Ireland-Hungary…).*

### J02 — Modèle non supérieur au marché → EV illusoire
Si le backtest ne prouve pas que le modèle bat le marché (IC95 du Δlog-loss
chevauche 0, ou poids de mélange optimal `w*≈0`), toute « EV contre le marché »
est **présumée illusoire** : ne pas la présenter comme value, ne pas miser.
> *Cas : `market_params.json` → « NON SUPÉRIEUR AU MARCHÉ », `market_blend_w=0.0`,
> « le modèle n'apporte rien, le mélange = marché ». E0 (IC95 [−0.008, +0.026]) et
> EC (IC95 [−0.006, +0.017]) chevauchent 0.*

### J07 — Conversion Elo→λ : régression gelée, pas heuristique
Utiliser la conversion Elo→λ **backtestée** (H1, régression gelée), jamais
l'heuristique H0.
> *Cas : 32 prévisions remplacées ; REPORT intl : H1−H0 Δlog-loss −0.0166
> IC95 [−0.0232, −0.0103] sur 3837 matchs de test.*

### J09 — Preflight : battre la baseline au Brier
Avant publication, le signal doit battre la baseline naïve au **test de Brier**
et s'appuyer sur une cote à marge plausible. Marge nulle/aberrante ⇒ bloqué.

### J10 — Échantillon trop court → NO_BET
Pas de BET sur échantillon insuffisant (début de saison, historique court).

### J11 — Volatilité → NO_BET ou marchés restreints
Au-delà du seuil de Volatility Score : NO_BET ou restriction des marchés
autorisés (gates G3/G4). La volatilité réduit la portée, pas seulement la mise.

### J12 — ROI backtest négatif → plafond SURVEILLER (turf)
Une discipline dont le backtest ROI est négatif ne dépasse **jamais**
SURVEILLER (jamais JOUER).
> *Cas : trot ROI −4,58 % IC95 [−44,04 % ; +42,06 %] sur 118 paris ; obstacle
> −89,05 % sur 21. Les deux gates de pari turf sont fermées par le backtest.*

### J20 — Moins de 3 voix indépendantes → COLLECTER
ACCEPTER exige **≥ 3 voix sourcées indépendantes** (W structure, M marché, C
comportemental, S forecast, K sharp). Plusieurs agents citant la même donnée ne
sont pas un consensus (corrélation des erreurs, META). WORM seul peut dire
JOUER : ORION refuse sans confirmation indépendante.
> *Cas : 15 « SURVEILLANCE / ABSTENTION » et la règle de fusion `< 3 voix → COLLECTER`.*

### J26 — Contradiction-de-série : une série n'est pas un signal
Un retournement de série (fin d'une série d'unders → over, et idem pour favori,
outsider, nul, O/U 2.5, total équipe O1.5/O2.5, O3.5, O4.5, BTTS oui/non) pris
seul est le **sophisme du joueur**. Il n'est un CANDIDATE que si **les quatre**
conditions tiennent : (1) série de N, (2) contradiction mesurée à l'attente du
modèle, (3) longueur atteignant le **plafond historique empirique** de l'équipe,
(4) forme récente alignée sur le retournement. Et il ne devient un pari
qu'après un backtest walk-forward (par marché **et** par ligue) à ROI et CLV
positifs stables, seuils gelés avant test. Sans backtest : **WATCH, jamais
JOUER**. Les facteurs latents (visibles en prématch, révélés en post-match) se
versent à la couche observationnelle (MI/BI, context, character),
`bet_authority=false`.
> *Spéc. : `docs/signal-contradiction-serie.md`. Garde-fou cohérent avec J01,
> J02, J10, J25.*

### J27 — ROI/CLV de ligue négatif → ABSTENTION (mécanisé)
Une ligue dont le **backtest de paris** a un ROI (ou CLV) **significativement
négatif** (IC95 entièrement sous 0) ne parie pas — **même si** le statut
log-loss n'est pas « significativement pire » et que le gate EV per-match passe.
Le gate per-match ne regarde pas le ROI de la ligue ; il faut un garde-fou
séparé. **Mécanisé** dans `apex_bsm.simulate` via `tools/params/roi_gate.json`
(fail-open). Toute ligue à `roi_ic95_haut < 0` force l'ABSTENTION.
> *Révélé le 2026-10-26 : sur P1 (log-loss NON CONCLUANT) le moteur proposait
> « SÉLECTION Santa Clara @9.0 (+63 % EV) » alors que le backtest de paris P1
> perdait 22 % (ROI −22,4 % IC95 [−39,3 % ; −4,2 %]). T1 : −29,3 % IC95
> [−44,6 % ; −12,4 %]. Avant/après vérifié : la décision passe de SÉLECTION à
> ABSTENTION.*

---

## II. Raisons NON STATISTIQUES (structure · périmètre · données · process)

### J03 — Non comparé au marché → pas d'edge prouvé
Un modèle validé hors-marché (pas de cotes historiques) ne démontre **aucun**
edge marché. Marquer NON COMPARÉ AU MARCHÉ ; ne jamais présenter comme value.
> *Cas : 113 prévisions « NON COMPARÉ AU MARCHÉ » ; APEX-INTL « H1 VALIDÉE contre
> H0 … NON COMPARÉE AU MARCHÉ ».*

### J04 — λ hors périmètre → NON VALIDÉ, mise ≤ ½
Avec des λ hors du périmètre backtesté (ligue sans moteur calibré), le modèle
est **NON VALIDÉ** : au mieux une sélection INDICATIVE à mise ≤ ½, jamais un
pari plein.
> *Cas : 98 « NON VALIDÉ — λ externes », 123 « SÉLECTION INDICATIVE (mise ≤ ½) ».*

### J05 — Sans cote vérifiée → pas de sélection
Sans cote vérifiée et horodatée, l'EV n'est pas calculable : aucune sélection
officielle. Relever la cote (snapshot) avant de pricer ; sinon rester indicatif.
> *Cas : 15 prévisions « SANS COTE VÉRIFIÉE ».*

### J06 — Convention de signe des handicaps asiatiques (⚠ cause n°1 d'exclusion)
**Toujours** vérifier la convention de signe des handicaps asiatiques de la
source (API-Football) avant de pricer. Au moindre doute, invalider la prévision
avant le coup d'envoi et rejouer sur un relevé corrigé.
> *Cas : 42 règlements EXCLUS « signe des handicaps asiatiques extérieurs mal lu
> (convention API-Football) » — la première cause d'exclusion du journal.*

### J08 — Gate DCS de fiabilité des données → NO_BET
Aucun signal sous le gate DCS de la ligue (ex. 58 Ligue 2, 62 Segunda/Roumanie,
63 Superligaen, 65 Allsvenskan). Fallback générique sans moteur dédié : DCS −5,
Kelly ×0,70.

### J13 — Plat turf → HORS_PERIMETRE
Le plat sort HORS_PERIMETRE. Hors discipline calibrée, le résultat n'est pas
moins précis, il est **de signe faux** (cf = 0,0 en trot vs 0,4 en obstacle) :
refuser, ne pas dégrader. Idem pour les quantiles `turf_worm`.

### J14 — Pari mutuel → signaux multi-book UNAVAILABLE_STRUCTUREL
En pari mutuel (une seule cote PMU), `SHARP_MOVE`, `STEAM_MOVE`, `RLM`,
`BOOKMAKER_DIVERGENCE`, `LIQUIDITY_SPIKE` n'existent pas : sortir
UNAVAILABLE_STRUCTUREL, ne pas enregistrer. Ne jamais fabriquer un signal absent
de la structure du marché.

### J15 — Source absente → ABSENT_SOURCE
Donnée absente de la source = écrite ABSENT_SOURCE, jamais comblée ni estimée.
Ce n'est pas un refus de périmètre, c'est une absence de données.
> *Cas : Nationale 3 marocaine (Anfa, Khemisset) hors API française.*

### J16 — LONACI : avertissement masse obligatoire
Ne jamais présenter une analyse LONACI comme fondée sur les cotes LONACI tant
que la comparaison masse FR/LONACI n'est pas faite : `cotes_origine=PMU_FRANCE`,
`avertissement_masse` obligatoire.

### J17 — VETO du conseil adversarial → non négociable
Un VETO (S8 / council / red team) annule la décision, quel que soit le reste de
la chaîne.

### J18 — Dead rubber → NO_BET
Un match sans enjeu réel bloque (gate S2) : l'enjeu théorique ne garantit pas la
motivation.

### J19 — Confirmations manquantes → WAIT_*
Suspendre la décision tant que manquent les compositions (WAIT_LINEUPS, foot),
le gardien partant (WAIT_GOALIE, hockey) ou les partants/déclarations
(WAIT_DECLARATIONS, turf). Ne pas pricer dans le flou — étape la plus critique
de chaque protocole.

### J21 — Bruit seul → WATCH, jamais un pari
Les cellules de bruit (APEX-MI / APEX-BI / TURF-MI) ont `bet_authority=false` :
BEHAVIORAL seul → WATCH ; + MARKET → CANDIDATE ; + DATA → CONFIRMED. Le bruit ne
remplace pas le module BSM obligatoire.

### J22 — Gel actif → tout ACCEPTER rétrogradé en ATTENDRE
Sous gel, aucune mise réelle n'est émise : la sortie se limite à la recherche
(digest, email, journal append-only).

### J23 — Doublon de fixture → EXCLU
Dédupliquer par identifiant de fixture avant d'enregistrer ; exclure le doublon
de l'audit (ne pas le régler comme pari).

### J24 — Match annulé → ANNULE (hors audit)
Régler ANNULE et exclure de l'audit de performance. Ne jamais compter un match
non joué comme un échec du modèle.

### J25 — Anti-invention (principe cardinal)
Ne jamais inventer un backtest, une cote, une statistique, un volume ou une
simulation. Hiérarchie des sources honnête (déplacement Pinnacle ≠ post
Telegram). Le RLM n'est pas calculable sans % public. Donnée manquante = écrite
manquante.

---

## Synthèse chiffrée de l'audit

| Décision / statut observé | n | Famille | Règle |
|---|---|---|---|
| SURVEILLANCE / ABSTENTION | 53 | statistique | J20 |
| ABSTENTION (EV < 3 % stable) | 43 | statistique | J01 |
| SANS COTE VÉRIFIÉE | 15 | structurel | J05 |
| SÉLECTION INDICATIVE (mise ≤ ½) | 123 | structurel | J04 |
| NON COMPARÉ AU MARCHÉ | 113 | structurel | J03 |
| NON VALIDÉ — λ externes | 98 | structurel | J04 |
| NON SUPÉRIEUR AU MARCHÉ | 22 | statistique | J02 |
| EXCLU — signe AH mal lu | 42 | process/données | J06 |
| Remplacé — Elo→λ heuristique | 32 | statistique | J07 |
| EXCLU — doublon fixture | 2 | process | J23 |
| ANNULE — CANC | 1 | process | J24 |
| *(réglés automatiquement, joués)* | 137 | — | — |

*Vocabulaire des gates relevé dans skills+outils : NO_BET 49×, SURVEILLER 26×,
UNAVAILABLE_STRUCTUREL 19×, COLLECTER 15×, VETO 14×, Brier 14×, HORS_PERIMETRE
7×, DEAD_RUBBER 7×, ABSENT_SOURCE 8×, WAIT_GOALIE/WAIT_DECLARATIONS 6× chacun,
WAIT_LINEUPS 2×.*

> Pour ajouter une règle : append dans `tools/params/jurisprudence.json`
> (nouvel `id` Jxx) et ici. Ne pas réécrire une règle existante.
