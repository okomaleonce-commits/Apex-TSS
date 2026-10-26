# Signal CONTRADICTION-DE-SÉRIE + facteurs latents

*Spécification d'un facteur demandé : exploiter la **fréquence à laquelle une
équipe contredit ses stats** (retournement de série), et verser
systématiquement les **facteurs latents** (visibles en prématch, révélés en
post-match). Statut : **HYPOTHÈSE NON VALIDÉE** — WATCH/CANDIDATE uniquement
jusqu'à backtest positif (cf. jurisprudence J26). Établi le 2026-10-26.*

---

## Partie A — Facteurs latents (observationnels)

Les statistiques seules ne déterminent pas l'issue : des facteurs réels sont
visibles avant le match mais passent inaperçus, et ne « ressortent » qu'au
post-match. Ils ne créent pas un pari, ils **modulent la confiance** et
peuvent déclencher une abstention.

- **Où ils vivent déjà** : cellules de bruit `apex-mi-*` (marché) et
  `apex-bi-*` (comportemental), `orion-context`, lecture `apex-character`,
  autopsie post-match. `bet_authority=false` : ils alimentent la convergence,
  ils ne parient jamais seuls (J21).
- **À verser systématiquement** (proxys OBSERVABLES, jamais un diagnostic
  mental) : enjeu réel vs théorique, rotation probable, fatigue/voyage,
  météo/terrain, pression/narration déjà pricée, absences de dernière minute,
  conflit interne documenté, arbitre, dynamique MT→FT.
- **Boucle d'apprentissage** : chaque divergence post-match alimente la
  Failure Memory (`orion-learner`) ; un facteur latent récurrent qui explique
  des échecs devient une règle de jurisprudence.

---

## Partie B — Signal CONTRADICTION-DE-SÉRIE (retour à la moyenne conditionnel)

### B.1 Le piège à éviter
« N unders d'affilée ⇒ over au prochain » **pris seul = sophisme du joueur**
(matchs quasi-indépendants). Interdit comme signal (J26).

### B.2 Les quatre conditions qui rendent le signal licite
Un retournement de série n'est un **candidat** que si **les quatre** tiennent :

1. **SÉRIE** — N occurrences consécutives du même côté d'un marché
   (N à fixer par backtest, par équipe et par marché).
2. **CONTRADICTION au modèle** — pendant la série, l'issue réalisée s'oppose à
   l'attente du modèle calibré : p. ex. des unders alors que les λ (BSM /
   xG) prédisaient des overs. On mesure l'écart cumulé réalisé − attendu, pas
   la seule séquence. Sans contradiction mesurée, pas de signal.
3. **PLAFOND HISTORIQUE** — distribution empirique des longueurs de série de
   l'équipe pour ce marché : la série courante atteint/dépasse le quantile
   haut au-delà duquel l'équipe retourne presque toujours. (empirique, comme
   les percentiles turf — jamais une formule linéaire.)
4. **FORME ALIGNÉE** — les métriques récentes (xG pour/contre, cadence,
   finition) pointent dans le sens du retournement. Une série qui casse
   « contre la forme » n'est pas prise.

### B.3 Sortie
- 4/4 conditions → **CANDIDATE** (verser à la convergence ORION comme une voix,
  pas comme un pari).
- < 4 → **WATCH** ou rien.
- **Jamais ACCEPTER/JOUER** avant le gate backtest (B.5).

### B.4 Déclinaison par marché
Pour chaque marché : *série* = répétitions consécutives ; *retournement* = le
bascule attendu ; *contradiction* = l'attente du modèle pendant la série.

| Marché | Série observée | Retournement prédit | Contradiction mesurée sur |
|---|---|---|---|
| Favori (1 ou 2) | favori qui ne « assume » pas (série sans gain net du favori) | le favori assume | p(favori) du modèle vs résultats |
| Outsider | série sans surprise de l'outsider | l'outsider accroche | p(outsider) vs résultats |
| Nul | série sans nul | nul | p(X) vs résultats |
| Over 2.5 | série d'**unders** 2.5 | over 2.5 | λ_total vs buts observés |
| Under 2.5 | série d'**overs** 2.5 | under 2.5 | λ_total vs buts observés |
| Total équipe O1.5 (≥2) | série où l'équipe reste ≤1 but | l'équipe marque ≥2 | λ_équipe vs buts marqués |
| Total équipe O2.5 (≥3) | série où l'équipe reste ≤2 buts | l'équipe marque ≥3 | λ_équipe vs buts marqués |
| Over 3.5 | série sous 4 buts | ≥4 buts | λ_total vs buts observés |
| Over 4.5 | série sous 5 buts | ≥5 buts | λ_total vs buts observés |
| BTTS oui | série de BTTS **non** | les deux marquent | λ_dom ∧ λ_ext vs résultats |
| BTTS non | série de BTTS **oui** | clean sheet d'un côté | λ_dom ∧ λ_ext vs résultats |

> Les marchés « totaux élevés » (O3.5/O4.5) et « total équipe » sont rares : le
> plafond historique y est court et l'échantillon faible → J10 (échantillon)
> mord vite. Ne pas forcer.

### B.5 Gate de validation obligatoire (avant tout pari)
Walk-forward chronologique, par marché **et** par ligue (jamais pooler des
ligues non calibrées — J13 dans l'esprit) :
1. backtest du signal CONTRADICTION-DE-SÉRIE isolé → ROI et **CLV** (`apex_clv`) ;
2. seuils (N, ampleur de contradiction, quantile de plafond) **appris en
   validation, gelés avant test** (comme BSM/INTL) ;
3. verdict publié dans `backtests/<run>/REPORT.md` avec IC95 ;
4. activation marché par marché / ligue par ligue **seulement** si ROI>0 et
   CLV>0 de façon stable (J01, J02). Sinon le signal reste descriptif.

### B.6 Implémentation proposée (à valider avec l'utilisateur)
Outil `tools/apex_streak.py` (`backtest`), réutilisant `apex_bsm.fetch_matches`
(football-data.co.uk) et branchable en **voix** d'`apex_fusion`. Attente
(« stat contredite ») = Pinnacle dé-viggé pour over25 ; base empirique de
l'équipe pour btts (football-data n'a pas de cote BTTS). Aucun chiffre en dur :
grille `{N, C, Q, F}` apprise en validation puis gelée.

### B.7 Premier backtest (2026-10-26) — NON VALIDÉ, reste WATCH

Walk-forward sur E0/E1/E2/E3/SP1/I1/D1/F1, amorçage 2122/2223, validation
2324/2425, **test 2526** (saison en cours → échantillon court).

| Marché | Seuils gelés | Test n | hit-rate | ROI | ROI IC95 | Verdict |
|---|---|---|---|---|---|---|
| over25 | N=3, C=0.50, Q=0.80, F=6 | 28 | 0.643 | **+15,2 %** | **[−18,5 % ; +45,5 %]** | NON VALIDÉ |
| btts | N=4, C=0.50, Q=0.80, F=6 | 17 | 0.706 | — (pas de cote) | — | NON VALIDÉ |

Lecture honnête : le ROI over25 est **positif en point** mais sur **28 paris**,
l'IC95 franchit largement 0 — indiscernable de zéro (J01). BTTS n'a pas de cote
dans football-data → ROI non calculable, hit-rate seul insuffisant (J25). **Le
signal reste WATCH, jamais JOUER (J26).** Prochaine étape pour trancher : cote
BTTS d'une vraie source (odds-api / SharpAPI), plus de saisons de test, et
mesure du **CLV** — pas plus de paris sur une intuition.
