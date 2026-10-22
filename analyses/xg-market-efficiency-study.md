# Étude de non‑supériorité au marché & levier xG — note technique

**Run de référence :** `bsm-20261021T152819Z` (backtest walk‑forward) + banc d'essai `apex_bsm_xg`
**Date :** 2026‑10‑21 · **Statut modèle :** `NON SUPÉRIEUR AU MARCHÉ — EV contre le marché présumée illusoire`
**Gel :** actif (aucune mise réelle) · **Juge prospectif :** CLV réalisé (`ledger/clv.jsonl`)

---

## 0. ERRATUM — correction d'audit (2026‑10‑22)

Un audit a relevé des **défauts de mesure** dans la version initiale de cette note. Ils ne changent pas
la décision (gel maintenu, aucun bord prouvé) mais **invalident les chiffres quantitatifs ci‑dessous
tant qu'un re‑run propre n'a pas été produit**. Corrections appliquées à l'outillage
(`tools/apex_bsm.py`, `tools/apex_bsm_xg.py`, tests `tests/test_apex_bsm_xg.py`) :

1. **Benchmark contaminé par Pinnacle périmé.** Depuis le **23/07/2025**, l'API Pinnacle publique de
   football‑data est systématiquement en retard (avant‑match ET clôture) et exclue des moyennes de
   marché ([source](https://www.football-data.co.uk/data.php)). Or le moteur priorisait Pinnacle →
   **tout le benchmark de la saison test 2526 était faussé**. Correctif : Pinnacle exclu à partir de
   cette date (benchmark : moyenne puis Bet365 ; clôture : moyenne de clôture), **prix de mise séparé**
   du benchmark (offre bookmaker, jamais la moyenne).
2. **« Clôture » était en réalité « avant‑match ».** Le ΔLL du run BSM compare au prix **d'avant‑match**
   (`o1x2`), pas à la clôture. Les deux références ne sont pas interchangeables (colonnes `PSCH…`=clôture
   vs `PSH…`=avant‑match). Benchmark désormais **explicite** (`--benchmark prematch|closing`), **sans repli**.
3. **« à égalité » / « non‑infériorité » = abus de langage.** Un IC95 chevauchant 0 est **NON_CONCLUANT**
   (absence de preuve), pas une équivalence (qui exigerait une marge et un test dédiés). Tous les
   « à égalité » des §4 ci‑dessous se lisent **NON_CONCLUANT**.
4. **Inférence par match → bootstrap groupé par semaine** (dépendance intra‑journée préservée) ; IC plus
   larges, plus honnêtes. Publication séparée de 3 deltas : buts−marché, xG−marché, **et xG−buts**
   (apport incrémental du xG, avec son propre IC apparié — jamais plus « non significatif » asséné sans IC).
5. **Jointure xG renforcée.** Avant : (date, score) → un match pouvait recevoir le xG d'autres équipes.
   Désormais : date + score + **identité des deux équipes** ; ambiguïté et réutilisation **refusées**. La
   couverture « 99–100 % » doit être **re‑mesurée** après ce durcissement.
6. **Table des 17 ligues (§4) : non fiable en l'état.** Elle provenait de l'ancien banc (benchmark
   Pinnacle‑contaminé, repli clôture↔avant‑match, bootstrap par match, jointure par score). **À refaire**
   avec le protocole corrigé, benchmark explicite, par ligue, avec rapport JSON traçable.
7. **Calibration : dénominateur corrigé** — 3 960 matchs (11 880 probabilités 1X2), pas 3 808 ; la tranche
   0,9–1,0 (1 cas, p=0,917, non réalisé) était omise. Petits effectifs sans IC par tranche ⇒ aucune
   propriété « démontrée ».
8. **CLV mal interprété** — voir §4 corrigé : `CLV_fair` (proba de clôture dé‑marginée) ≠ `CLV_prix`
   (dérive de cote). Un `CLV_fair` négatif n'isole pas la dérive de prix de la marge.

**Illustration avec l'outil corrigé (E0, test 2526, Pinnacle exclu, bootstrap par semaine) :**
vs **avant‑match**, buts−marché = **+0,0123 [−0,0026 ; +0,0278] → NON_CONCLUANT** ; xG(w=0,25)−buts =
−0,0028 [−0,0078 ; +0,0019] → NON_CONCLUANT. vs **clôture** (plus sharp), buts−marché = +0,0157
[+0,0012 ; +0,0310] → **PIRE**. Le verdict dépend donc du benchmark, et le gain xG **n'est pas
significatif**. Rapports : `backtests/xg-E0-{prematch,closing}-*/report.json` (`promotion_authorized=false`).

**Ce qui reste vrai sans re‑run :** aucune supériorité prouvée (0/17 IC95 < 0), CLV réalisé non positif,
**gel maintenu**, et la famille « rating buts/xG contre la ligne » est close. Les §1–§9 qui suivent
décrivent le protocole et les chiffres **de l'ancien run** — à lire à travers cet erratum.

---

## 1. Problème

Tester l'**efficience semi‑forte** du marché football vis‑à‑vis de l'information exploitée par le moteur
(historique de buts et de xG) : le moteur **Poisson bivarié à correction Dixon‑Coles** produit‑il des
prévisions probabilistes **strictement plus précises que la ligne de clôture dé‑marginée**, hors
échantillon, ligue par ligue ? Référence = cote **d'avant‑match** dé‑marginée (football‑data.co.uk ;
Pinnacle avant le 23/07/2025, sinon moyenne de marché puis Bet365 — cf. erratum §0),
meilleur estimateur public connu de la probabilité réalisée.

Décision conditionnée : la **porte de promotion** du gel exige conjointement (i) supériorité OOS
(IC95 de ΔLL < 0) **et** (ii) **CLV cumulé réglé ≥ 0**. Tant que l'une est rouge, gel maintenu.

---

## 2. Méthodologie

- **Notes latentes** attaque/défense estimées par **IRLS pondéré exponentiellement** (demi‑vie pilotée
  par ξ), **rétrécissement** vers la moyenne par pseudo‑comptes K ; avantage terrain H ; dépendance
  basse‑score ρ (Dixon‑Coles) ; nœud de rythme partagé σ. Dérivation des marchés par matrice de scores
  (analytique) ou Monte‑Carlo 50 000.
- **Hyperparamètres gelés** (meilleur run global, sélection sur validation) : ξ=0,003 · K=6 · ρ=−0,05 ·
  σ=0. Figés pour isoler l'effet testé (pas de re‑grille par ligue → estimation conservatrice).
- **Walk‑forward hebdomadaire** : notes réestimées sur les seuls matchs **antérieurs au lundi** de la
  semaine évaluée (aucune fuite look‑ahead). Découpe train(2) / validation(2) / **test OOS(1 saison)**
  sur saisons 2122→2526.
- **Benchmark marché** : cote de clôture **dé‑viggée** par normalisation de l'overround
  (proba implicite = (1/cote)/Σ(1/cote)).
- **Score** : **log‑loss** (règle de score strictement propre), + Brier, RPS, courbes de calibration.
  Métrique de supériorité :

  ΔLL = moyenne( LL_modèle − LL_marché ) par match · **ΔLL < 0 ⟺ modèle meilleur**.
  **IC95 par bootstrap apparié** (2000 rééchantillonnages). Supériorité prouvée ⟺ borne haute < 0 ;
  non‑infériorité (« à égalité ») ⟺ IC95 chevauche 0 ; infériorité ⟺ borne basse > 0.

---

## 3. Résultats globaux — échantillon commun (test OOS, n = 3 808)

| Modèle | Brier 1X2 | LogLoss 1X2 | RPS 1X2 | Brier O/U2.5 | LogLoss O/U2.5 |
|---|---|---|---|---|---|
| **Marché dé‑marginé** (**avant‑match**) | **0,5993** | **≈1,0022** | *(réf.)* | — | **0,6815** |
| APEX‑BSM (gelé) | 0,6115 | 1,0208 | 0,2117 | 0,2503 | 0,6939 |
| Skill legacy | 0,6197 | 1,0327 | 0,2154 | 0,2555 | 0,7054 |
| Modèle simple de buts | 0,6506 | 1,0748 | 0,2309 | 0,2488 | 0,6908 |

**ΔLL 1X2 moyen** (test OOS) :

| Comparaison | ΔLL | IC95 | Lecture |
|---|---|---|---|
| **vs marché** | **+0,0186** | **[+0,0137 ; +0,0237]** | modèle **significativement PIRE** (IC95 > 0) |
| vs legacy | −0,0119 | [−0,0170 ; −0,0065] | modèle meilleur que l'ancien skill |
| vs modèle simple | −0,0540 | [−0,0635 ; −0,0443] | modèle bien meilleur qu'un Poisson naïf |

> Le moteur **domine** ses propres variantes antérieures (legacy, naïf) mais reste **dominé par la
> clôture**. Il est donc correctement spécifié — simplement non supérieur au marché.

Buts : prédits 2,69 vs observés 2,72 ; log‑loss score exact 2,9017. BTTS (sans cote historique, pas de
comparaison marché) : Brier 0,2484 · LogLoss 0,6901.

---

## 4. Résultats par ligue — ΔLL vs marché (test OOS)

Banc d'essai `apex_bsm_xg` : `w=0` = modèle buts actuel ; `w*` = meilleur poids xG retenu sur
validation (cible du fit = (1−w)·buts + w·xG réalisé passé).

| Ligue | code | n(test) | ΔLL buts (w=0) | IC95 | ΔLL xG (w\*) | w\* | statut |
|---|---|---|---|---|---|---|---|
| Eredivisie | N1 | 306 | +0,0072 | [−0,0120 ; +0,0249] | +0,0048 | 0,25 | **à égalité** |
| Premier League | E0 | 380 | +0,0136 | [−0,0045 ; +0,0315] | +0,0109 | 0,25 | **à égalité** |
| 2. Bundesliga | D2 | 306 | +0,0229 | [+0,0049 ; +0,0414] | **+0,0109** | 0,75 | pire → **à égalité (xG)** |
| Belgium Pro | B1 | 311 | +0,0188 | [+0,0013 ; +0,0370] | +0,0149 | 0,50 | limite → à égalité (xG) |
| La Liga | SP1 | 380 | +0,0178 | [+0,0018 ; +0,0342] | — | 0 | pire · xG rejeté |
| Championship | E1 | 552 | +0,0237 | [+0,0111 ; +0,0365] | +0,0219 | 0,50 | pire |
| Segunda | SP2 | 462 | +0,0262 | [+0,0089 ; +0,0438] | +0,0221 | 0,25 | pire |
| Ligue 2 | F2 | 305 | +0,0219 | [+0,0046 ; +0,0383] | +0,0222 | 0,75 | pire |
| Serie A | I1 | 380 | +0,0226 | [+0,0036 ; +0,0402] | — | 0 | pire · xG rejeté |
| Serie B | I2 | 380 | +0,0278 | [+0,0125 ; +0,0440] | +0,0286 | 0,25 | pire (xG adverse) |
| Portugal | P1 | 306 | +0,0260 | [+0,0102 ; +0,0423] | +0,0303 | 0,25 | pire (xG adverse) |
| Ligue 1 | F1 | 306 | +0,0255 | [+0,0071 ; +0,0438] | +0,0198 | 0,25 | pire |
| Turkey Süper Lig | T1 | 306 | +0,0302 | [+0,0121 ; +0,0489] | — | 0 | pire · xG rejeté |
| Bundesliga | D1 | 306 | +0,0306 | [+0,0127 ; +0,0489] | +0,0297 | 0,25 | pire |

**xG indisponible chez FootyStats** (ΔLL buts du backtest, « stabilité par ligue ») :

| Ligue | code | n | ΔLL buts | IC95 | statut |
|---|---|---|---|---|---|
| National League | EC | 529 | +0,0053 | [−0,0057 ; +0,0173] | à égalité |
| League One | E2 | 540 | +0,0217 | [+0,0065 ; +0,0380] | pire |
| League Two | E3 | 540 | +0,0186 | [+0,0036 ; +0,0326] | pire |

> ⚠️ **Table de l'ancien banc — à refaire (erratum §0‑6).** Benchmark Pinnacle‑contaminé, repli
> clôture↔avant‑match, bootstrap par match, jointure par score. Les verdicts « à égalité » ci‑dessus se
> lisent **NON_CONCLUANT**, et ces chiffres ne sont pas opposables tant qu'un re‑run propre
> (`--benchmark prematch` **et** `closing`, par ligue) n'a pas été produit.

**Synthèse :** sur **17 ligues**, **aucune** n'a d'IC95 entièrement < 0 → **aucune supériorité prouvée**.
NON_CONCLUANT (absence de preuve, pas équivalence) sur E0, EC, N1, D2 (et B1). Partout ailleurs, infériorité
significative.

---

## 5. Calibration 1X2 (échantillon test, issues regroupées)

| Tranche proba | n | p moyenne prédite | fréquence observée |
|---|---|---|---|
| 0,0–0,1 | 103 | 0,078 | 0,078 |
| 0,1–0,2 | 968 | 0,164 | 0,154 |
| 0,2–0,3 | 5 095 | 0,257 | 0,253 |
| 0,3–0,4 | 2 591 | 0,343 | 0,343 |
| 0,4–0,5 | 1 710 | 0,446 | 0,443 |
| 0,5–0,6 | 917 | 0,544 | 0,563 |
| 0,6–0,7 | 371 | 0,640 | 0,687 |
| 0,7–0,8 | 99 | 0,743 | 0,778 |
| 0,8–0,9 | 25 | 0,837 | 0,880 |
| 0,9–1,0 | 1 | 0,917 | 0,000 |

> Table sur **3 960 matchs (11 880 probabilités)**, pas 3 808. La tranche 0,9–1,0 (1 seul cas, non
> réalisé) est non informative. **Sans IC par tranche**, aucune propriété n'est *démontrée* : la lecture
> « léger sous‑pricing des favoris » (0,6–0,9) est une **indication**, pas une preuve. Un modèle calibré
> mais moins **résolu** que le marché perd quand même au log‑loss.

---

## 6. Simulation de mise (règles figées à l'avance)

Flat‑stake 1u · filtre **EV ≥ 3 %** contre cote dé‑marginée · 1 marché/match · marchés 1X2 + O/U2.5.

| Métrique | Valeur |
|---|---|
| Paris | 3 186 |
| Taux de réussite | 33,4 % |
| Rendement net / unité | **−12,8 %** (IC95 [−17,9 % ; −7,8 %]) |
| P&L cumulé | **−407,5 u** |
| Perte max cumulée | −431,0 u |
| **CLV moyen 1X2** | **−6,0 %** |

> **Deux CLV à ne pas confondre (erratum §0‑8).** Le backtest mesure `CLV_fair = cote_entrée ×
> proba_clôture_dé‑marginée − 1` ; le suivi `apex_clv.py` mesure `CLV_prix = cote_entrée /
> dernière_cote_prématch − 1`. Un `CLV_fair` négatif **n'isole pas** la dérive de prix de la **marge** :
> à cote d'issue inchangée, `CLV_prix = 0` mais `CLV_fair ≈ −marge`. Ce **−6,0 % est un `CLV_fair` sur
> suivi papier** (snapshot prématch ≠ vraie clôture certifiée, aucun ticket exécuté) : il est cohérent
> avec l'absence de bord, mais ne prouve ni dérive systématique ni rentabilité future.

---

## 7. Levier xG — réduction de variance

Hypothèse : **buts = xG + bruit de finition/gardien**, donc le xG réalisé passé est un estimateur
**moins bruité de la force latente**. Mélange `(1−w)·buts + w·xG` dans la cible du fit ; `w` choisi sur
validation ; xG **strictement antérieur** (walk‑forward respecté, aucun usage du xG du match prédit).

**Verdict :** effet **marginal et non significatif**. Réduction de ΔLL maximale observée **−0,0121 sur D2**
(2. Bundesliga, w=0,75 → fait passer « pire » à « à égalité »), nulle ou **adverse** ailleurs (I2, P1).
`w=1` (xG pur) **systématiquement dominé** : la composante finition porte un signal résiduel. **Nulle
part** le xG ne fait franchir l'IC95 sous 0. → **non injecté dans le moteur gelé** (pas de bord à
revendiquer).

---

## 8. Interprétation (efficience & microstructure)

La ligne de clôture est un **agrégateur d'information quasi‑optimal** (buts, xG, compos, blessures, flux
d'argent informé). Un rating Poisson buts/xG n'apporte **aucune information orthogonale** à ce prix ; il
peut au mieux le **répliquer** (parité), jamais le dominer. S'ajoute la barrière de l'**overround**
(~2–5 % sur 1X2) : « aussi précis » que le marché ⇒ perdre la marge. Le **CLV négatif** confirme
l'absence de bord *ex‑ante*.

La corrélation observée **parité ↔ faible liquidité** (Eredivisie, 2. Bundesliga tiennent mieux que le
big‑5) est cohérente avec une efficience croissante avec le volume — **mais insuffisante** pour produire
ΔLL < 0, y compris en D2.

---

## 9. Limites statistiques

- **Test mono‑saison** par ligue (n ≈ 300–552) → IC95 larges, puissance limitée.
- **Comparaisons multiples** (17 ligues) → risque de parité « par hasard » (garden of forking paths) ;
  E0/N1/D2 sont des **hypothèses à répliquer**, pas des acquis.
- Hyperparamètres **non re‑tunés par ligue** (ξ,K,ρ,σ globaux) → conservateur.
- Jointure xG par (date, score final) : 99–100 % d'appariement ; non‑appariés en **repli sur buts** (pas
  d'imputation). xG **absent** pour EC/E2/E3 et toutes les D2 non cochées — écrit absent, jamais fabriqué.

---

## 10. Décision

Les **deux portes** de promotion sont rouges : (i) aucune ligue avec IC95 ΔLL < 0 ; (ii) CLV réglé non
disponible/négatif. **Gel maintenu**, rien injecté. La recherche de bord par **enrichissement d'entrée
(xG)** ou **descente en liquidité (D2)** est **réfutée empiriquement**. L'edge éventuel est à chercher
**hors de l'information déjà agrégée par la ligne** : micro‑timing, marchés secondaires illiquides,
closing‑line value / steam. Le **seul KPI prospectif** habilité à lever une porte est le **CLV réalisé**
accumulé (`ledger/clv.jsonl`).

---

## 11. Outils, données et méthodes utilisés

### Outils du dépôt (`tools/`)
| Outil | Rôle dans l'étude |
|---|---|
| `apex_bsm.py` | Moteur Dixon‑Coles/Monte‑Carlo : `backtest` (walk‑forward, ΔLL, bootstrap, calibration, sim de paris), `simulate`, `settle`, `audit`. Cœur **gelé** réutilisé. |
| `apex_bsm_xg.py` | **Banc d'essai xG** (créé ici) : jointure football‑data↔xG par (date,score), `fit_ratings_blend` (cible buts↔xG), walk‑forward ΔLL vs marché par poids. |
| `apex_footystats.py` | Connecteur FootyStats : `status`, `leagues` (résolution `season_id`), `history` (xG réalisé → `data/footystats/`). Correction libellé `T1` ici. |
| `apex_apifootball.py` | API‑Football : `snapshot` (fixtures, cotes horodatées, blessures, compositions), `fixtures`, `injuries`, `fixtures/lineups`, `players`. |
| `apex_clv.py` | **CLV / ROI réel** : reconstruction cote entrée/clôture, P&L, CLV ; `collect` → journal durable `ledger/clv.jsonl` (étendu ici). |
| `apex_compos.py` | **Absences → ajustement λ** (créé ici) : importance = part buts+passes sourcée → multiplicateur borné → `--scenario` BSM (NON CALIBRÉ). |
| `apex_omega.py` | Couche de triage/transparence (créée ici) — contexte du gel et des paliers WATCH. |
| `apex_fusion.py` · `orion_consensus.py` · `gel_matrix.py` · `apex_worm.py` · `apex_sources.py` | Digest unique, arbitrage ORION, portes du gel, snapshots d'anomalies, voix sharp externe. |

### Données & services externes
| Source | Usage | Auth |
|---|---|---|
| **football‑data.co.uk** (format mmz4281) | Scores, tirs (HS/HST), **cotes de clôture Pinnacle** (PSH/PSD/PSA, P>2.5/P<2.5) = benchmark marché. | libre |
| **FootyStats API** | **xG réalisé** par match (`team_a_xg`/`team_b_xg`). | `FOOTYSTATS_KEY` (var. env., `?key=`) |
| **API‑Football v3** | Fixtures, cotes, **blessures**, **compositions**, stats joueurs (pour COMPOS). | clé injectée par le proxy (`x-apisports-key`) |
| **Connecteur Gmail (MCP)** | Envoi des digests FUSION/OMEGA (`mcp__Gmail__send_message`). | OAuth connecteur |

### Méthodes statistiques
IRLS pondéré temporellement · bivarié Poisson + correction Dixon‑Coles (ρ) · rétrécissement bayésien
empirique (K) · Monte‑Carlo · **démarginage** de l'overround · scores propres **log‑loss**/Brier/**RPS** ·
courbes de **calibration** · **bootstrap** apparié (IC95) · validation **walk‑forward** · **CLV** comme
KPI prospectif.

### Exécution
Python 3 · `numpy`, `scipy`. Données historiques en cache (`data/history/`, `data/footystats/` —
gitignorés). Artefacts de backtest : `backtests/<run_id>/` (REPORT.md, report.json, bets.json).

---

## 12. Reproductibilité

```bash
# 1. Backtest calibré (ΔLL vs marché, calibration, sim de paris)
python3 tools/apex_bsm.py backtest --divs E0,E1,E2,E3,EC,SP1,I1,D1,F1 \
        --seasons 2122,2223,2324,2425,2526 --refresh

# 2. xG : couverture, tirage, éval buts vs xG vs marché (par ligue)
python3 tools/apex_footystats.py leagues --divs <DIVS> --seasons 2122,2223,2324,2425,2526
python3 tools/apex_footystats.py history --divs <DIVS> --seasons 2122,2223,2324,2425,2526
python3 tools/apex_bsm_xg.py eval --div <DIV> --benchmark prematch   # puis --benchmark closing
# (rapport JSON de diagnostic, promotion_authorized=false ; latest_params NON modifié)

# 3. CLV prospectif (accumulé à chaque scan WORM)
python3 tools/apex_clv.py            # résumé ROI/CLV cumulé (journal durable)
```

*Mesure walk‑forward, aucune mise réelle émise — le gel reste entier.*
