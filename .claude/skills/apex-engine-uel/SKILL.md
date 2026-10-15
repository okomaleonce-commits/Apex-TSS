---
name: apex-engine-uel
description: >
  Moteur de coupe APEX dédié à la UEFA Europa League (league_id 3), séparé du moteur
  unifié apex-engine-uefa. DÉCLENCHER pour toute analyse, pronostic ou lecture d'un match
  de Ligue Europa — phase de ligue (36 équipes) comme phase à élimination directe. Fournit
  les paramètres CALIBRÉS SUR DONNÉES RÉELLES (avg_goals, HOME_ADV, baselines 1X2/O2.5/BTTS,
  mesurés sur 540 matchs 2024-26) + les règles contextuelles de coupe. Dixon-Coles + Monte-Carlo.
  NE PAS utiliser un moteur domestique pour un match UEL. Toute proba reste INDICATIVE /
  NON VALIDÉE (pas de backtest walk-forward sur cotes) — jamais étiquetée BSM.
---

# APEX-ENGINE UEL — moteur UEFA Europa League

Moteur **dédié**, séparé d'`apex-engine-uecl` et du moteur unifié `apex-engine-uefa`. Source de
vérité des paramètres : `tools/params/uefa_engines.json` (clé `"3"`). Ce moteur **ne price pas en
réel** (gel) et **n'émet aucun pari seul** : il fournit la couche structurelle de buts à la chaîne
BSM / WORM / S1-S8.

## 1. Statut & honnêteté (à lire avant tout)

- **CALIBRÉ** sur résultats réels : 540 matchs (269 en 2024-25 + 271 en 2025-26), scores API-Football.
- **NON comparé au marché** : aucun backtest walk-forward sur cotes historiques n'existe pour l'UEL.
  Toute probabilité produite est **INDICATIVE / NON VALIDÉE**, **jamais étiquetée BSM**.
- **Sur-confiance de petit échantillon — CORRIGÉE (shrinkage validé).** Le classement de coupe est
  mince en début de phase ⇒ taux gf/ga extrêmes ⇒ « favoris à 98 % » artefactuels. **Backtest
  walk-forward (540 matchs) : le modèle brut (K=0) est PIRE que le taux de base** (log-loss 1,18 vs
  1,04). Un **rétrécissement bayésien `shrink_k=8`** (câblé dans `apex_worm`) ramène le log-loss au
  niveau du base-rate (1,03) et fait tomber les artefacts p≥70 % de 110 à 2. Le 1X2 est désormais
  **sain** (ex. Lyon–Crystal Palace 37/25/38 au lieu de 2/9/89).
- **Force inter-ligue CÂBLÉE : Elo club (`tools/apex_uefa_elo.py` → `data/uefa/club_elo.json`).** Elo
  walk-forward sur **1 918 matchs UCL+UEL+UECL** (clubelo étant injoignable réseau, cet Elo est
  construit sur les résultats réels). **Il BAT le taux de base** (log-loss 1,025 vs base 1,033) — un
  edge **réel mais modeste**, et sans sur-confiance. `apex_worm` l'utilise en priorité pour ordonner
  les forces (ex. Milan favori à Salzburg, Real Sociedad à Union SG) ; **repli sur le classement +
  shrinkage** pour un club absent de la table (promu, petit championnat). Top Elo cohérent : PSG,
  Arsenal, Aston Villa, Bayern, Chelsea, Barcelone.
- **Limite résiduelle / honnêteté** : « bat le taux de base » n'est **pas** « bat le marché ». Statut
  **INDICATIF** maintenu (`data_quality ≤ 70`), jamais BSM, tant qu'un backtest sur **cotes** (CLV)
  n'a pas eu lieu. Les marchés de **buts** (avg_goals mesuré) restent la partie la plus robuste.

## 2. Paramètres calibrés (mesurés, n=540)

| Paramètre | Valeur | Source |
|---|---|---|
| avg_goals / match | **2,72** (dom 1,55 · ext 1,17) | mesuré |
| HOME_ADV (multiplicateur DC) | **1,12** (grands stades 1,14) | sqrt(ratio 1,327), conservateur |
| ratio buts dom/ext (brut mesuré) | 1,327 | mesuré |
| baseline 1X2 (D/N/E) | **49 % / 20 % / 31 %** | mesuré |
| Over 2.5 baseline | **54 %** | mesuré |
| BTTS baseline | **50 %** | mesuré |
| ρ Dixon-Coles | −0,05 | standard APEX |
| Monte-Carlo N | 50 000 | standard |
| DCS gate | 65 | cohérent avec apex-engine-uefa UEL |

> **Correction vs apex-engine-uefa** : ce dernier annonçait avg_goals UEL = 2,58 ; la mesure réelle
> sur 540 matchs donne **2,72**. Ce moteur retient la valeur mesurée.

## 3. Format & avantage terrain

- **Phase de ligue** : 36 équipes, 8 matchs chacune (4 domicile / 4 extérieur), classement unique.
  Top 8 qualifiés directs en 8es ; 9e–24e en barrage ; 25e–36e éliminés.
- **Élimination directe** : aller-retour (barrage, 8es, quarts, demies) ; finale terrain neutre → `HOME_ADV = 1,00`.
- HOME_ADV domicile standard **1,12** ; grands stades 1,14 ; finale neutre 1,00.

## 4. Règles contextuelles (lecture de coupe)

1. **Dead rubber** (phase de ligue, dernière(s) journée(s)) : une équipe déjà qualifiée/éliminée voit
   sa motivation chuter → baisser la confiance sur son camp, relever nul/outsider. Signal observable
   uniquement (classement, enjeu réel), jamais supposé.
2. **Rotation** : l'UEL est souvent la coupe « B » des grands clubs → turnover élevé le jeudi,
   surtout avant/après un gros match domestique. Intégrer les compositions réelles si publiées.
3. **Disrupteur contextuel** (→ APEX-MI) : perturbation extra-sportive documentée et datée d'un favori
   marché → baisser sa confiance. Cf. CLAUDE.md « Modulateurs de contexte en coupe ».
4. **Forteresse à domicile** (→ structure / moteur domestique de l'équipe qui reçoit) : intégrer son
   avantage structurel mesuré par sa propre ligue plutôt que de s'ancrer au seul favori venu de
   l'extérieur. Vecteur d'upset structurel documenté.
5. **Voyage / fuseaux** : déplacements longs (est-ouest européen, Caucase, Israël) → léger malus
   extérieur. Observable (distance, fuseau), non inventé.

## 5. Intégration APEX

- Fournit `lh/la` structurels (buts attendus) à `apex_bsm.py simulate` (via `--lh/--la`, marqués
  NON VALIDÉS) et la couche `W` (structure) de la fusion ORION.
- Ne remplace **pas** le module BSM obligatoire : une décision de pari passe par le backtest +
  simulation + convergence 3 voix. Ici, palier maximal **SURVEILLER** (gel).
- Chemin de validation (pour passer d'INDICATIF à VALIDÉ) : câbler une force inter-ligue (coefficient
  UEFA / Elo club), puis un backtest walk-forward sur cotes historiques UEL → `apex-llm-council-audit`.
