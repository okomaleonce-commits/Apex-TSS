---
name: apex-engine-uecl
description: >
  Moteur de coupe APEX dédié à la UEFA Europa Conference League (league_id 848), séparé du
  moteur unifié apex-engine-uefa. DÉCLENCHER pour toute analyse, pronostic ou lecture d'un match
  de Conference League — phase de ligue (36 équipes) comme phase à élimination directe. Fournit
  les paramètres CALIBRÉS SUR DONNÉES RÉELLES (avg_goals, HOME_ADV, baselines 1X2/O2.5/BTTS,
  mesurés sur 818 matchs 2024-26) + les règles contextuelles de coupe, avec le plus fort avantage
  domicile des trois coupes (tier forteresse). NE PAS utiliser un moteur domestique pour un match
  UECL. Toute proba reste INDICATIVE / NON VALIDÉE (pas de backtest walk-forward sur cotes) —
  jamais étiquetée BSM.
---

# APEX-ENGINE UECL — moteur UEFA Europa Conference League

Moteur **dédié**, séparé d'`apex-engine-uel` et du moteur unifié `apex-engine-uefa`. Source de vérité
des paramètres : `tools/params/uefa_engines.json` (clé `"848"`). Ne price pas en réel (gel), n'émet
aucun pari seul : fournit la couche structurelle de buts à la chaîne BSM / WORM / S1-S8.

## 1. Statut & honnêteté (à lire avant tout)

- **CALIBRÉ** sur résultats réels : 818 matchs (409 en 2024-25 + 409 en 2025-26), scores API-Football.
- **NON comparé au marché** : aucun backtest walk-forward sur cotes historiques pour l'UECL →
  proba **INDICATIVE / NON VALIDÉE**, **jamais étiquetée BSM**.
- **Limite structurelle connue (critique en UECL)** : le vivier est très hétérogène (grands clubs en
  reconversion européenne ↔ champions de micro-championnats). Le classement de force inter-ligue
  n'étant pas câblé, le **1X2 structurel est fortement sur-confiant** → plafonner `data_quality ≤ 68`,
  rejeter les 1X2 extrêmes, préférer les marchés de **buts**. *C'est exactement le cas qui, au scan du
  15/10/2026, rendait les 18 matchs UECL non classables (model_prob absent → NO BET).* Ce moteur
  fournit au moins les **baselines de buts** fiables en attendant la force inter-ligue.

## 2. Paramètres calibrés (mesurés, n=818)

| Paramètre | Valeur | Source |
|---|---|---|
| avg_goals / match | **2,72** (dom 1,62 · ext 1,10) | mesuré |
| HOME_ADV (multiplicateur DC) | **1,15** (grands stades 1,17 · **forteresse 1,21**) | sqrt(ratio 1,467), conservateur |
| ratio buts dom/ext (brut mesuré) | **1,467** (le + élevé des 3 coupes) | mesuré |
| baseline 1X2 (D/N/E) | **49 % / 21 % / 30 %** | mesuré |
| Over 2.5 baseline | **52 %** | mesuré |
| BTTS baseline | **47 %** | mesuré |
| ρ Dixon-Coles | −0,05 | standard APEX |
| Monte-Carlo N | 50 000 | standard |
| DCS gate | 70 (le plus strict) | cohérent avec apex-engine-uefa UECL |

> **Correction MAJEURE vs apex-engine-uefa** : ce dernier annonçait avg_goals UECL = **2,44** ; la
> mesure réelle sur 818 matchs donne **2,72** — un écart important qui faussait les marchés de buts.
> Ce moteur retient la valeur mesurée.

## 3. Format & avantage terrain

- **Phase de ligue** : 36 équipes, 6 matchs chacune (3 domicile / 3 extérieur). Top 8 qualifiés
  directs en 8es ; 9e–24e en barrage ; 25e–36e éliminés.
- **Élimination directe** : aller-retour ; finale terrain neutre → `HOME_ADV = 1,00`.
- **Avantage domicile le plus marqué des trois coupes** (ratio buts 1,47). Tier **forteresse** 1,21
  pour un club modeste réputé imprenable chez lui (pelouse synthétique, déplacement extrême, public) —
  vecteur d'upset structurel face à un cador en déplacement.

## 4. Règles contextuelles (lecture de coupe)

1. **Dead rubber** : fréquent en UECL où les écarts de niveau créent vite des situations acquises →
   baisser la confiance du camp démotivé, relever nul/outsider. Observable uniquement.
2. **Rotation lourde** : pour les clubs dont l'UECL est la 3e priorité, turnover très élevé →
   compositions réelles décisives.
3. **Forteresse à domicile** (cœur de l'UECL) : intégrer l'avantage structurel mesuré du club qui
   reçoit (sa propre ligue) plutôt que le favori venu de l'extérieur. Cf. CLAUDE.md « forteresse ».
4. **Disrupteur contextuel** (→ APEX-MI) : perturbation extra-sportive datée d'un favori → confiance ↓.
5. **Hétérogénéité du vivier** : un « 98 % » structurel contre un champion de micro-championnat est
   un artefact tant que la force inter-ligue n'est pas câblée — ne jamais le présenter comme calibré.

## 5. Intégration APEX

- Fournit `lh/la` structurels à `apex_bsm.py simulate` (`--lh/--la`, marqués NON VALIDÉS) et la couche
  `W` de la fusion ORION. Palier maximal **SURVEILLER** (gel).
- Ne remplace pas le module BSM obligatoire (backtest + simulation + convergence 3 voix).
- Chemin de validation : câbler une force inter-ligue (coefficient UEFA club / Elo club), puis backtest
  walk-forward sur cotes historiques UECL → `apex-llm-council-audit`, avant tout passage à VALIDÉ.
