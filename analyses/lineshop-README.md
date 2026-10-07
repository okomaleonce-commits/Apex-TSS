# Line shopping vs Pinnacle — le comparateur de bookmakers bat-il le marché ?

Backtest `tools/apex_lineshop.py` · 15 269 matchs · 8 championnats · saisons 2021-22 à 2025-26.
Vérité de référence : cote juste de **Pinnacle en avant-match**, démarginée (le backtest APEX-BSM a montré que le marché est le meilleur estimateur). Contrôle : la cote prise bat-elle la **clôture de Pinnacle** (CLV) ?
Règle fixée à l'avance : 1 unité par issue dès que le bookmaker paie au-dessus de la cote juste d'au moins un seuil. Aucun paramètre optimisé après coup.

## Résultat principal

**Aucune configuration n'a un rendement dont l'intervalle de confiance à 95 % est entièrement au-dessus de zéro.**
Autrement dit : sur 5 saisons, aucune règle de line shopping n'a prouvé qu'elle gagnait de l'argent.

| Source de prix | seuil 3 % | rendement/u | IC95 | CLV moyen |
|---|---|---|---|---|
| Meilleure cote du marché (Max) | 3 913 paris | −4,7 % | [−11,5 % ; +2,5 %] | **+5,0 %** |
| Bet365 | 555 paris | −1,5 % | [−20 % ; +19 %] | +2,7 % |
| Betway | 112 paris | +2,9 % | [−47 % ; +68 %] | +2,5 % |
| Cote moyenne (Avg) | 100 paris | −10 % | [−57 % ; +52 %] | +2,7 % |

Les rendements positifs (Betway, seuils élevés) reposent sur quelques dizaines de paris : leurs intervalles de confiance sont énormes, ce n'est que du bruit.

## Le signal réel : le CLV est positif et constant

En prenant la **meilleure cote du marché**, on obtient systématiquement de **meilleures cotes que la clôture de Pinnacle** :
- CLV moyen +5,0 % au seuil 3 %, et **70 % des paris** ont un CLV positif.
- Plus le seuil monte, plus le CLV monte (+7 % à +9 %).

Le CLV positif est, à long terme, le meilleur indicateur qu'une sélection prend de la valeur. **Mais ici il ne se convertit pas en rendement positif** sur l'échantillon. Deux raisons probables :
1. L'avantage de prix est mince (~5 %) et les résultats réels sont trop bruités pour le confirmer sur 4 000 paris.
2. « Max bat Pinnacle démarginé » est en partie **mécanique** : Max est le sommet du marché, sans marge, alors que Pinnacle porte la sienne. Trouver Max au-dessus de Pinnacle ne prouve pas une vraie erreur de prix.

## Conclusion pratique

- **Le line shopping n'est pas une source de profit en soi.** Parier « dès que le meilleur bookmaker dépasse ma cote juste » aurait perdu de l'argent.
- **Le line shopping est un multiplicateur d'avantage, pas un avantage.** Quand une stratégie a un vrai edge (issu d'un modèle qui bat le marché), prendre toujours la meilleure cote ajoute ~2 à 5 % de CLV par-dessus. C'est défensif, pas offensif.
- **Il faut d'abord un modèle qui bat le marché.** Or le backtest APEX-BSM a montré que le nôtre n'y arrive pas encore. Sans edge en amont, aucun comparateur de cotes ne crée de profit.

## Suite

La seule voie qui reste réaliste : un modèle qui **part des cotes du marché** et ne s'en écarte que là où il a une information en plus (compositions, xG horodatés), validé sur la saison 2026-27 encore jamais vue. Le line shopping viendra ensuite, en bout de chaîne, pour grappiller le CLV sur les paris déjà sélectionnés.
