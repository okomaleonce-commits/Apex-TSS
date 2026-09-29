# Modèle ancré sur le marché — calibration et test en avant

Outil : `tools/apex_market.py` (`calibrate`, `forward`). Principe : on part des probabilités du **marché** (Pinnacle avant-match démarginé), le meilleur estimateur connu, et le modèle APEX-BSM ne sert qu'à s'en écarter, d'un poids `w` estimé sur données passées :

`p_final ∝ p_marché^(1−w) × p_modèle^w`  ·  w=0 → marché pur, w=1 → modèle pur.

## Calibration historique (`market-20260929T144246Z`)

8 championnats · validation 2023-25 · test final isolé 2025-26 (3 408 matchs).

| Modèle | log-loss 1X2 (test) |
|---|---|
| Marché pur | **1,0027** |
| Mélange au w optimal | 1,0027 |
| Modèle pur | 1,0238 |

**Le poids optimal est w = 0.** Sur la validation, la log-loss monte de façon monotone dès qu'on ajoute du modèle (w=0 : 0,9944 → w=1 : 1,0137). Le mélange optimal est donc **exactement le marché**, et son écart avec le marché est nul (IC95 [−0,000 ; −0,000]).

**Verdict : le modèle n'apporte rien au marché.** C'était attendu, et c'est cohérent avec tout ce qui précède : le modèle est construit **uniquement à partir des buts passés**, une information que le marché intègre déjà (et mieux). Mélanger deux estimateurs quand l'un domine strictement l'autre ne peut pas améliorer le meilleur.

## Ce que ça prouve — et la seule voie qui reste

Le problème n'est pas la façon de mélanger. C'est que le modèle **n'a aucune information que le marché ne possède pas**. Pour espérer battre le marché, il faut lui donner un signal qu'il price lentement :

1. **Le mouvement de cote ouverture → clôture** : si on peut anticiper vers où la ligne va bouger, on bat la clôture. Nécessite des relevés horodatés à l'ouverture puis avant le match — c'est ce que le pipeline quotidien accumule désormais.
2. **Les compositions au bon moment** : parier entre la publication des compositions et la correction de la cote.
3. **Des xG horodatés** en remplacement des buts bruts.

Aucun de ces signaux n'est backtestable sur l'historique football-data (pas d'horodatage d'ouverture, pas de compositions passées). **Ils ne peuvent être validés qu'en avant**, sur la saison 2026-27.

## Test en avant (`forward`)

`python3 tools/apex_market.py forward` lit le journal `ledger/` et, pour chaque match **réglé**, compare la log-loss du marché, du modèle et du mélange sur des matchs que rien n'a servi à régler. Le pipeline quotidien l'exécute et l'ajoute au rapport du jour.

- État actuel : 1 match réglé — **très loin** des quelques centaines nécessaires pour conclure.
- Le journal se remplit à chaque passage quotidien (relevé horodaté des cotes + règlement automatique).
- Tant que le modèle reste « buts passés » seuls, l'attente honnête est : **il égalera le marché, sans le battre**. Le test en avant sert d'abord à mesurer, sans biais, l'apport des signaux d'information qu'on ajoutera ensuite (mouvement de cote, compositions, xG).
