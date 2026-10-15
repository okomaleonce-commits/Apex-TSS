---
description: Contester la dernière proposition ORION COMMERCE et contrôler ses chiffres.
---

Tu lances un **audit de contradiction** sur la dernière proposition (achat, lot,
publicité, contrat) avant qu'elle ne devienne une décision du dirigeant.

## Étapes
1. **Activer `commerce-controle`** sur la proposition courante.
2. **Contrôler les chiffres** : relancer le calculateur
   (`python3 -m unittest tools.test_calculateur` puis `python3 tools/calculateur.py --exercice`)
   et **recalculer le scénario défavorable** avec les vraies entrées de la
   proposition (coût rendu +20 %, prix −10 %, acquisition +30 %, échecs +10 points).
3. **Appliquer la grille de veto** : coût rendu incomplet/inventé, demande non
   prouvée, cash qui ne tient pas, prix trop bas pour financer la vente, risque hors
   moyens, données clients exposées.
4. **Rendre un verdict** : `VETO` motivé (fait + preuve) **ou** « pas de motif de
   rejet trouvé à ce stade ». Jamais une validation : l'audit ne valide pas, il
   cherche la faille.

## Garde-fous
- Un bon taux de marge ne suffit pas : vérifier le **cash au pire moment** et la
  **réserve** sous le plafond de perte.
- **Anti-invention** : si une entrée manque, l'audit nomme le trou et bloque, il ne
  le comble pas.
- Le verdict est écrit (`preuves/VETO_<jalon>_<piste>.md` si VETO) et consigné dans
  `registre/`.
