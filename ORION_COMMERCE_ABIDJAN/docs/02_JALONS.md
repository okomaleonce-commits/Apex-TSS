# 02 — Sept jalons, avec preuves de passage

On avance **jalon par jalon**. Aucun jalon ne se franchit sans sa preuve, déposée
dans `preuves/`. Le `commerce-controle` peut poser un **VETO** à n'importe lequel.

| Jalon | Preuve AVANT de poursuivre | Agents typiques |
|-------|----------------------------|-----------------|
| **G0 — Mandat** | Capital exact, plafond de perte, lieu de stock et opérateur chiffré. | (dirigeant) |
| **G1 — Marché** | Trois finalistes, concurrence comparable et besoin documenté. | `commerce-marche` |
| **G2 — Échantillon** | Qualité, conformité, devis complet et essais paiement/livraison. | `commerce-achats`, `commerce-import` |
| **G3 — Offre** | Prix total testé auprès de clients ciblés ; refus et intentions tracés. | `commerce-offre` |
| **G4 — Petit lot** | Base/stress, réserve, avis critique et décision du dirigeant. | `commerce-finance`, `commerce-controle` |
| **G5 — Livraison** | Stock reçu, commandes suivies, argent réellement rapproché. | `commerce-stock`, `commerce-commandes` |
| **G6 — Réachat** | Résultat après fixes, pertes sous plafond, cash libre après dettes/réserve. | `commerce-finance`, `commerce-controle` |

## Détail des preuves

### G0 — Mandat
Capital exact engagé pour le test ; **plafond de perte** accepté ; **lieu de stock**
à Abidjan ; **opérateur** identifié avec **son coût chiffré**. Sans ces quatre
éléments, aucune piste ne démarre.

### G1 — Marché
Trois références finalistes ; pour chacune, concurrence **comparable** (page, URL,
date, prix affiché) et **besoin client documenté**. Un prix affiché atteste une
offre concurrente, **pas** une vente actuelle ni notre prix acceptable.

### G2 — Échantillon
Échantillon **reçu** et contrôlé (qualité, conformité) ; **devis complet** (FOB,
MOQ, délais, incoterm) ; **coût rendu** établi ; essais de **paiement** et de
**livraison** réalisés.

### G3 — Offre
Offre démontrable avec **prix total livré testé** auprès de clients ciblés.
**Refus et intentions tracés séparément** des commandes et des paiements.

### G4 — Petit lot
Calcul **base + scénario défavorable** (calculateur), **réserve** vérifiée au pire
moment, **avis critique** (`commerce-controle`) et **décision explicite du
dirigeant**. C'est le dernier filtre avant d'acheter un lot.

### G5 — Livraison
Stock **reçu et compté**, unités vendables chiffrées ; commandes **suivies** ;
**argent réellement rapproché** (preuve de livraison + reversement).

### G6 — Réachat
**Résultat après charges fixes**, pertes **sous le plafond**, **cash libre** après
dettes et réserve. Décision : réachat, ajustement, ou arrêt.

> Un jalon franchi « sur impression » ne compte pas. La preuve, ou rien.
