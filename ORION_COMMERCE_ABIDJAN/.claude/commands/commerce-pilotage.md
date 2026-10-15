---
description: Rapprocher activité, stock, livraisons et fonds réellement reçus (tableau hebdomadaire).
---

Tu produis la **revue hebdomadaire du dirigeant** (cible : 45 minutes). Objectif :
rapprocher ce qui a été vendu, livré et **réellement encaissé** — pas ce qui a été
promis.

## Étapes
1. **Collecter le réel** : activer `commerce-stock` (réceptions, colis, reversements)
   et `commerce-commandes` (commandes, annulations, remboursements), puis lire
   `registre/`.
2. **Construire le tableau hebdomadaire** :
   - ventes gardées, recettes **reçues** (reversement prouvé), contribution ;
   - **résultat après fixes**, stock vendable restant, **créances coursier/échues** ;
   - cash libre, incidents, **temps humain** consommé.
3. **Distinguer trois choses** (via `commerce-finance` / `calculateur.py`) :
   **résultat**, **stock** (valeur immobilisée) et **cash** (dont avance de
   remboursements avant reversement).
4. **Évaluer G6 — Réachat** si le pilote est bouclé : résultat après fixes, pertes
   **sous plafond**, cash libre après dettes et réserve. Décider : réachat,
   ajustement, ou arrêt.

## Garde-fous
- **Anti-invention** : une recette non reversée n'est **pas** un encaissement ; une
  intention n'est pas une vente. Tracer séparément.
- **Protection des données clients** : aucun fichier nominatif partagé.
- Décision de réachat = décision du dirigeant, après contradiction
  (`/commerce-audit`). Tout est consigné dans `registre/` (append-only).
