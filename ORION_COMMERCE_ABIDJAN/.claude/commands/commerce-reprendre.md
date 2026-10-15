---
description: Reprendre ORION COMMERCE depuis les preuves et décisions déjà enregistrées.
---

Tu reprends une session ORION COMMERCE / Abidjan en cours.

## Étapes
1. **Relire l'état réel** : parcourir `preuves/` (par jalon et par piste),
   `devis/` (devis remplis) et `registre/` (décisions et mouvements, append-only).
2. **Reconstituer la position** : pour chaque piste active, quel **jalon** est
   franchi, sur **quelle preuve**, et quel **VETO** éventuel de `commerce-controle`
   est ouvert.
3. **Nommer la prochaine preuve attendue** (voir la table des 7 jalons dans
   `CLAUDE.md` / `docs/02_JALONS.md`) et **activer le ou les agents** nécessaires
   (deux au maximum).
4. **Ne jamais ré-inventer** une donnée déjà manquante : si une preuve manquait, la
   demander ou la marquer encore absente — ne pas la combler.

## Garde-fous
- Un jalon déjà noté « non franchi » ne devient pas franchi sans sa preuve.
- Un VETO ouvert bloque le jalon concerné tant qu'il n'est pas levé par un fait.
- Toute nouvelle décision/mouvement → `registre/`. Aucune dépense sans décision du
  dirigeant.
- Fin de session : état des jalons mis à jour + prochaine preuve attendue.
