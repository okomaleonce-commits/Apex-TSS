# registre/ — journal append-only

Ce dossier est le **journal des décisions et des mouvements**. Règle unique :
**on ajoute, on ne réécrit jamais** (append-only). Une erreur se corrige par une
**nouvelle ligne** de correction datée, pas par une modification de l'ancienne.

## Ce qui est consigné
- **Décisions du dirigeant** : franchissement de jalon, autorisation d'une dépense,
  choix d'un prestataire, réachat/arrêt.
- **Mouvements d'argent** : commande/échantillon payé, encaissement **reversé**,
  remboursement, avance de remboursement.
- **VETO** de `commerce-controle` (avec renvoi au fichier `preuves/VETO_*`).

## Format conseillé (`registre/journal.csv`)
```
date_iso,type,jalon,piste,montant_fcfa,sens,preuve,note
2026-10-16,DECISION,G0,,,,preuves/G0_mandat.md,"Mandat confirmé : capital 300k, plafond perte 90k"
2026-10-20,VETO,G2,entretien_manuel,,,"preuves/VETO_G2_entretien_manuel.md","Coût rendu incomplet : douane non confirmée"
```
- `type` : DECISION | MOUVEMENT | VETO | NOTE
- `sens` : entree | sortie (vide si non financier)
- `montant_fcfa` : entier, vide si non financier

> **Anti-invention** : une recette non reversée n'est **pas** un encaissement
> (sens=entree) — la noter en attente. Aucune donnée nominative client ici.
