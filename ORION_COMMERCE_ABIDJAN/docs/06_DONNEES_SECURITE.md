# 06 — Données clients & sécurité

Le kit prépare des décisions, mais dès qu'il y a des clients réels, il y a des
**données personnelles** et des **obligations**. Ces principes sont permanents.

## Principes
1. **Minimisation.** Ne collecter et ne faire circuler que le **strict nécessaire**
   à la commande et à la livraison (nom/contact, adresse/repère, article, montant).
2. **Aucun fichier nominatif partagé aux agents.** Les agents travaillent sur des
   données **agrégées ou anonymisées** ; les coordonnées restent côté opérateur /
   dirigeant.
3. **Traçabilité, pas surveillance.** On trace les **mouvements** (commande,
   livraison, paiement, retour), pas plus que nécessaire.
4. **Conservation limitée.** Garder les données le temps utile (SAV, comptabilité),
   puis les supprimer selon la règle fixée.

## À vérifier avant mise en service (Côte d'Ivoire)
- **Conditions de vente** et mentions obligatoires (prix total, délais, retours).
- **Facturation** conforme pour une personne physique / le statut retenu (CEPICI,
  RCCM, code import-export selon dossier).
- **Formalités de protection des données** applicables en Côte d'Ivoire (déclaration
  éventuelle, consentement, droits des clients).

> Ces vérifications sont des **points à confirmer** auprès des autorités /
> professionnels compétents. Le kit ne fournit pas d'avis juridique ; il impose
> seulement de **ne pas mettre en service avant d'avoir vérifié**.

## Hygiène technique minimale
- Comptes (fournisseur, paiement, livraison) avec mots de passe forts et, si
  possible, double authentification.
- Accès partagés limités ; révoquer un accès prestataire dès la fin du besoin.
- Sauvegarde simple du `registre/` (journal append-only) et des `preuves/`.
