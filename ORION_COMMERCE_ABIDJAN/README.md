# ORION COMMERCE / Abidjan

**Lancer un commerce. Commencer par les preuves.**

Kit Claude Code pour tester — avec discipline — un petit commerce d'importation
(Alibaba → vente en ligne → livraison Abidjan), **sans immobiliser le capital
avant d'avoir prouvé la vente**. Nom de projet interne ; **aucune société créée**
par ce kit.

- **Capital confirmé :** 300 000 à 750 000 FCFA (la fourchette n'autorise aucune dépense).
- **Disponibilité :** moins de cinq heures par semaine.
- **Règle d'or :** le système doit pouvoir conclure « aucune piste ne passe ». Une
  intention, un devis ou un calcul n'autorise jamais à acheter.

## Démarrer
```
claude
/commerce-demarrer
```
Voir `docs/00_INSTALLATION.md`. **À lire en premier : `CLAUDE.md`** (mandat, règles,
7 jalons).

## Contenu
| Élément | Où |
|---------|-----|
| Mandat + règles + jalons G0–G6 | `CLAUDE.md` |
| Documentation (modèle, jalons, pistes, finance, flux, données) | `docs/` |
| 8 agents spécialisés | `.claude/agents/` |
| 4 commandes de travail | `.claude/commands/` |
| Calculateur financier + 13 tests | `tools/` |
| Modèles de demande de devis (vides) | `devis/` |
| Journal append-only (décisions & mouvements) | `registre/` |
| Preuves par jalon et par piste | `preuves/` |

## Vérifier le kit
```
python3 -m unittest tools.test_calculateur -v   # 13 tests financiers
python3 tools/calculateur.py --exercice          # exercice fictif (chiffres inventés)
```

## Ce que le kit ne fait pas
Aucun produit sélectionné, fournisseur contacté, compte connecté, achat engagé ou
livraison réalisée. Le kit **prépare** la décision et conserve les preuves ; il ne
dépense rien et ne décide rien à la place du dirigeant.

## Les 8 agents
`commerce-marche` · `commerce-achats` · `commerce-import` · `commerce-finance` ·
`commerce-offre` · `commerce-commandes` · `commerce-stock` · `commerce-controle`
(contradiction : cherche la raison de **ne pas** acheter — veto recevable à tout jalon).
