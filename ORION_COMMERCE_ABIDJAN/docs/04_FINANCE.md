# 04 — Protéger le capital

Les montants ci-dessous sont des **allocations illustratives, sans valeur de
devis**. Le budget réel doit être reconstruit sur les coûts de formalisation, de
l'opérateur et de l'offre retenue. **La fourchette de capital n'autorise aucune
dépense.**

## Enveloppes proposées (illustratives)
| Enveloppe proposée | Capital 300 000 | Capital 750 000 |
|--------------------|-----------------|-----------------|
| Stock pilote rendu Abidjan | 65 000 | 225 000 |
| Échantillons / validation | 20 000 | 40 000 |
| Acquisition / contenus | 20 000 | 50 000 |
| Opérateur / préparation / incidents | 45 000 | 90 000 |
| Formalités / outils / mise en place | 60 000 | 75 000 |
| Réserve initialement protégée | 90 000 | 270 000 |
| **Total FCFA** | **300 000** | **750 000** |

Le dossier CEPICI de personne physique affiche notamment RCCM 10 000 FCFA et code
import-export 30 000 si commerce extérieur, avec d'autres frais selon dossier. **Ce
n'est pas un coût complet garanti** — à confirmer.

## Le coût qui doit réellement être payé
**Coût rendu** (par unité) : produit, transport Chine, inspection, fret, assurance,
formalités/douane, terminal/stockage, transport au stock, frais de paiement/change.

**Puis, pour chaque commande** : publicité, emballage, opérateur, livraison,
commission, échecs, retours et remboursements. **Ajouter les charges fixes** pour
calculer le résultat.

## L'outil : `tools/calculateur.py`
Le modèle distingue **résultat**, **stock** et **cash** — y compris l'**avance
possible de remboursements avant reversement**.

```
python3 -m unittest tools.test_calculateur      # 13 tests financiers
python3 tools/calculateur.py --exercice          # tableau base + défavorable
python3 tools/calculateur.py --json              # sortie JSON
```

### Exercice fictif du calculateur
> Chiffres **entièrement inventés** pour tester l'outil : il ne prédit aucun
> produit. Stress appliqué : coût rendu +20 %, prix −10 %, acquisition +30 %,
> échecs +10 points.

| Poste | Base | Défavorable |
|-------|------|-------------|
| Coût rendu unitaire | 3 300 FCFA | 3 960 FCFA |
| Contribution par expédition | 1 990 FCFA | 186 FCFA |
| Résultat de période avant fiscalité | **+3 970 FCFA** | **−39 694 FCFA** |
| Valeur du stock restant | 6 600 FCFA | 15 840 FCFA |
| Cash libre | +43 700 FCFA | **−20 200 FCFA** |

La lecture qui compte : un pilote **bénéficiaire en base** devient **perte de
~40 000 FCFA et trésorerie négative** sous un stress modéré. **Un bon taux de marge
ne suffit pas** — c'est le cash au pire moment qui décide.

> **Avant achat :** coût complet connu, scénario défavorable examiné, plafond de
> perte fixé, et **réserve disponible au pire moment**.
