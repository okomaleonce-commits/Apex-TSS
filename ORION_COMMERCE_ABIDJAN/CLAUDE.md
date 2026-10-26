# ORION COMMERCE / Abidjan — consignes permanentes

> Nom de projet interne. **Aucune société n'est créée par ce kit.** Préparé pour
> Léonce — 15 octobre 2026. Achat envisagé sur Alibaba, vente en ligne, livraison
> depuis Abidjan.

## Mandat (G0)

- **Capital confirmé : 300 000 à 750 000 FCFA.** La fourchette n'autorise aucune
  dépense : toute sortie d'argent est une proposition à décider, jamais un acquis.
- **Disponibilité du dirigeant : moins de cinq heures par semaine.** Une revue
  hebdomadaire de 45 minutes est la cible d'exploitation ; les démarches et le
  travail initial se planifient séparément.
- **La première contrainte est l'exécution quotidienne.** Les agents préparent
  recherches, calculs et procédures. Une personne ou un prestataire à Abidjan
  doit répondre aux clients, contrôler les produits, préparer les colis et suivre
  les incidents. **Son coût entre dans la marge AVANT le premier stock.**

## Le modèle de départ

**Une offre, un petit stock local, un opérateur identifié.** Tester la demande et
comparer l'importation au gros local facturé. Lancer un site complet ou acheter
plusieurs références avant les preuves immobiliserait le capital sans résoudre la
vente.

## Règle d'or — commencer par les preuves

Le système doit pouvoir conclure **« aucune piste ne passe »**. Une intention
client, un devis ou un calcul **ne donne jamais** automatiquement l'autorisation
d'acheter. Les messages, achats, publicités et contrats restent des **propositions
concrètes à décider** par le dirigeant.

1. **Anti-invention, strict.** Ne jamais inventer un prix fournisseur, un coût
   rendu, une cote de livraison, un MOQ ou une intention client. **Donnée absente =
   écrite absente.** Une hiérarchie de sources honnête : un prix FOB ferme daté ≠
   une estimation ; un devis transporteur ≠ un tarif public d'accueil.
2. **Coût complet avant marge.** Aucun pari de rentabilité sans **coût rendu**
   (produit + transport Chine + inspection + fret + assurance + douane/formalités +
   terminal/stockage + transport au stock + paiement/change) PUIS coûts par
   commande (publicité, emballage, opérateur, livraison, commission, échecs,
   retours, remboursements) PUIS charges fixes. Outil : `tools/calculateur.py`.
3. **Scénario défavorable obligatoire** avant tout engagement : coût rendu +20 %,
   prix −10 %, acquisition +30 %, échecs +10 points. Si la réserve ne tient pas au
   pire moment, on n'achète pas.
4. **Plafond de perte fixé et réserve protégée.** Un bon taux de marge ne suffit
   pas : c'est le cash au pire moment qui décide.
5. **Preuves conservées, pas des souvenirs.** Chaque jalon ne se franchit que sur
   la preuve attendue (voir « Sept jalons »). Les preuves vont dans `preuves/`, les
   devis dans `devis/`, les décisions et mouvements dans `registre/` (append-only).
6. **Protection des données clients.** Ne transmettre aux agents que le nécessaire,
   **aucun fichier nominatif partagé**. Vérifier les conditions de vente, la
   facturation et les formalités de données applicables en Côte d'Ivoire avant mise
   en service.

## L'équipe et ses décisions

La **session principale** (Claude Code) coordonne, distribue les missions,
conserve les preuves et arbitre les propositions. **Deux agents simultanés au
maximum**, activés selon le jalon : huit définitions ne signifient pas huit agents
en activité permanente. Les spécialistes ont des outils explicitement limités.

| Agent (`.claude/agents/`)           | Livrable attendu |
|-------------------------------------|------------------|
| `commerce-marche`                   | Comparables locaux, client cible, besoin, test de demande. |
| `commerce-achats`                   | Spécification, comparaison de devis, contrôles vendeur, échantillon. |
| `commerce-import`                   | Importateur officiel, exigences du produit, documents, coût rendu. |
| `commerce-finance`                  | Contribution, charges fixes, point mort, réserve, scénario défavorable. |
| `commerce-offre`                    | Offre démontrable, contenu honnête, test commercial plafonné. |
| `commerce-commandes`                | Confirmation, réponses client, annulations, remboursements (SAV). |
| `commerce-stock`                    | Réception, préparation, suivi des colis et des reversements. |
| `commerce-controle`                 | Raisons de **rejeter** le produit ou de **suspendre** l'engagement. |

`commerce-controle` est l'agent de **contradiction** : il ne cherche pas à valider,
il cherche la raison de ne pas acheter. Son veto est recevable à tout jalon.

## Sept jalons, avec preuves de passage

Aucun jalon ne se franchit sans sa preuve. On avance jalon par jalon.

| Jalon | Preuve AVANT de poursuivre |
|-------|----------------------------|
| **G0 — Mandat**     | Capital exact, plafond de perte, lieu de stock et opérateur chiffré. |
| **G1 — Marché**     | Trois finalistes, concurrence comparable et besoin documenté. |
| **G2 — Échantillon**| Qualité, conformité, devis complet et essais paiement/livraison. |
| **G3 — Offre**      | Prix total testé auprès de clients ciblés ; refus et intentions tracés. |
| **G4 — Petit lot**  | Base/stress, réserve, avis critique et décision du dirigeant. |
| **G5 — Livraison**  | Stock reçu, commandes suivies, argent réellement rapproché. |
| **G6 — Réachat**    | Résultat après fixes, pertes sous plafond, cash libre après dettes/réserve. |

## Commandes de travail (`.claude/commands/`)

| Commande | Usage |
|----------|-------|
| `/commerce-demarrer` | Charger le mandat et préparer le premier jalon. |
| `/commerce-reprendre`| Continuer depuis les preuves et décisions existantes. |
| `/commerce-audit`    | Contester la dernière proposition et contrôler ses chiffres. |
| `/commerce-pilotage` | Rapprocher activité, stock, livraisons et fonds réellement reçus. |

## Trois pistes à tester (aucune validée)

Elles passent seulement un **premier filtre de simplicité matérielle**. Ni demande,
ni rentabilité démontrées. Aucun prix fournisseur ferme ni coût rendu Abidjan n'est
disponible. Détail dans `docs/03_PISTES.md`.

1. **Entretien manuel** (lot chiffons + petite brosse, sans liquide).
2. **Rangement de voyage** (pochettes, nombre et dimensions précis).
3. **Accessoires satin** (bonnet ajustable ; étudier aussi les salons).

> Un produit peu cher à acheter peut être **trop peu cher à vendre** pour financer
> publicité, préparation et livraison. Le lot doit rendre un service réel au client.

## Points à régler avant engagement (sources à reconfirmer)

- **Douane :** seuil FDI lié à la valeur FOB, documenté à partir de 500 000 FCFA ;
  ce n'est pas le budget du projet ni une exonération en dessous. Confirmer la
  procédure du lot avec le déclarant.
- **Livraison :** un tarif public d'accueil (ex. ~950 FCFA jusqu'à 5 kg hors
  TVA/taxes) **ne remplace pas** un devis précisant éligibilité, collecte, gabarit,
  retours et délai de reversement. Aucun partenaire n'est retenu.
- **Achat :** examiner la protection Trade Assurance et les termes de la commande ;
  elle ne remplace pas un contrôle qualité et ne garantit pas la rentabilité.
- **Formalisation :** le dossier CEPICI de personne physique affiche notamment
  RCCM et code import-export selon dossier. Ce n'est pas un coût complet garanti.

## État réel (anti-illusion)

Vérifié statiquement : 8 définitions d'agents, 4 commandes, 13 tests financiers
réussis. **À vérifier sur l'ordinateur du dirigeant :** accès, chargement et
invocation réelle des agents. **Aucun** produit sélectionné, fournisseur contacté,
compte commercial connecté, achat engagé ou livraison réalisée par ce kit. Le kit
prépare la décision ; il ne la prend pas et ne dépense rien.

## Cellule FOUNDER — tester l'entreprise avant de la lancer (`.claude/skills/founder-*`)

Installé le 26/10/2026 : **« The Founder skill »** (Jake Schincariol, MIT, 11 skills +
4 outils Python stdlib, **sans réseau** — vérifié : 29/29 tests du dépôt au clonage).
C'est un **renfort de la méthode ORION**, pas un raccourci : mêmes règles d'or
(anti-invention, preuves, gel). Le panel est **simulé** (acheteurs IA) → à lire comme
une **borne haute**, à confirmer par de vrais clients (le test de demande réel reste
le juge). Localisation : le CFO affiche en **FCFA** (copie locale modifiée).

Branchement sur les 7 jalons :

| Jalon | Skill(s) founder | Apport |
|-------|------------------|--------|
| G0 Mandat | `/founder-board` | 3 regards (Offres/Hormozi, Monopole/Thiel, Produit/Jobs) votent go / go-if / pass. |
| G1 Marché | `/founder-competitors`, `/founder-consumer --quick` | Concurrents sourcés + pré-test rapide de demande. |
| G2 Échantillon | `/founder-cfo`, `/founder-pricing` | Économie unitaire (point mort, année 1, cash) + fourchette de prix acceptable (Van Westendorp). |
| G3 Offre/Demande | `/founder-consumer`, `/founder-offer`, `/founder-marketing`, `/founder-brand` | Panel 100 acheteurs, stack d'offre, campagne 30 j, nom/visuel. |
| G4 Petit lot | `/founder-cfo` (what-ifs) | Stress prix/volume/coûts avant d'acheter. |
| G5 Livraison | `/founder-ops` | Fournisseurs, routines, risques → coûts réels renvoyés au CFO. |
| G6 Réachat | `/founder-plan` | Compile tout en un plan + **verdict calculé** : Rentable / Pas encore / Incomplet. |

- **Fichiers de travail** : dans `founder/` (les skills se lisent entre eux).
  Déjà créés pour la boîte 66 L : `numbers.json` (CFO), `customer.json` + `pitch.md`
  (panel), `cfo.md` (sortie CFO).
- **Les outils ne remplacent pas** le module BSM-équivalent du kit (`tools/calculateur.py`)
  ni les preuves : ils les **croisent**. Un chiffre du CFO reste un **estimé** tant
  qu'un devis réel ne l'a pas confirmé.
- **Panel 100 acheteurs = sous-agents** : ne le lancer que sur demande explicite du
  dirigeant (coût en agents). `--quick` pour une lecture légère.

> Crédit : The Founder skill — Jake Schincariol (opusjake.ai), licence MIT
> (`.claude/skills/FOUNDER_LICENSE`). Copie locale ; `founder-cfo` localisé en FCFA.
