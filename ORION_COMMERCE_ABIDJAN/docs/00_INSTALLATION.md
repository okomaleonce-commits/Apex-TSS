# 00 — Installation & démarrage

## Pré-requis
- **Claude Code** installé (CLI, app desktop, web ou extension IDE). En cas
  d'absence, suivre l'installation officielle ; un accès éligible est nécessaire.
- **Python 3.10+** pour le calculateur financier (bibliothèque standard uniquement,
  aucune dépendance à installer).

## Mise en place
1. **Décompresser l'archive entière**, y compris le dossier caché `.claude`, dans
   un dossier `ORION_COMMERCE_ABIDJAN` — là où se trouve `CLAUDE.md`.
2. Ouvrir un **terminal** dans ce dossier.
3. Lancer Claude Code :
   ```
   claude
   ```
4. Dans Claude Code, démarrer :
   ```
   /commerce-demarrer
   ```

## Vérifier que le kit tourne
- **Agents** (8) : listés dans `.claude/agents/`. Dans Claude Code, vérifier qu'ils
  se chargent et s'invoquent réellement sur votre ordinateur.
- **Commandes** (4) : `/commerce-demarrer`, `/commerce-reprendre`,
  `/commerce-audit`, `/commerce-pilotage` (dossier `.claude/commands/`).
- **Calculateur** (13 tests financiers) :
  ```
  python3 -m unittest tools.test_calculateur -v
  python3 tools/calculateur.py --exercice
  ```
  Les 13 tests doivent passer ; l'exercice fictif doit s'afficher (chiffres
  **inventés**, pour tester l'outil — pas un devis).

## Ce que le kit NE fait pas
Aucun compte commercial connecté, aucun fournisseur contacté, aucun achat engagé,
aucune livraison réalisée. Le kit **prépare** la décision et conserve les preuves ;
il ne dépense rien et ne décide rien à la place du dirigeant.

## Carte du kit
```
ORION_COMMERCE_ABIDJAN/
├── CLAUDE.md                 # mandat + règles + 7 jalons (à lire en premier)
├── README.md
├── docs/                     # 00 installation … 06 données & sécurité
├── .claude/
│   ├── agents/               # 8 agents spécialisés
│   └── commands/             # 4 commandes de travail
├── tools/
│   ├── calculateur.py        # calculateur financier (stdlib)
│   └── test_calculateur.py   # 13 tests
├── devis/                    # modèles de demande de devis (vides)
├── registre/                 # journal append-only (décisions & mouvements)
└── preuves/                  # preuves par jalon et par piste
```
