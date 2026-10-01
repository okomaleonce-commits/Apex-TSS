---
name: apex-character
description: Caractère & schéma d'un match football, INDÉPENDANT des cotes et du pricing statistique. DÉCLENCHER pour classer un match (avant ou après) par sa forme réelle — profil dominant (Verrou, Match fermé, Équilibré, Bataille ouverte, Victoire nette, Démonstration) et schéma de dynamique MT→FT (renversement, égalisation tardive, seconde période folle, BTTS…). Outil mécanique : tools/apex_character.py (backtest, classify, profile). Se branche sur APEX-WORM, APEX-SYNC et APEX-PROTOCOL car tous disposent déjà des λ structurels. Ne produit PAS de value ni de cote — c'est une lecture descriptive.
---

# APEX-CHARACTER — caractère de match (odds-free)

Un match a un **caractère** : sa forme réelle sur le terrain, qui existe **en dehors** du marché et
des modèles de value. Ce module le mesure sans jamais regarder une cote.

## Taxonomie

**Profil primaire** (partition exhaustive du score final — 6 classes mutuellement exclusives) :

| Profil | Définition |
|---|---|
| 🔒 VERROU | 0-0 |
| 🧱 MATCH_FERME | 1 seul but (1-0 / 0-1) |
| ⚖️ EQUILIBRE | écart ≤1, total 2-3 buts |
| 🔥 BATAILLE_OUVERTE | écart ≤1, total ≥4 buts |
| ✔️ VICTOIRE_NETTE | écart de 2 buts |
| 💥 DEMONSTRATION | écart ≥3 buts |

**Schéma de dynamique** (tags MT→FT, indépendants) : `RENVERSEMENT` (l'équipe menée à la MT gagne),
`EGALISATION_TARDIVE`, `DECISION_2E_PERIODE`, `SECONDE_PERIODE_FOLLE`, `BTTS`, `OVER25`, `FERME`,
`CLEAN_SHEET_DOM/EXT`.

## Avant / après match

- **AVANT** (schéma attendu) : `profile(lh, la)` calcule la distribution des 6 profils à partir des
  **seuls λ structurels** (forces d'équipe → Poisson). Zéro cote. C'est ce que WORM/SYNC/PROTOCOL
  affichent comme « Caractère attendu ».
- **APRÈS** (schéma observé) : `classify(hg, ag, hthg, htag)` donne le profil réel + les tags de
  dynamique. Sert aux bilans et à l'autopsie.

## Outil

```bash
python3 tools/apex_character.py backtest            # classe + backteste tout data/history/*.csv
python3 tools/apex_character.py classify --hg 4 --ag 2 --hthg 1 --htag 0   # profil + tags (après)
python3 tools/apex_character.py profile --lh 2.1 --la 0.6                  # schéma attendu (avant)
```

Sorties : `backtests/character-<ts>/REPORT.md` + `data/character/latest_params.json`
(taux de profils par ligue, taux de tags, calibration).

## Honnêteté (règles APEX)

- Le prédicteur structurel **ne bat pas** la simple fréquence de base sur le profil top-1 (backtest
  walk-forward : ~27 % vs baseline ~29 %). Le module est **descriptif**, pas un edge de pari : il
  éclaire le *type* de match attendu, il ne dit pas quoi parier.
- Aucune donnée inventée : si les λ ou la mi-temps manquent, le profil/tag est **omis** (« — »).
- Walk-forward strict : chaque prédiction n'utilise que les matchs antérieurs.

## Intégration

- **APEX-WORM** : colonne « Caractère attendu » dans le digest e-mail (via λ du snapshot).
- **APEX-SYNC** : champ `caractere_attendu` sur chaque candidat de la file PROTOCOL.
- **APEX-PROTOCOL** : à citer dans la lecture tactique (S3) et l'autopsie post-match — le profil
  attendu vs observé qualifie la surprise du match indépendamment du résultat de pari.
