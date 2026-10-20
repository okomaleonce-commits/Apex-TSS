---
name: apex-engine-omega
description: Couche méta de TRIAGE, TRANSPARENCE et CLASSEMENT au-dessus de FUSION — pas un moteur de pricing. DÉCLENCHER quand la chaîne APEX produit trop de « NO BET secs » et qu'on veut (1) comprendre POURQUOI chaque match s'abstient (gate bloquante, donnée manquante, seuil raté, verdict des moteurs), (2) voir le classement « closest-to-actionable » de tous les marchés d'un match avec un EV honnête, (3) une WATCH list réelle, (4) des paliers BET/LEAN/WATCH/PASS adossés à l'EV mesuré. Outil mécanique tools/apex_omega.py (triage, card, demo). OMEGA NE PRICE PAS, NE FORCE AUCUNE RECOMMANDATION, N'INVENTE NI COTE NI EDGE : il lit les verdicts ORION de FUSION et classe. Gel actif, gates conservées, anti-invention. Ne remplace ni apex_bsm (pricing) ni apex_fusion (digest) : il les enrichit en tête.
---

# APEX-OMEGA — triage, transparence et classement

OMEGA répond à un seul défaut produit : **trop de NO BET *secs***. Un « NO BET » opaque n'est pas
actionnable. OMEGA ne supprime pas l'abstention — c'est une sortie juste quand il n'y a pas de bord —
il la rend **lisible** et classe ce qui s'en rapproche le plus.

OMEGA **n'est pas un moteur de ligue** et ne figure pas parmi les `apex-engine-*` de pricing. C'est une
**couche au-dessus de FUSION** : elle lit les verdicts ORION déjà calculés et produit, par match, un
triage de marchés et une explication d'abstention.

## Ce que OMEGA fait

1. **Explique chaque abstention** — pour tout match sans mise, il énumère :
   - la **gate bloquante** (cases de la matrice du gel ❌ + portes globales dures) ;
   - les **données manquantes** (sim BSM absente, voix sharp absente, indice comportemental absent, cote absente) ;
   - le **seuil raté** (meilleur EV coté vs 3 %, voix indépendantes < 3, désaccord élevé) ;
   - le **verdict des moteurs** (ORION, tier WORM, confiance, désaccord).

2. **Classe tous les marchés** d'un match (« closest-to-actionable ») — résultat (1X2, double chance),
   buts (O/U 0.5→4.5, BTTS), handicap asiatique — avec proba consensus, cote réelle, **EV honnête** et poids.

3. **WATCH list réelle** — les marchés sans cote dans le snapshot, à suivre pour un mouvement de ligne.

4. **Paliers adossés à l'EV mesuré** :

   | Palier | Condition | Mise réelle |
   |---|---|---|
   | **BET**   | EV ≥ 3 % (cote réelle) | 0 — **gel actif** |
   | **LEAN**  | EV 1,5–3 % (cote réelle) | 0 — gel actif |
   | **WATCH** | EV 0–1,5 %, **ou** cote absente (à tracker) | 0 |
   | **PASS**  | **tous** les marchés cotés à EV < 0 | 0 — informatif |

   En **PASS**, OMEGA affiche quand même les **3 marchés les moins mauvais** à titre informatif, EV négatif
   clairement marqué. Un PASS est un classement de marchés défavorables, **jamais un vide**.

## Ce que OMEGA NE fait PAS (refusé sciemment)

La directive initiale « APEX-OMEGA souverain » exigeait de **supprimer le NO BET** et de forcer une
recommandation partout. Ces mécaniques **ne sont pas implémentées**, car elles reviennent à recommander
des paris à espérance négative — ce qui perd de l'argent et contredit l'anti-invention d'APEX :

- **Pas de recommandation forcée à EV négatif** (ex-article 4.3). En PASS, les 3 marchés sont *informatifs*, pas des mises.
- **Pas d'« emprunt » de probabilité fabriqué** quand le moteur de ligue est muet (ex-article 7). Moteur muet ⇒ voix écrite absente.
- **Pas de relance anti-NO BET** qui forcerait une sortie (ex-article 8).
- **Pas d'abrogation des gates** (ex-article 11.4). Les gates G0–G7 et la matrice du gel restent entières.

## Garde-fous conservés (inchangés)

- **Gel actif** : aucune mise réelle émise. La mise de **tout** palier, BET inclus, vaut 0 tant que le gel
  n'est pas levé par décision explicite. Sortie étiquetée « RECOMMANDATION — gel actif ».
- **Gates G0–G7 + matrice du gel** (`tools/gel_matrix.py`) : un palier n'affirme jamais ce que la matrice
  nie. Les deux portes globales (modèle supérieur au marché ; CLV cumulé non négatif) restent dures.
- **Plafonds Kelly fractionné, anti-corrélation, 1 pari/marché/match**, journal append-only.
- **ANTI-INVENTION** : un EV n'est calculé que s'il existe une **cote réelle horodatée**. Pas de cote ⇒ pas
  d'EV, le marché part en WATCH. Corners/cartons/buteurs absents du snapshot ⇒ écrits absents.
- **Respect des backtests négatifs** : si le statut backtest est « NON SUPÉRIEUR AU MARCHÉ », la porte
  globale est rouge et **tous** les matchs restent GELÉS. OMEGA le rend visible, il ne le contourne pas.

## Probabilités : structure WORM, pas BSM

Les probas consensus viennent de la **structure WORM** (λ de classement, Monte-Carlo Dixon-Coles,
`apex_bsm.simulate_scores` + `markets_from_sims`). Ce **n'est pas** le modèle BSM calibré (règle CLAUDE.md) :
lecture descriptive, non validée. Rien n'est émis comme pari réel.

## Mécanique

```bash
python3 tools/apex_omega.py triage [--date J]   # blocs de triage par match → reports/omega/<J>.triage.{txt,json}
python3 tools/apex_omega.py card   [--date J]   # carte HTML OMEGA seule
python3 tools/apex_omega.py demo                # démonstration autonome (sans réseau ni snapshot)
```

## Place dans la chaîne

```
APEX-WORM (snapshot) → FUSION (verdicts ORION par match) → OMEGA (triage/classement/explication)
```

`apex_fusion.build_fusion_digest` greffe la **carte OMEGA en tête** du digest unique (avant la carte
ORION), puis la matrice du gel et les marchés étendus. **Un seul digest, un seul email** — OMEGA ne crée
pas un envoi séparé. L'email se termine, comme tout passage APEX, par `mcp__Gmail__send_message`
(`reports/fusion/<J>.email.html`).

OMEGA se déclenche automatiquement dans FUSION ; en interactif, `apex_omega.py triage` donne le détail
par match (classement + « pourquoi pas de mise ») que la carte HTML résume.
