---
name: orion-superbrain
description: Cerveau collectif hiérarchique ORION — orchestre 15 agents spécialisés (ORION-Core, SENSOR, MEMORY, PATTERN, FORECAST, CONTEXT, SKEPTIC, RED TEAM, SIMULATOR, RISK, DECISION, AUDITOR, META, LEARNER, EXECUTOR) en un système qui CONTREDIT, MESURE SON DÉSACCORD, SE SOUVIENT DE SES ÉCHECS et peut DÉCIDER DE NE RIEN FAIRE. DÉCLENCHER pour une analyse à fort enjeu/incertitude où l'on veut spécialisation + contradiction + arbitrage plutôt qu'un avis unique. Cœur mécanique : tools/orion_consensus.py. Ne price pas de pari réel (gel), n'invente rien ; s'appuie sur les outils APEX existants (apex_worm, apex_bsm, apex_sync, apex_mi, apex_validate, apex_clv).
---

# ORION SUPERBRAIN — cerveau collectif

La puissance ne vient PAS du nombre d'agents mais de leur **spécialisation + contradiction + mémoire
+ mesure de leur propre incertitude + une autorité centrale qui peut s'abstenir**. Le piège inverse —
15 agents qui parlent en même temps — ne produit que du bruit.

## Architecture (régions d'un même cerveau)

```
MONDE → SENSOR → MEMORY → PATTERN → FORECAST
                                     ├─ SIMULATOR
                                     ├─ SKEPTIC → RED TEAM
                                     ├─ CONTEXT
                                     └─ RISK
            tous → DECISION → AUDITOR → META → ORION-Core → EXECUTOR
                        RÉSULTAT → LEARNER → MEMORY   (boucle apprenante)
```

Chaque agent est défini dans `.claude/agents/orion-*.md` et câblé sur un outil APEX réel :
SENSOR→apex_worm + essaims apex-mi/apex-bi ; MEMORY→bilans/snapshots/CLV ; PATTERN→moteurs apex_worm
+ apex_character ; FORECAST/SIMULATOR→apex_bsm (Monte-Carlo) ; CONTEXT→apex-bi-* ;
SKEPTIC/RED TEAM→apex-llm-council-audit / S8 ; RISK→apex_sync ; DECISION/META→tools/orion_consensus.py ;
AUDITOR→apex_common + apex_validate + apex_clv ; LEARNER→apex_clv/bilans ; EXECUTOR→sorties de recherche.

## Règle fondamentale
**Aucun agent ne produit seul la décision finale.** Un « 78 % » de PATTERN est obligatoirement confronté
à SKEPTIC (arguments contre), RED TEAM (démontrer que c'est faux), CONTEXT (ce que les stats ignorent),
RISK (coût d'une erreur), AUDITOR (les données sont-elles fiables ?). DECISION ne conclut qu'ensuite :
**ACCEPTER / REJETER / ATTENDRE / COLLECTER DAVANTAGE**.

## Trois niveaux de pensée (choisis par ORION-Core)
1. **Réflexe** — situation connue, faible enjeu : peu d'agents, réponse rapide.
2. **Analyse** (normal) — branches parallèles confrontées.
3. **Deep Reasoning** — déclenché si désaccord élevé, incertitude forte, données contradictoires, enjeu
   important ou anomalie. ORION-Core lance des hypothèses concurrentes (A normal · B contraire ·
   C donnée cachée/manquante · D rupture historique) ; SIMULATOR les teste.

## Mesure du désaccord (le cœur, `tools/orion_consensus.py`)
Les avis sont agrégés par `arbitrate(votes)` :
- **META** fond d'abord les votes partageant une même `source` → compte les **voix INDÉPENDANTES**,
  pas les agents (cinq agents sur une même mauvaise source ≠ consensus).
- La **confiance finale = consensus pénalisé par la dispersion** des voix indépendantes.
  Ex. voix 82/79/55/48/61 → consensus ≈ 65 %, désaccord élevé → **confiance ≈ 58 % → ATTENDRE**.
- Trop peu de voix indépendantes → **COLLECTER**. L'abstention est une décision pleine.

## Mémoire à quatre niveaux (MEMORY)
Working (tâche) · Episodic (cas similaires) · Semantic (règles, moteurs de ligue) ·
**Failure** (échecs : paris perdus, CLV négatif). La Failure Memory produit des corrections mesurées
(`failure_bias`) appliquées AVANT l'arbitrage.

## Invariants (hérités de l'audit APEX)
- **Gel actif** : ORION est un cerveau de RECHERCHE. `arbitrate` rétrograde tout « ACCEPTER » en
  ATTENDRE tant que `PROMOTION_FROZEN` est vrai. EXECUTOR n'émet jamais un pari réel.
- **Anti-invention** : donnée absente écrite absente ; chaque avis porte sa `source`.
- **CLV = vérité** : un signal n'a de valeur que s'il bat la clôture (`tools/apex_clv.py`). Tant que le
  CLV cumulé est négatif, DECISION/AUDITOR refusent toute promotion.

## Lancer un cycle
1. ORION-Core choisit le niveau de pensée et séquence les branches (via l'outil Task/les agents orion-*).
2. Chaque agent rend un avis sourcé {agent, p, source}.
3. DECISION appelle `python3 tools/orion_consensus.py` (ou `arbitrate`) sur les avis.
4. AUDITOR + META valident ; ORION-Core tranche ; EXECUTOR produit la sortie de recherche.
5. Après résultat : LEARNER met à jour la Failure Memory.
