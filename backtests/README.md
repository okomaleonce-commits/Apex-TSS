# Backtests APEX-BSM — bilan et journal des versions

## Run de référence : `bsm-20260928T153332Z` (modèle `apex-bsm-1.0.0`)

- **Période et volume**
  - 8 championnats : Premier League, Championship, League One, League Two, Liga, Serie A, Bundesliga, Ligue 1.
  - Apprentissage : 2021-22 et 2022-23. Validation (réglage) : 2023-24 et 2024-25. **Test final isolé : 2025-26**, soit 3 279 matchs comparables.
- **Méthode**
  - Walk-forward hebdomadaire, prévision le lundi 00:00.
  - Poisson attaque/défense pondéré dans le temps, rétréci vers la moyenne, Dixon-Coles.
  - 256 variantes testées sur la validation uniquement.
- **Paramètres gelés** : ξ = 0,0015/jour, K = 6, ρ = −0,05, σ = 0 (le rythme partagé n'apportait rien).

### Résultats sur le test final

| | Log-loss 1X2 | Brier 1X2 | Log-loss O/U 2,5 |
|---|---|---|---|
| APEX-BSM | 1,0262 | 0,6152 | 0,6952 |
| Skill actuel (legacy) | 1,0380 | 0,6235 | 0,7085 |
| Modèle simple de buts | 1,0753 | 0,6508 | **0,6914** |
| Marché démarginé | **1,0048** | **0,6013** | **0,6827** |

- **Face au skill actuel** : APEX-BSM est **meilleur** (Δ −0,0118, IC95 [−0,0187 ; −0,0051]).
- **Face au modèle simple** : APEX-BSM est **meilleur en 1X2** (Δ −0,049), mais **moins bon en Over/Under 2,5**. Le total de buts propre à chaque équipe n'apporte rien, et ajoute même du bruit.
- **Face au marché** : APEX-BSM est **moins bon** (Δ +0,0214, IC95 [+0,016 ; +0,027]). Il l'est dans 7 ligues sur 8 ; en Premier League, l'écart n'est pas significatif.
- **Calibration 1X2** : correcte entre 10 et 60 %. Le modèle est un peu sous-confiant sur les gros favoris.
- **Calibration Over 2,5** : les probabilités sont trop étalées. À 27 % annoncé, la fréquence observée est de 44 %.
- **Paris simulés** (règles fixées avant le test : EV ≥ 3 % contre la cote d'avant-match, un marché par match, 1 unité) :
  - 2 813 paris, taux de réussite 32 % ;
  - **rendement −14,1 % par unité** (IC95 [−19 % ; −9 %]), perte maximale cumulée −427 unités ;
  - CLV 1X2 : −5,6 %.

### Statut : **NON SUPÉRIEUR AU MARCHÉ**

Une EV calculée contre les cotes par ce modèle est présumée illusoire. Le module impose donc la surveillance ou l'abstention sur les marchés cotés. Les probabilités restent publiables comme estimation, mais pas comme signal de value.

## Variantes et écarts de procédure (transparence)

1. **Premier essai (E0 seule, validation 2024-25)** : l'optimum tombait en bordure de grille, alors la grille a été élargie. Cet essai a aussi affiché le test E0 2025-26. **Le test E0 2025-26 n'est donc pas totalement vierge** pour la conception de la grille.
2. **Premier run complet** (σ = 0,1 retenu, écart de 0,00002 avec σ = 0) : une règle de parcimonie (`MIN_GAIN = 0,0005`) a été ajoutée ensuite, conformément au principe « la complexité seulement si son apport est démontré ». Elle ne change presque rien au test : log-loss 1,0263 → 1,0262.
3. **K = 6 est encore en bordure de grille.** La grille n'a **pas** été ré-élargie : les résultats du test étaient déjà connus. Un élargissement sera évalué sur un test vierge.

## Pistes pour une v1.1 (à tester sur la saison 2026-27, jamais sur 2025-26)

- **Ancrage marché** : mélanger les probabilités du modèle et celles du marché, avec un poids estimé en validation. C'est la seule voie réaliste pour détecter un écart exploitable, puisque le modèle seul est moins bon que le marché.
- **O/U** : remplacer le total de buts propre à chaque équipe par une moyenne de ligue, ou rétrécir davantage.
- **xG** : utiliser une source horodatée, en remplacement ou en mélange estimé des buts. Jamais en surcouche.
