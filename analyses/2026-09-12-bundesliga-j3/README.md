# Bundesliga J3 2026/27 — 12/09/2026 15:30 CEST

Chaîne APEX exécutée : `apex-s0-orchestrator` → `apex-engine-bundesliga v2.0` → `apex-preflight-gate v1.0`.

## Verdict : NO_BET sur les 5 matchs — publication INDICATIVE uniquement

Deux gates bloquantes, indépendantes l'une de l'autre :

| Gate | Source | Constat |
|---|---|---|
| **DCS < 68** | apex-engine-bundesliga v2.0 | DCS reconstitué 34–39/100. Aucune source xG disponible (Understat 2026/27 vide, FBref 403, WhoScored 403) = −38 pts de DCS structurels. |
| **G3 Maturité** | apex-preflight-gate v1.0 | 2 journées jouées par équipe → « NO_BET absolu, quel que soit l'edge affiché ». |

Le garde-fou inversé de la G3 se déclenche aussi : le modèle affiche des edges de +20 à +22 points
contre Pinnacle à 2.8–3.8 % de marge. Sur n=2, un tel écart est un symptôme de sur-ajustement,
pas une opportunité.

## Preuve d'instabilité — test de sensibilité au shrinkage

`W` = poids de l'ancrage 2025/26 face aux 2 matchs 2026/27. Paramètre non identifiable à n=2.

| Match | P(1) W=1.00 | P(1) W=0.85 | P(1) W=0.65 | Marché dévigorisé |
|---|---|---|---|---|
| Freiburg–Gladbach | 54.1 % | 66.0 % | 79.6 % | 57.1 % |
| Mainz–Frankfurt | 41.8 % | 56.5 % | 75.1 % | 54.0 % |
| Dortmund–Paderborn | 80.4 % | 79.5 % | 77.8 % | 76.2 % |
| Hoffenheim–Stuttgart | 40.5 % | 41.2 % | 41.4 % | 43.0 % |
| Augsburg–Leverkusen | 24.5 % | 32.8 % | 45.2 % | 25.4 % |

Le verdict bascule entièrement avec un paramètre arbitraire → aucun signal exploitable.
À W=1.00 (ancrage pur saison passée) le modèle recolle au marché sur 4 matchs sur 5.

## Données collectées

- **soccerstats.com/germany** : classement, home/away splits, buts par segment, H2H 10–14 matchs, corners.
- **Pinnacle (API guest, marge 2.8–4.8 %)** : 1X2, échelle AH complète, échelle totals 2.25→4.0, team totals.
- **sportsgambler / previews presse** : blessures et compos probables des 10 clubs.
- **Indisponible** : Understat 2026/27 (vide), FBref (403), WhoScored (403), odds-radar (auth).

## Reproduire

```
python3 pricing_dixon_coles.py     # Dixon-Coles rho=-0.08, avg_goals hors-Bayern 3.06, R14 WCH x0.93
```
