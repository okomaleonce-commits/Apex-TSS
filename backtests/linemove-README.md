# Le mouvement de cote est-il prévisible, et le modèle l'anticipe-t-il ?

`tools/apex_linemove.py` · test 2025-26 · 3 408 matchs · 8 championnats.
Proxy du mouvement : cote Pinnacle **avant-match → clôture** (football-data). Le mouvement
ouverture→avant-match n'est pas dans l'historique ; il ne se teste qu'en avant (`forward`).

## Q1 — la ligne se resserre en s'approchant du coup d'envoi
Log-loss avant-match 1,0027 → clôture **1,0014**. La clôture est un peu plus précise : la cote intègre de l'information jusqu'au bout. Effet réel mais faible (la « clôture » football-data est proche du coup d'envoi).

## Q2 — suivre le mouvement (« steam »)
Parier l'issue vers laquelle la ligne s'est déplacée, à la cote d'avant-match :

| Mouvement | Paris | Rendement | IC95 |
|---|---|---|---|
| ≥ 2 % | 1 148 | +3,0 % | [−4,5 % ; +11 %] |
| ≥ 5 % | 167 | +10,3 % | [−8,4 % ; +30 %] |
| **Fader** ≥ 2 % | 1 200 | **−13,7 %** | [−21 % ; −7 %] |

- **Suivre** le mouvement est positif mais **non significatif** (intervalles larges, peu de gros mouvements).
- **Fader** le mouvement perd nettement. C'est le résultat important : ça prouve que **le mouvement porte une vraie information** (aller contre est puni). « Suivre le steam » est donc directionnellement juste, sans qu'on puisse démontrer un profit sur cet échantillon.

## Q3 — le modèle anticipe-t-il le mouvement ? NON
- Corrélation entre (modèle − avant-match) et (clôture − avant-match) : **−0,015** ≈ 0.
- Parier là où le modèle est en désaccord avec l'avant-match : **−16 % de rendement, CLV −5,8 %, 17 % de CLV positif**.

Le désaccord du modèle ne prédit pas le mouvement — il va même légèrement à l'envers. Notre modèle « buts passés » n'est pas l'outil pour anticiper la ligne.

## Conclusion

- **L'information prévisible existe, mais elle est dans le mouvement du marché lui-même**, pas dans notre modèle. Le marché se corrige vers la vérité ; notre modèle ne voit pas cette correction à l'avance.
- **La seule piste vivante** : capter le mouvement **ouverture → avant-match** et suivre le steam tôt, puis mesurer le CLV. C'est plus tôt et plus large que le proxy avant-match→clôture testé ici, donc potentiellement plus exploitable.
- **Ce n'est backtestable qu'en avant.** `apex_linemove.py forward` lit les snapshots API-Football horodatés et calcule le mouvement ouverture→avant-match par match. Il faut **au moins deux relevés par match** (un tôt, un juste avant le coup d'envoi) : le passage quotidien de 08:53 + un second passage tardif les fournissent.

## Suite proposée
1. Programmer un **second passage quotidien tardif** (≈1 h avant les matchs du soir) pour capter le mouvement de la journée.
2. Laisser `forward` accumuler quelques centaines de matchs sur 2026-27.
3. Alors seulement, tester « suivre le steam ouverture→avant-match » sur données réelles, avec CLV.
