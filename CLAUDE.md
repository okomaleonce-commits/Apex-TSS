# Apex-TSS — consignes permanentes

## Analyses football : module BSM obligatoire

Toute analyse football, sans exception, applique le skill `apex-backtest-simulation`, outillé par `tools/apex_bsm.py`. Cela vaut pour un pronostic, un coupon, une recherche de value, un scan de journée ou un match isolé, que la chaîne d'agents soit utilisée ou non.

1. **Commencer par le bilan du backtest chronologique.** Il se trouve dans `backtests/<run_id>/REPORT.md`, avec le statut dans `backtests/latest_params.json`. S'il manque ou ne couvre pas la ligue, lancer `backtest` d'abord.
2. **Simuler chaque match avec `simulate`.** Indiquer le nombre de simulations exécutées et le statut du modèle. Les matchs hors périmètre passent par `--lh/--la` et sont marqués NON VALIDÉS. Pour les sélections, les λ viennent de `tools/apex_intl.py lambdas` (conversion Elo → λ backtestée, non comparée au marché).
3. **Déduire tous les marchés des mêmes simulations.** Pour les combinés d'un même match, utiliser la probabilité jointe, jamais un produit.
4. **Au maximum un marché officiel par match**, et seulement si toutes ces conditions sont réunies : cote vérifiée et horodatée, EV ≥ 3 %, EV stable en sensibilité, aucun veto.
5. **Enregistrer chaque prévision avant le coup d'envoi** (`--record`), puis la régler après le match avec `settle`. Le journal `ledger/` est append-only.
6. **Ne jamais inventer** un backtest, une cote historique, une statistique ou une simulation. Si une donnée manque, l'écrire.

Le hockey (`apex-hockey-team`) et le turf (`apex-turf-team`) ont leurs propres protocoles.

## Cellule de bruit marché & comportement : APEX-MI

La captation du **bruit informationnel et comportemental** du marché avant le coup d'envoi est une cellule **séparée** de la cellule statistique : le skill `apex-market-intel-team` (essaim `apex-mi-*` pour le marché, `apex-bi-*` pour le comportemental), outillé par `tools/apex_mi.py`.

- Elle **ne price pas** et **n'émet jamais un pari seule** : `bet_authority=false`. Ses sorties alimentent le moteur statistique APEX (brique DATA) pour la convergence finale. Règle d'intégration : `BEHAVIORAL seul → WATCH` ; `BEHAVIORAL + MARKET → CANDIDATE` ; `+ DATA → CONFIRMED`.
- Elle ne remplace **pas** le module BSM obligatoire ci-dessus : une décision de pari football passe toujours par le backtest + la simulation. APEX-MI est un apport de contexte marché, pas un raccourci.
- Mêmes règles anti-invention : hiérarchie des sources honnête (un déplacement Pinnacle ≠ un post Telegram), RLM non calculable sans % public, donnée absente écrite comme absente. Le moteur score, les agents observent.
- **Branchée sur APEX-WORM** : chaque passage `apex_worm.py scan` active automatiquement APEX-MI H-60 sur les matchs dont le coup d'envoi est dans l'heure (`worm-hook`, focus **UPSET + BLOWOUT**), croise les anomalies structurelles WORM avec le mouvement du marché — `UPSET_WATCH` (argent vers l'outsider) et `BLOWOUT_WATCH` (argent vers le favori dominant) — et l'email WORM joint la section « APEX-MI — bruit de marché H-60 (focus UPSET + BLOWOUT) ». Désactivable par `--no-mi`.

## Envoi des emails APEX (WORM et MI) : connecteur Gmail (MCP), pas SMTP — OBLIGATOIRE À CHAQUE PASSAGE

**Règle absolue : TOUT passage APEX se termine par l'envoi du digest par email. Un passage sans email envoyé est INCOMPLET.** Cela vaut pour tous les passages (WORM et MI), interactifs comme déclenchés. Le SMTP n'étant pas configuré, le canal est le connecteur Gmail (`mcp__Gmail__send_message`), appelé depuis la session (OAuth du connecteur, aucun secret manipulé).

- **APEX-MI** : `python3 tools/apex_mi.py finalize --run runs_mi/<run>` construit désormais TOUJOURS le digest (`email.html`/`email.txt`/`email.subject.txt`) et affiche une bannière « ENVOI EMAIL OBLIGATOIRE ». Enchaîner immédiatement avec `mcp__Gmail__send_message` : `to=["okoma.leonce@gmail.com"]`, `subject` (= `email.subject.txt`), `htmlBody` (= `email.html`), `body` (= `email.txt`).
- **APEX-WORM** : `scan --email` écrit `reports/worm/<jour>.email.html` ; envoyer ce HTML via `mcp__Gmail__send_message` (sujet produit par le scan). La Routine horaire `trig_01Kh82cbpvCSCtGFMB9WrQ3M` le fait déjà à chaque passage.
- Recharger l'outil via `ToolSearch` si le connecteur Gmail s'est déconnecté ; preuve d'envoi = `id`/`threadId` Gmail. Envoi 100 % autonome par un cron à session fraîche (sans modèle) = renseigner les secrets `WORM_SMTP_*`.



Les secrets `WORM_SMTP_*` ne sont pas configurés dans l'environnement : `tools/apex_worm.py scan --email` **génère** seulement le HTML (`reports/worm/<jour>.email.html`) et affiche « Email non envoyé : WORM_SMTP_* non configurés ». L'envoi réel se fait **en session interactive via le connecteur Gmail** (`mcp__Gmail__send_message`), depuis `okoma.leonce@gmail.com` (OAuth géré par le connecteur, aucun secret manipulé). Procédure : (1) `scan --money --email`, (2) `apex_worm.build_email_html("<jour>")` → `(sujet, html)`, (3) `mcp__Gmail__send_message` avec `to`, `subject`, `htmlBody` (recharger l'outil via `ToolSearch` si le connecteur s'est déconnecté), (4) preuve = `id`/`threadId` Gmail. Pour un envoi 100 % autonome par le cron (GitHub Actions ou Routine à session fraîche), il faudrait renseigner les secrets `WORM_SMTP_*`.

## Environnement

- Dépendances Python du module : `pip install numpy scipy`.
- API-Football : la clé est fournie par les « Identifiants API » de l environnement cloud (hôte `v3.football.api-sports.io`, en-tête `x-apisports-key`, injecté par le proxy) ou, à défaut, par la variable `API_FOOTBALL_KEY`. Jamais dans le code. Avant chaque analyse, lancer `python3 tools/apex_apifootball.py snapshot --date <J>` pour relever les cotes horodatées, les blessures et les compositions, puis `bsm-args --fixture <id>` pour obtenir la commande `simulate`.
- FootyStats (xG historiques) : clé dans la VARIABLE D'ENVIRONNEMENT `FOOTYSTATS_KEY` (FootyStats authentifie par `?key=`, donc pas un identifiant API en en-tête). Connecteur `tools/apex_footystats.py` (status, leagues, history → `data/footystats/`).
- Historique mis en cache dans `data/history/`, rapports de backtest dans `backtests/`, journal dans `ledger/`.
