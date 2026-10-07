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

## Courses hippiques : trois cellules séparées, dont deux nouvelles

Une course est un **classement de N partants**, pas un score entre deux équipes : aucun skill `apex-engine-*` ni moteur football ne doit être chargé. Trois cellules, qui ne se mélangent pas.

- `apex-turf-team` (agents `apex-turf-*`, outil `tools/apex_turf_lead.py`) — cellule **statistique**. La seule qui price.
- `apex-turf-worm-scanner` (agents `apex-turf-worm-*`, outil `tools/apex_turf_worm.py`) — scanner **d'anomalies** horaire, miroir d'APEX-WORM. Fenêtre APEX 08:00→07:59, snapshots `data/turf_worm/snapshots/<jour>.jsonl` append-only, comparaison au passage précédent. Cron `.github/workflows/apex-turf-worm.yml`.
- `apex-turf-market-intel-team` (agents `apex-tmi-*` marché, `apex-tbi-*` comportemental, outil `tools/apex_turf_mi.py`) — cellule de **bruit**, miroir d'APEX-MI. `bet_authority=false`, `requires_statistical_convergence=true` ; règle d'intégration identique (`BEHAVIORAL seul → WATCH` ; `+ MARKET → CANDIDATE` ; `+ DATA → CONFIRMED`), la brique DATA venant d'`apex-turf-team`. Le maximum atteignable sans elle est donc `CANDIDATE`.

### Quatre écarts imposés par le pari mutuel, à ne pas « corriger »

1. **Le PMU n'a qu'une cote.** `SHARP_MOVE`, `STEAM_MOVE`, `RLM`, `BOOKMAKER_DIVERGENCE` et `LIQUIDITY_SPIKE` sortent `UNAVAILABLE_STRUCTUREL` et le moteur **refuse** de les enregistrer. Ce qui les remplace est propre au turf et publié par la source officielle, donc `OBSERVED` : `NON_PARTANT`, `DRIVER_CHANGE`, `DEFERRE_CHANGE`. L'essaim turf compte 13 agents et non 22 : les agents des familles disparues n'auraient rien à observer.
2. **Palier maximal `SURVEILLER`, jamais `JOUER`.** Les deux gates de pari turf sont fermées par le backtest : trot ROI −4,58 % sur 118 paris IC95 [−44,04 % ; +42,06 %], obstacle −89,05 % sur 21.
3. **Le plat sort `HORS_PERIMETRE`.** Aucun moteur calibré, et les coefficients ne sont pas transposables : `cf` vaut 0,0 en trot contre 0,4 en obstacle, la règle du top 3 change de camp. Hors discipline le résultat n'est pas moins précis, il est de signe faux. Même interdiction pour les quantiles de `tools/params/turf_worm_quantiles.json`.
4. **Les scores sont des percentiles empiriques**, pas des formules — mesurés sur 14 861 courses de trot et 2 952 d'obstacle. Une première version en formule linéaire notait 100/100 presque partout, ce qui ne porte aucune information. Et le contexte structurel (`non_terminaison`) ne classe **pas** la course : constant à discipline et champ donnés, il mettait tout l'attelé à 51/100.

### Périmètre LONACI — les courses réellement jouables en Côte d'Ivoire

`tools/apex_turf_lonaci.py` restreint le scan au **programme officiel PMU LONACI**, et non à tout le programme français. `apex_turf_worm.py scan --lonaci` applique le périmètre ; **sans périmètre du jour, il refuse de scanner** plutôt que de retomber silencieusement sur les 57 courses françaises.

**Le fait qui rend la restriction simple :** LONACI emploie **les mêmes codes `R#C#`** que le PMU français. Vérifié sur les 30 courses du 02/10/2026 et les 24 du 07/10 : chaque code tombe sur le bon hippodrome et le bon nom de course. Le périmètre est donc une liste de codes.

Trois conséquences mesurées :

1. **L'heure n'est pas une clé de validation.** Sur 30 courses : 9 heures identiques, 13 écarts de 1 à 5 min, aucun au-delà. On valide sur le **nom de la course**, tolérance d'heure 6 min. Un nom discordant est signalé comme périmètre peut-être périmé.
2. **La Nationale 3 est marocaine et absente de l'API française** (Anfa, Khemisset). Ni partant, ni cote : `ABSENT_SOURCE`. Ce n'est pas un refus de périmètre, c'est une absence de données.
3. **Le plat domine.** Au 07/10 : 24 courses LONACI → 6 trot attelé, 2 indicatif (monté), 9 plat refusées, 7 marocaines. **8 sur 24 entrent dans le périmètre des moteurs.** C'est la lecture la plus utile de ce module : les deux tiers du programme LONACI sont hors de ce que les moteurs peuvent chiffrer.

**Question ouverte, non tranchée — de qui sont les cotes ?** LONACI sert ses propres rapports par sa propre passerelle (`api.lonacionline.flexbet-software.com`, injoignable depuis le réseau de ces sessions : le tunnel s'ouvre, le serveur coupe). Rien ne prouve que sa masse d'enjeux soit celle du PMU français. Or les moteurs utilisent la cote **française** comme offset de marché. Le champ `cotes_origine` vaut `PMU_FRANCE` et `avertissement_masse` le dit, dans le périmètre, le rapport et l'email. **Ne jamais présenter une analyse LONACI comme fondée sur les cotes LONACI** tant que la comparaison n'a pas été faite — relever, sur une vingtaine de courses, le rapport LONACI et le rapport français du même cheval.

**Source du programme** : `https://pmu.lonacionline.ci/mobile/`, page publique sans login. Application Angular : **un `curl` ne rend rien**, le texte doit venir d'un moteur de rendu. Le domaine `pmu.lonaci.ci` que l'on cite parfois **ne résout pas en DNS**. Aucun appel aux points d'entrée de pari ou de compte de la passerelle : lecture du programme uniquement.

### Pont WORM → MI, trois axes

```
OUTSIDER_WATCH    = 0,55 · outsider structurel + 0,45 · confirmation (l'outsider se raccourcit)
FAVORI_WATCH      = 0,55 · favori dominant     + 0,45 · confirmation (le favori se raccourcit)
NON_PARTANT_WATCH = 0,60 · retraits tardifs    + 0,40 · recomposition du marché
```

Les deux premiers sont les miroirs exacts d'`UPSET_WATCH` et `BLOWOUT_WATCH`. Le troisième n'a aucun équivalent football : un retrait à H-30 redistribue *tout* l'argent de la course. Tri par le maximum des trois. Seul un **raccourcissement** compte comme confirmation — une dérive est une infirmation (`*_FADING`). Fenêtre H-30 et non H-60 : en pari mutuel l'argent décisif arrive dans le dernier quart d'heure.

### Email

**Voie autonome (cron, Routine à session fraîche) :** `apex_turf_worm.py` envoie seul par SMTP dès que l'environnement porte `WORM_SMTP_HOST`, `WORM_SMTP_USER`, `WORM_SMTP_PASS`, `WORM_EMAIL_TO` (+ `WORM_SMTP_PORT` défaut 587, `WORM_EMAIL_FROM` défaut l'utilisateur), STARTTLS obligatoire. C'est la **seule** voie quand il n'y a pas de connecteur. **Mais mesuré le 07/10/2026 : les ports 587, 465 et 25 expirent tous depuis un conteneur claude.ai** — la sortie passe par un proxy HTTPS, pas par du TCP brut. Cette voie ne vaut donc que sur un runner **GitHub Actions** (`.github/workflows/apex-turf-worm.yml`, secrets `WORM_SMTP_*`), où la sortie SMTP est ouverte. En session claude.ai, c'est le connecteur Gmail qui reste la voie fiable.

**Voie entierement autonome** : `.github/workflows/apex-turf-worm.yml` fait tout seul le rendu du programme LONACI (`tools/apex_turf_lonaci_render.py`, passerelle puis Playwright), le perimetre, le scan `--lonaci`, le pont MI et l'envoi SMTP — sans connecteur ni modele. C'est le seul endroit ou la chaine tourne seule : sur un runner GitHub il n'y a ni blocage SMTP ni proxy MITM. Secrets `WORM_SMTP_*` dans `Settings > Secrets and variables > Actions`. L'etape de rendu n'est pas eprouvable depuis un conteneur claude.ai : elle le sera au premier passage reel, et echoue en nommant lequel des deux chemins a cede.

La règle ci-dessous s'applique aussi au turf. `apex_turf_worm.py scan` construit le digest en fin de passage (`reports/turf_worm/<jour>.email.html`, + `build_email_html(day)` importable) ; `apex_turf_mi.py finalize` construit toujours `email.html` / `email.txt` / `email.subject.txt` dans le run. Aucun des deux n'envoie : enchaîner avec `mcp__Gmail__send_message`.

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
