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

## Moteur unique : APEX-FUSION (un seul digest, un seul email)

Les quatre cellules (WORM radar, MI bruit marché/comportemental, PROTOCOL/BSM simulation calibrée, ORION arbitrage) sont fondues dans un **moteur unique** : `tools/apex_fusion.py`. Il produit **UN seul digest et UN seul email**, au lieu de quatre sorties séparées.

- `python3 tools/apex_fusion.py run --date <J>` : enchaîne (optionnellement `--scan` pour relancer WORM+MI, sinon lit le dernier snapshot), puis rend **par match un verdict ORION unique** (`ACCEPTER / REJETER / ATTENDRE / COLLECTER`) obtenu en fondant les couches présentes en voix **sourcées** via `tools/orion_consensus.py`. Le digest (`reports/fusion/<J>.email.html` + `.subject.txt` + `.txt`) porte la carte ORION en tête et réutilise tel quel le corps WORM/MI/SYNC/CHARACTER/bilan.
- **Couches et sources honnêtes** : `W`=structure WORM (classement), `M`=marché/MI, `C`=comportemental, `S`=FORECAST BSM. La Poisson-classement WORM n'est **jamais** étiquetée BSM ; une voix `S` n'existe que pour une ligue backtestée ET une simulation calibrée réellement présente (`data/fusion/bsm/<J>.json`). META fond les sources partagées, le désaccord pénalise la confiance, **< 3 voix indépendantes → COLLECTER**. WORM peut dire « JOUER » : ORION refuse tant qu'une confirmation indépendante manque.
- **Gel actif** : aucune mise réelle émise ; tout « ACCEPTER » est rétrogradé en ATTENDRE.
- **Email unique** : `run` affiche la bannière « ENVOI EMAIL OBLIGATOIRE » avec `subject` + `htmlBody = reports/fusion/<J>.email.html`. Enchaîner `mcp__Gmail__send_message` (`to=["okoma.leonce@gmail.com"]`). Ce digest de fusion **remplace** les envois WORM/MI séparés pour un passage interactif.

## Sources externes & voix sharp : APEX-SOURCES

`tools/apex_sources.py` agrège des sources externes pour renforcer la robustesse (voix indépendantes). **Câblé par défaut** : `football-data.co.uk` (cotes Pinnacle ouverture/clôture dé-viggées + O/U 2.5 — référence sharp et CLV, gratuit). **À configurer côté client/clés** (jamais simulé sinon) : Infersports (MCP), SSB/PropProfessor (MCP), SharpAPI/`SHARPAPI_KEY`, odds-api.io/`ODDSAPI_IO_KEY`, TheStatsAPI/`THESTATSAPI_KEY`. Procédure complète dans `SOURCES.md`.

- Ces sources ajoutent à ORION une voix **`marche_sharp`** (couche **K** d'`apex_fusion`), indépendante du classement WORM et du bruit MI — c'est elle qui peut porter le nombre de voix indépendantes à ≥ 3 et permettre un vrai arbitrage (hors COLLECTER). **Anti-invention absolu** : ligue non couverte ou source absente ⇒ voix écrite absente, jamais fabriquée.
- **Ajouter des sources ne lève pas le gel** : plus de données ≠ bord. Le CLV (`tools/apex_clv.py`) reste le seul juge ; le dé-vigging est une estimation de la proba implicite, pas une vérité.

## Favoris raisonnables, SANS cote : APEX-FAVORITES

Cellule **descriptive** (`tools/apex_favorites.py`, agents `apex-favorites-conductor` / `apex-favorites-reader`), miroir d'APEX-CHARACTER : elle liste, par journée, les favoris que le **modèle** soutient nettement, **indépendamment du marché** (aucune cote lue, aucun EV calculé). Source = classement Poisson-λ WORM (`model_prob_1x2`, `lambdas`, `data_quality`, `signal_stable`, flags `upset`/`blowout`), **jamais étiqueté BSM** ; `+BSM` signale seulement une ligue backtestée qui corrobore.

- **Favori raisonnable ≠ pari.** Sans cote, pas de value : la sortie est une liste **SURVEILLER — NON JOUABLE**. La cellule ne price pas, n'émet aucun pari (gel), alimente la brique DATA de la convergence 3-voix, ne la remplace pas.
- **Seuils calibrés sur les coupons 1xBet partagés** : un favori modèle à **82 %** (Augsburg–Bayern, Bayern ext.) a perdu ⇒ seuil **exigeant**. `strict` = p ≥ 75 % + marge ≥ 40 pts + `data_quality` ≥ 65 + signal stable + hors profil volatil ; sous-classe **DOMINANT** ≥ 85 %/60 pts. Profils `modere`/`large` disponibles. La variance n'est pas supprimable : un FAVORI_NET gagne *souvent*, jamais *toujours*, et un combiné de favoris reste à espérance négative.
- `scan --date <J> [--profile strict] [--json] [--include-live]` ; `calibrate --coupon <legs.json>` score des jambes de coupon par bande de probabilité (anti-invention : match sans proba modèle → non listé).

## Moteur de backtest de règles : APEX-STRATEGY

`tools/apex_strategy.py` transforme une **règle de pari précise** en **verdict chiffré** sur l'historique réel (football-data en cache, ~20 000 matchs, cotes Pinnacle). C'est le moteur qui remplace les scripts jetables : « un jour ça va prendre » → un nombre.

- **Règle** = CLI ou `--spec <fichier.json>` : bandes de cotes (`fav_min/max`, `opp_min/max`, `max_diff`, `draw_min/max`), camp (`--pick fav|dog|home|away|draw`), marché (`--market 1X2|DNB`), ligues (`--divs`), saisons (`--seasons`), filtre d'équipes (`--only-teams/--exclude-teams <fichier>`).
- **Sortie** : échantillon, taux de victoire, cote moyenne, seuil de rentabilité, **ROI** à mise plate, **stabilité par saison** (`--by-season`), verdict EDGE/PAS D'EDGE.
- **Garde-fous anti-illusion** : ROI sur cotes **Pinnacle** (marge ~2,5 %) — sur un book soft (betclic/1xBet, 6–10 %) retrancher 4–8 pts, affiché ; échantillon < 200 → marqué **BRUIT** ; le moteur **ne price pas, n'émet aucun pari** (gel), **n'invente rien**. Un edge n'est retenu que confirmé par CLV + gros échantillon + book réel.
- Exemple validé : règle « favori 2,00–2,35, adversaire 2,35–3,20, DNB » → 1 484 paris, 44,1 % de réussite, ROI +1,0 % (Pinnacle) mais instable par saison (−3,3 % à +3,5 %) ⇒ **pas d'edge**.
- Exemple validé (audit du coupon Xander, 4 nuls, 09/10/2026) : règle « camp = nul, cote du nul 3,75–4,33 » → **4 632 paris, 24,1 % de réussite (seuil 25,2 %), ROI −4,3 % (Pinnacle)**, instable par saison ⇒ **pas d'edge**. Parier le nul à ~4.0 est structurellement perdant ; empiler 4 nuls (−EV chacun) donne une EV de combiné ≈ −16 % (book sharp) à −30/−35 % (book soft) — un ticket gagnant à 0,36 % reste une décision négative.

### Leçon mesurée — le « contexte-nul » n'est pas un edge, c'est un veto

Audit causal du même coupon (3 jambes/4 = « un favori modéré ne gagne pas »). Tous les angles de cette situation mesurés sur l'historique réel (favori 1,70–2,15, 7 227 matchs, Pinnacle) : **NUL −5,3 % · FAVORI −2,9 % · OUTSIDER (fade) −5,6 % · FAVORI en DNB −1,3 %**. **Aucun angle ne bat la marge : le marché price ces situations efficacement.** Le seul sous-cas du nul qui frôle l'équilibre est le **match sans favori net** (écart de cote faible) : −1,0 % sur 2 449 paris, neutre sur book sharp, négatif sur book soft.

Trois conséquences durables :

1. **Audit d'une conjonction : expliquer les jambes, jamais le combiné.** Chaque nul est un événement ~24 % (fréquent) ; un 4-fold de nuls à 0,36 % n'a pas d'autre « cause » que la variance. Coller un récit à la conjonction est le biais de survie.
2. **« Le favori a calé » (ex post) ≠ « le favori était surcoté » (ex ante).** Fader le favori est encore plus perdant que le suivre (−5,6 %). Un résultat surprenant n'implique pas un prix inefficace.
3. **Si un lecteur « contexte-nul » est ajouté** (frère des modulateurs disrupteur/forteresse : il relèverait la proba lue du nul quand match équilibré + deux blocs défensifs/peu prolifiques + enjeu faible + rivalité + rythme perturbé), **garde-fou dur** : comme le meilleur contexte mesuré reste ≤ seuil de rentabilité, il ne **déclenche jamais** un pari sur le nul — son seul usage légitime est de **retirer ou rétrograder un pari sur un favori fragile** (un veto de prudence, pas un déclencheur). La seule zone encore à backtester avant toute idée d'edge : le match **sans favori net**, plutôt en **DNB / lignes asiatiques** et par ligue — à prouver, jamais à affirmer.

## Modulateurs de contexte en coupe (UCL/UEL/UECL) : disrupteur contextuel & forteresse

En coupe d'Europe, l'ancrage au seul favori marché rate les deux vecteurs d'upset les plus répétables. Ces deux règles sont des **modulateurs de lecture**, pas une autorité de pari : elles **n'émettent aucun pari seules** (gel), ne remplacent ni le BSM ni la convergence 3 voix, et ne valent que sur **signal observable** (anti-invention : signal absent = écrit absent). Elles alimentent les briques existantes — le **disrupteur** entre par la cellule comportementale **APEX-MI** (`apex-bi-*`), la **forteresse** par la structure **APEX-WORM** / le moteur de ligue domestique de l'équipe à domicile.

1. **Disrupteur contextuel (→ APEX-MI).** Quand une équipe — surtout un favori marché — aborde le match avec une **perturbation extra-sportive documentée et datée** (sanction/condamnation, crise de gouvernance, changement d'entraîneur récent, conflit de vestiaire public, enjeu asymétrique type dead rubber), **baisser la confiance** sur son camp et **relever la probabilité de l'issue adverse**. Jamais un diagnostic mental ni une généralisation : uniquement un **proxy observable** daté, pondéré comme les signaux `apex-bi-*`. Cas d'école : Man City–PSG (14/10/2026), 1er match à domicile de City après sa condamnation pour irrégularités financières → PSG l'emporte 4-2 ; le favori marché était fragilisé par un signal que la lecture statistique seule ignorait.

2. **Forteresse à domicile en coupe (→ APEX-WORM / moteur domestique).** Pour l'équipe qui **reçoit**, intégrer son **avantage structurel à domicile mesuré par son propre moteur de ligue** (ex. Bodø/Glimt `HOME_ADV 1,22`, pelouse synthétique, déplacement arctique — `apex-engine-eliteserien`) **au lieu de s'ancrer au seul favori marché** venu de l'extérieur. Un club modeste dans une **forteresse documentée** face à un cador en déplacement est un **vecteur d'upset structurel**, pas une surprise aléatoire. Cas d'école : Bodø/Glimt 1-0 Dortmund (14/10/2026), déjà accroché 2-2 fin 2025 à domicile.

Portée : ces modulateurs **ajustent la proba lue**, le palier de décision restant soumis au gel, à l'EV ≥ 3 % horodatée et à la convergence. L'UCL/UEL est couverte par `apex-engine-uefa` mais **sans historique backtesté** : tant que la calibration n'existe pas, toute proba de coupe reste **INDICATIVE / NON VALIDÉE**, jamais étiquetée BSM.

## Audit d'un coupon partagé (captures de parieurs) : protocole en 5 temps

Un coupon reçu en capture (BetsWall, 1xBet, etc.) s'audite **toujours dans cet ordre** — c'est un protocole reproductible, pas une lecture au jugé. Il ne lève pas le module BSM : il l'applique jambe par jambe.

1. **Identifier les vrais matchs de la période d'édition AVANT de juger.** La date affichée est celle de l'**édition**, pas forcément du coup d'envoi : vérifier chaque fixture (date réelle, compétition, round, résultat) sur source externe recoupée. *Leçon du coupon Hacker2 (édité 12/10/2026) :* des matchs bien réels joués le 14/10 (UCL J2) avaient d'abord été qualifiés à tort de « fabriqués » faute d'avoir cherché les fixtures. **On ne déclare jamais un match faux sans avoir cherché le fixture réel.**
2. **Classer chaque jambe par périmètre.** Pour chaque match : dans les **21 moteurs de ligue** ? dans **football-data** (APEX-STRATEGY) ? Une **2ᵉ division**, une ligue mineure ou une coupe non backtestée sort **HORS_PERIMETRE** — ce n'est pas une invention, c'est une **absence de moteur**. *Leçon du coupon Xander (édité 09/10/2026, 4 jambes en 2ᵉˢ divisions KAZ/SUI + NIFL) :* **0 jambe sur 4 chiffrable**, verdict ORION **COLLECTER** partout, aucune voix DATA calibrée disponible.
3. **Appliquer le BSM là où c'est possible, NON VALIDÉ ailleurs.** Dans le périmètre : backtest + `simulate` calibré. Hors périmètre : `--lh/--la` manuels marqués NON VALIDÉS, ou pas de simulation du tout — jamais de pricing présenté comme calibré quand il ne l'est pas.
4. **Lire la côte totale comme une probabilité, pas comme un gain.** Côte totale 276 ⇒ proba implicite ≈ 0,36 %. **Gagner un longshot une fois est de la variance / survie, jamais une preuve d'edge** : sur 100 coupons de ce profil, 99+ perdent la mise entière. Un combiné de favoris est déjà à espérance négative (cf. APEX-FAVORITES) ; un combiné d'outsiders en ligues non calibrées l'est davantage. Le résultat affiché « gagné » ne change rien au verdict pré-match.
5. **Picks illisibles = non-invention stricte.** Si la capture masque les sélections (marché/camp de chaque jambe), **ne pas les supposer** pour faire « coller » l'analyse au résultat affiché. On audite ce qu'on voit, on écrit *absent* ce qu'on ne voit pas.

**Les cotes se tirent des connecteurs, jamais estimées ni demandées à l'utilisateur.** Pour un coupon récent ou à venir, relever la cote réelle de chaque jambe via **INFERSPORTS** (`get_opening_line` ouverture+actuelle, `get_match_odds`, books sharp sbobet/pinnacle) et **SharpAPI** (`get_events` → `get_event_odds`, Pinnacle + 17 books) avant tout calcul d'EV. *Leçon du coupon johniliya (14/10/2026) :* faute d'avoir tiré la vraie cote, Southampton avait été **estimé à 2,10 → EV −5 %** ; la **vraie cote 2,70 donne EV +22,1 %** — une cote estimée a **inversé le signe de l'EV**. Corollaire de vérification : un score servi par un connecteur dont le cache est daté **avant** le coup d'envoi est un placeholder (ex. UCL affichée « 0-0 » le 02/10 pour un match du 13/10) — le recouper, ne jamais le présenter comme résultat.

**Résultat ≠ qualité de décision (EV).** Un audit post-match juge chaque jambe par son **EV d'avant match**, pas par le vert. *Leçon johniliya (gagné 6/6) :* sur les 2 jambes calibrées, Southampton @2,70 était un **bon pari** (EV +22 %) et Portsmouth @2,40 un **mauvais pari gagné quand même** (EV −16,6 %, un événement à 35 % qui tombe). Un combiné de favoris courts n'est pas plus un edge qu'un combiné de nuls : 6 marges de book empilées ⇒ EV combinée franchement négative malgré un 6/6. Gagner reste de la variance.

Gel maintenu : un audit de coupon ne produit **jamais** une mise, seulement une lecture honnête de ce qu'APEX peut — ou ne peut pas — chiffrer. Le passage se termine par l'envoi du digest (connecteur Gmail), comme tout passage APEX.

## Environnement

- Dépendances Python du module : `pip install numpy scipy`.
- API-Football : la clé est fournie par les « Identifiants API » de l environnement cloud (hôte `v3.football.api-sports.io`, en-tête `x-apisports-key`, injecté par le proxy) ou, à défaut, par la variable `API_FOOTBALL_KEY`. Jamais dans le code. Avant chaque analyse, lancer `python3 tools/apex_apifootball.py snapshot --date <J>` pour relever les cotes horodatées, les blessures et les compositions, puis `bsm-args --fixture <id>` pour obtenir la commande `simulate`.
- FootyStats (xG historiques) : clé dans la VARIABLE D'ENVIRONNEMENT `FOOTYSTATS_KEY` (FootyStats authentifie par `?key=`, donc pas un identifiant API en en-tête). Connecteur `tools/apex_footystats.py` (status, leagues, history → `data/footystats/`).
- Historique mis en cache dans `data/history/`, rapports de backtest dans `backtests/`, journal dans `ledger/`.
