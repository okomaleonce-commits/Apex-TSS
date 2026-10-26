# agentmemory — mémoire persistante des agents (Apex-TSS)

[`rohitg00/agentmemory`](https://github.com/rohitg00/agentmemory) — couche de
mémoire persistante pour agents de code (Claude Code, Cursor, Codex, …).
Paquet **npm/npx**, pas pip. Node.js 20+ requis. Licence Apache-2.0.

Trois pièces : un **serveur MCP** (branché dans ce repo), un **worker** local
qui stocke/recherche la mémoire, et **17 skills** qui apprennent à l'agent quand
appeler la mémoire.

---

## 1. Serveur MCP — déjà branché (`.mcp.json`)

`.mcp.json` à la racine enregistre agentmemory comme serveur MCP projet. Il
lance `npx -y @agentmemory/mcp`, qui se connecte au worker REST sur
`localhost:3111`. Aucun secret commité : `AGENTMEMORY_SECRET` et l'URL sont lus
de l'environnement (`${VAR:-default}`). À la première session, Claude Code
demande d'approuver ce serveur projet (ou `/mcp`).

## 2. Worker — à démarrer (sinon le MCP n'a rien à interroger)

```bash
npx -y @agentmemory/agentmemory@latest        # démarre REST :3111, viewer :3113
npx -y @agentmemory/agentmemory@latest status # santé, nb de mémoires, flags
```

Le worker génère/lit son secret dans `~/.agentmemory/secret` ; le CLI et le
shim MCP le résolvent seuls. Pour le forcer dans l'environnement avant de lancer
Claude Code :

```bash
export AGENTMEMORY_SECRET="$(cat ~/.agentmemory/secret)"
```

> **Conteneur cloud (claude.ai/code) :** le worker et sa mémoire vivent dans un
> conteneur éphémère et disparaissent à la fin de session. Le hook SessionStart
> ci-dessous le relance à chaque session ; la mémoire accumulée n'est pas
> conservée d'une session cloud à l'autre. Pour une mémoire qui dure, lancer le
> worker sur une machine permanente.

## 3. Les 17 skills — à installer

```bash
npx skills add rohitg00/agentmemory -y
```

Installe les skills natifs pour que l'agent sache *quand* chercher/écrire en
mémoire. À relancer sur chaque machine (comme le worker, l'état est local).

## 4. Démarrage auto du worker (hook SessionStart)

Pour que chaque session cloud démarre le worker automatiquement, ajouter ce bloc
à `.claude/settings.json` (nécessite ton approbation — Claude Code protège
l'écriture de sa propre config) :

```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "curl -fsS http://localhost:3111/agentmemory/livez >/dev/null 2>&1 || (command -v npx >/dev/null 2>&1 && nohup npx -y @agentmemory/agentmemory@latest >>/tmp/agentmemory-worker.log 2>&1 & ) ; exit 0",
            "async": true,
            "timeout": 120,
            "statusMessage": "Starting agentmemory worker…"
          }
        ]
      }
    ]
  }
}
```

Non bloquant (`async`), idempotent (ne relance pas si `:3111` répond déjà),
n'échoue jamais la session (`exit 0`).

## Santé / dépannage

```bash
curl -fsS http://localhost:3111/agentmemory/livez     # {"status":"ok",...}
npx -y @agentmemory/agentmemory@latest doctor          # diagnostic + fixer
npx -y @agentmemory/agentmemory@latest remove --force  # désinstaller proprement
```

Viewer : http://localhost:3113 · REST : http://localhost:3111
