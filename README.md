# Token Burn Tracker

Self-hosted usage monitor for Claude and ChatGPT/Codex API rate limits. Polls authenticated usage endpoints, stores snapshots in SQLite, and shows a live dashboard with progress bars and historical charts.

## Features

- **Live dashboard** with Claude-style usage bars grouped by session and weekly windows
- **Historical charts** with per-series toggles and period filters (auto-refreshes every 60s)
- **Custom metric labels** to rename default metric names in settings
- **Curl import** for both providers - paste the full curl command from browser devtools
- **Save & Test** flow with instant pass/fail feedback per provider
- **Deduplication** - only writes snapshots when usage actually changes, plus periodic heartbeats
- **Encrypted secret storage** with Fernet (optional)
- **Docker-ready** with a single `docker compose up`

## Quick start

```bash
docker compose up --build
```

Open `http://localhost:8574/settings` (default credentials: `admin` / `change-me`).

### Setup each provider

**Claude:**
1. Go to `https://claude.ai/settings/usage`
2. Open DevTools (F12) → Network tab
3. Click the refresh button next to "Last updated"
4. Right-click the `usage` request → **Copy as cURL**
5. Paste into the Claude section in settings → **Save & Test**

**Codex (ChatGPT):**
1. Go to `https://chatgpt.com/codex/settings/usage`
2. Open DevTools (F12) → Network tab
3. Find the `usage` request (refresh if needed)
4. Right-click → **Copy as cURL**
5. Paste into the Codex section in settings → **Save & Test**

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `ADMIN_USERNAME` | `admin` | HTTP Basic Auth username |
| `ADMIN_PASSWORD` | `change-me` | HTTP Basic Auth password (unset = auth disabled) |
| `APP_ENCRYPTION_KEY` | _(none)_ | Fernet key for encrypting stored secrets |
| `POLL_INTERVAL_SECONDS` | `60` | How often to poll usage endpoints |
| `HEARTBEAT_INTERVAL_SECONDS` | `3600` | Max interval between stored snapshots |
| `TOKEN_BURN_PORT` | `8574` | External port (Docker Compose) |
| `TZ` | `UTC` | Timezone for the container |

Generate an encryption key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## How it works

1. The server stores cookies and auth tokens in SQLite (encrypted if `APP_ENCRYPTION_KEY` is set)
2. A background poller checks the usage JSON endpoints on a fixed interval
3. Snapshots are only written when the normalized response changes, plus periodic heartbeats so charts stay continuous
4. The dashboard auto-refreshes every 60 seconds
5. No browser automation - it only replays saved cookies/tokens against JSON API endpoints

## Development

```bash
pip install -r requirements.txt
uvicorn token_burn.web:app --reload
```

## License

MIT
