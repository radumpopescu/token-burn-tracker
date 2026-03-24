# Token Burn Tracker

Self-hosted usage monitor for Claude and ChatGPT/Codex API rate limits. Polls authenticated usage endpoints, stores snapshots in SQLite, and shows a live dashboard with progress bars and historical charts.

## Features

- **Live dashboard** with Claude-style usage bars grouped by session and weekly windows
- **Historical charts** with per-series toggles and period filters from `1h` to `All`
- **Live refresh status** showing the latest successful update time and the next dashboard refresh countdown
- **Custom metric labels** to rename default metric names in settings
- **Curl import** for both providers - paste the full curl command from browser devtools
- **Save & Test** flow with instant pass/fail feedback per provider
- **Deduplication** - only writes snapshots when usage actually changes, plus periodic heartbeats
- **Lean storage** - raw provider payloads stay out of SQLite and rotate in one-day NDJSON logs under `data/raw-payloads/`
- **Encrypted secret storage** with Fernet (optional)
- **Docker-ready** with a single `docker compose up`

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Open `http://localhost:8574/settings`.

Admin auth is optional. If `ADMIN_PASSWORD` is blank or omitted in `.env`, the settings page is not password protected. If you set `ADMIN_PASSWORD`, the default username is `admin` unless you also set `ADMIN_USERNAME`.

Docker Compose reads configuration from `.env` and passes it into the container.

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
| `ADMIN_USERNAME` | `admin` | HTTP Basic Auth username when admin auth is enabled |
| `ADMIN_PASSWORD` | _(blank)_ | HTTP Basic Auth password; blank or unset disables admin auth |
| `APP_ENCRYPTION_KEY` | _(none)_ | Fernet key for encrypting stored secrets |
| `TOKEN_BURN_PORT` | `8574` | External port (Docker Compose) |
| `TZ` | `UTC` | Timezone for the container |

Start by copying `.env.example` to `.env` and adjusting the values you need.

Polling defaults are stored in the database when it is first created: `60` seconds for polling and `3600` seconds for heartbeat snapshots. After that, change them from the Settings page.

Generate an encryption key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## How it works

1. The server stores cookies and auth tokens in SQLite (encrypted if `APP_ENCRYPTION_KEY` is set)
2. A background poller checks the usage JSON endpoints on a fixed interval
3. Snapshot events are only written when the canonical metric state changes, plus periodic heartbeats so charts stay continuous
4. Raw provider payloads are written to short-lived NDJSON files in `data/raw-payloads/` and pruned after one day instead of inflating the SQLite database
5. The dashboard auto-refreshes using the configured poll interval and shows the next refresh countdown
6. No browser automation - it only replays saved cookies/tokens against JSON API endpoints

## Development

```bash
pip install -r requirements.txt
uvicorn token_burn.web:app --reload
```

## License

MIT
