# Token Burn

Server-side usage monitor for:

- Claude usage limits via `https://claude.ai/api/organizations/<org-id>/usage`
- Codex usage limits via `https://chatgpt.com/backend-api/wham/usage`

The app polls those endpoints on a schedule, deduplicates unchanged results, stores snapshots in SQLite, and exposes a dashboard with graphs and date filters.

## How it works

1. Paste the authenticated `Cookie` request header for each provider into the settings page.
2. The server stores those secrets in SQLite. If `APP_ENCRYPTION_KEY` is set, they are encrypted with Fernet first.
3. A background poller checks the usage JSON endpoints on a fixed interval.
4. Snapshots are only written when the normalized response changes, plus periodic heartbeat snapshots so graphs remain continuous.

## Run with Docker Compose

```bash
docker compose up --build
```

The app listens on port `8000` by default through [`compose.yml`](/Users/radu/Developer/token-burn/compose.yml).

### Useful environment variables

- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `APP_ENCRYPTION_KEY`
- `POLL_INTERVAL_SECONDS`
- `HEARTBEAT_INTERVAL_SECONDS`
- `TOKEN_BURN_PORT`

Generate an encryption key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Initial setup

1. Start the app and open `http://localhost:8000/settings`.
2. In browser devtools on Claude, copy the `Cookie` header from the `GET /api/organizations/<org-id>/usage` request.
3. In browser devtools on ChatGPT, copy the `Cookie` header from the `GET /backend-api/wham/usage` request.
4. Paste each cookie into the matching provider form, save, and run a manual poll.

The app does not use Playwright or browser automation. It only replays the saved `Cookie` header against the JSON usage endpoints.
