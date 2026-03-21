"""FastAPI app exposing the dashboard, settings UI, and polling API."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .crypto import SecretBox
from .db import DB_SENTINEL, Database
from .providers import PROVIDER_SPECS, provider_choices
from .security import admin_auth_enabled, require_admin
from .service import UsageMonitorService

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "token_burn.db"
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database(DB_PATH)
    db.init_db(
        poll_interval_seconds=int(os.environ.get("POLL_INTERVAL_SECONDS", "300")),
        heartbeat_interval_seconds=int(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "3600")),
    )
    secret_box = SecretBox(os.environ.get("APP_ENCRYPTION_KEY"))
    monitor = UsageMonitorService(db, secret_box)
    app.state.db = db
    app.state.secret_box = secret_box
    app.state.monitor = monitor
    app.state.provider_specs = PROVIDER_SPECS
    await monitor.start()
    yield
    await monitor.stop()


app = FastAPI(title="Token Burn", lifespan=lifespan)


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_monitor(request: Request) -> UsageMonitorService:
    return request.app.state.monitor


def get_secret_box(request: Request) -> SecretBox:
    return request.app.state.secret_box


@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    provider: str = "all",
    period: str = "7d",
    start: str | None = None,
    end: str | None = None,
    db: Database = Depends(get_db),
):
    latest = db.latest_snapshots_by_provider()
    states = db.get_provider_states()
    filters = _resolve_range(period=period, start=start, end=end)
    settings = db.get_app_settings()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "provider_choices": provider_choices(),
            "latest": latest,
            "states": states,
            "filters": {
                "provider": provider,
                "period": period,
                "start": filters["start_input"],
                "end": filters["end_input"],
            },
            "poll_interval_seconds": settings.get("poll_interval_seconds", "300"),
            "heartbeat_interval_seconds": settings.get("heartbeat_interval_seconds", "3600"),
        },
    )


@app.get("/api/history")
async def api_history(
    provider: str = "all",
    period: str = "7d",
    start: str | None = None,
    end: str | None = None,
    db: Database = Depends(get_db),
) -> JSONResponse:
    filters = _resolve_range(period=period, start=start, end=end)
    payload = {
        "filters": filters,
        "latest": db.latest_snapshots_by_provider(),
        "states": db.get_provider_states(),
        "series": db.list_metric_series(
            provider=provider,
            start_at=filters["start_at"],
            end_at=filters["end_at"],
        ),
        "change_counts": db.list_change_counts(
            provider=provider,
            start_at=filters["start_at"],
            end_at=filters["end_at"],
        ),
        "snapshots": db.list_snapshots(
            provider=provider,
            start_at=filters["start_at"],
            end_at=filters["end_at"],
            limit=250,
        ),
    }
    return JSONResponse(payload)


@app.get("/settings", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def settings_page(request: Request, db: Database = Depends(get_db)):
    configs = {config.provider: config for config in db.list_provider_configs()}
    states = db.get_provider_states()
    settings = db.get_app_settings()
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "provider_specs": provider_choices(),
            "configs": configs,
            "states": states,
            "settings": settings,
            "notice": request.query_params.get("notice"),
            "admin_auth_enabled": admin_auth_enabled(),
            "encryption_enabled": request.app.state.secret_box.enabled,
        },
    )


@app.post("/settings/provider/{provider}", dependencies=[Depends(require_admin)])
async def update_provider_settings(
    provider: str,
    request: Request,
    enabled: Annotated[str | None, Form()] = None,
    usage_url: Annotated[str, Form()] = "",
    headers_json: Annotated[str, Form()] = "{}",
    notes: Annotated[str, Form()] = "",
    secret_input: Annotated[str, Form()] = "",
    clear_secret: Annotated[str | None, Form()] = None,
    db: Database = Depends(get_db),
    secret_box: SecretBox = Depends(get_secret_box),
):
    if provider not in PROVIDER_SPECS:
        return RedirectResponse(url="/settings?notice=unknown-provider", status_code=303)

    try:
        header_payload = headers_json.strip() or "{}"
        parsed_headers = json.loads(header_payload)
        if not isinstance(parsed_headers, dict):
            raise ValueError
    except ValueError:
        return RedirectResponse(url="/settings?notice=invalid-headers-json", status_code=303)

    sealed_secret: str | object = DB_SENTINEL
    if clear_secret:
        sealed_secret = None
    elif secret_input.strip():
        sealed_secret = secret_box.seal(secret_input.strip())

    db.update_provider_config(
        provider=provider,
        enabled=bool(enabled),
        collector_type="json_api",
        credential_type="cookie_header",
        usage_url=usage_url.strip(),
        headers_json=json.dumps(parsed_headers, sort_keys=True),
        notes=notes.strip(),
        secret_blob=sealed_secret,
    )
    return RedirectResponse(url=f"/settings?notice={quote(provider + '-saved')}", status_code=303)


@app.post("/settings/app", dependencies=[Depends(require_admin)])
async def update_app_settings(
    poll_interval_seconds: Annotated[int, Form()],
    heartbeat_interval_seconds: Annotated[int, Form()],
    db: Database = Depends(get_db),
):
    db.update_app_setting("poll_interval_seconds", str(max(60, poll_interval_seconds)))
    db.update_app_setting("heartbeat_interval_seconds", str(max(300, heartbeat_interval_seconds)))
    return RedirectResponse(url="/settings?notice=app-settings-saved", status_code=303)


@app.post("/settings/poll", dependencies=[Depends(require_admin)])
async def manual_poll(
    provider: Annotated[str, Form()] = "all",
    monitor: UsageMonitorService = Depends(get_monitor),
):
    results = await monitor.run_once(provider=provider)
    status = "manual-poll-ok" if any(item["ok"] for item in results) else "manual-poll-error"
    return RedirectResponse(url=f"/settings?notice={status}", status_code=303)


@app.get("/healthz")
async def healthcheck(db: Database = Depends(get_db)) -> JSONResponse:
    return JSONResponse(
        {
            "ok": True,
            "providers": db.get_provider_states(),
        }
    )


def _resolve_range(period: str, start: str | None, end: str | None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    start_at: datetime | None = None
    end_at: datetime | None = None

    if start:
        start_at = _parse_datetime(start)
    if end:
        end_at = _parse_datetime(end)

    if start_at is None and end_at is None:
        if period == "24h":
            start_at = now - timedelta(hours=24)
        elif period == "7d":
            start_at = now - timedelta(days=7)
        elif period == "30d":
            start_at = now - timedelta(days=30)
        elif period == "90d":
            start_at = now - timedelta(days=90)
        elif period == "all":
            start_at = None
            end_at = None

    if end_at is None and period != "all":
        end_at = now

    return {
        "period": period,
        "start_at": start_at.isoformat() if start_at else None,
        "end_at": end_at.isoformat() if end_at else None,
        "start_input": start_at.astimezone().strftime("%Y-%m-%dT%H:%M") if start_at else "",
        "end_input": end_at.astimezone().strftime("%Y-%m-%dT%H:%M") if end_at else "",
    }


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
