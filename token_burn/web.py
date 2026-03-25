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
from .db import (
    DB_SENTINEL,
    DEFAULT_AUTO_REFRESH_EQUAL_POLLS_BEFORE_STEP,
    DEFAULT_AUTO_REFRESH_STEP_SECONDS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_SLOW_REFRESH_INTERVAL_SECONDS,
    Database,
)
from .providers import PROVIDER_SPECS, provider_choices
from .request_imports import decode_secret_payload, encode_secret_payload, parse_curl_import
from .security import admin_auth_enabled, require_admin
from .service import UsageMonitorService

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "token_burn.db"
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database(DB_PATH)
    db.init_db()
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
    metric_labels = _parse_metric_labels(settings.get("metric_labels", "{}"))
    initial_history_payload = {
        "filters": filters,
        "refresh_settings": _refresh_settings(settings),
        "metric_labels": metric_labels,
        "latest": latest,
        "states": states,
        "series": db.list_metric_series(
            provider=provider,
            start_at=filters["start_at"],
            end_at=filters["end_at"],
        ),
        "provider_order": _dashboard_provider_order(settings),
    }
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
            "poll_interval_seconds": settings.get("poll_interval_seconds", str(DEFAULT_POLL_INTERVAL_SECONDS)),
            "heartbeat_interval_seconds": settings.get(
                "heartbeat_interval_seconds",
                str(DEFAULT_HEARTBEAT_INTERVAL_SECONDS),
            ),
            "refresh_settings": _refresh_settings(settings),
            "provider_order": _dashboard_provider_order(settings),
            "initial_history_payload": initial_history_payload,
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
    settings = db.get_app_settings()
    metric_labels = _parse_metric_labels(settings.get("metric_labels", "{}"))
    refresh_settings = _refresh_settings(settings)
    payload = {
        "filters": filters,
        "poll_interval_seconds": refresh_settings["fast_interval_seconds"],
        "refresh_settings": refresh_settings,
        "metric_labels": metric_labels,
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
        "provider_order": _dashboard_provider_order(settings),
    }
    return JSONResponse(payload)


@app.get("/settings", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def settings_page(request: Request, db: Database = Depends(get_db)):
    configs = {config.provider: config for config in db.list_provider_configs()}
    secret_details: dict[str, dict[str, bool]] = {}
    for provider, config in configs.items():
        raw_secret = request.app.state.secret_box.open(config.secret_blob) if config.secret_blob else None
        secret_values = decode_secret_payload(raw_secret)
        secret_details[provider] = {
            "has_cookie": bool(secret_values.get("cookie_header")),
            "has_authorization": bool(secret_values.get("authorization")),
            "has_imported_headers": bool((config.headers_json or "").strip() not in {"", "{}"}),
        }
    states = db.get_provider_states()
    settings = db.get_app_settings()
    metric_labels = _parse_metric_labels(settings.get("metric_labels", "{}"))
    known_metrics = db.list_distinct_metrics()
    refresh_settings = _refresh_settings(settings)
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
            "secret_details": secret_details,
            "known_metrics": known_metrics,
            "metric_labels": metric_labels,
            "dashboard_top_provider": _dashboard_provider_order(settings)[0] if provider_choices() else "",
            "refresh_settings": refresh_settings,
        },
    )


@app.post("/settings/provider/{provider}", dependencies=[Depends(require_admin)])
async def update_provider_settings(
    provider: str,
    request: Request,
    enabled: Annotated[str | None, Form()] = None,
    usage_url: Annotated[str, Form()] = "",
    request_import: Annotated[str, Form()] = "",
    notes: Annotated[str, Form()] = "",
    secret_input: Annotated[str, Form()] = "",
    authorization_input: Annotated[str, Form()] = "",
    clear_secret: Annotated[str | None, Form()] = None,
    db: Database = Depends(get_db),
    secret_box: SecretBox = Depends(get_secret_box),
):
    if provider not in PROVIDER_SPECS:
        return RedirectResponse(url="/settings?notice=unknown-provider", status_code=303)

    current_config = db.get_provider_config(provider)
    existing_secret_values = {}
    if current_config.secret_blob:
        existing_secret_values = decode_secret_payload(secret_box.open(current_config.secret_blob))

    final_usage_url = usage_url.strip() or current_config.usage_url
    parsed_headers: dict[str, str] = {}
    if provider == "codex":
        try:
            current_headers = json.loads(current_config.headers_json or "{}")
            if isinstance(current_headers, dict):
                parsed_headers = {str(key): str(value) for key, value in current_headers.items()}
        except ValueError:
            parsed_headers = {}

    secret_values = dict(existing_secret_values)
    if request_import.strip():
        try:
            imported_request = parse_curl_import(request_import)
        except ValueError:
            return RedirectResponse(url="/settings?notice=invalid-request-import", status_code=303)
        final_usage_url = imported_request.url or final_usage_url
        if imported_request.cookie_header:
            secret_values["cookie_header"] = imported_request.cookie_header
        if imported_request.authorization:
            secret_values["authorization"] = imported_request.authorization
        parsed_headers = imported_request.headers

    sealed_secret: str | object = DB_SENTINEL
    if clear_secret:
        sealed_secret = None
    else:
        if secret_input.strip():
            secret_values["cookie_header"] = secret_input.strip()
        if authorization_input.strip():
            secret_values["authorization"] = authorization_input.strip()

        if request_import.strip() or secret_input.strip() or authorization_input.strip():
            sealed_secret = secret_box.seal(encode_secret_payload(secret_values))

    db.update_provider_config(
        provider=provider,
        enabled=bool(enabled),
        collector_type="json_api",
        credential_type="cookie_header",
        usage_url=final_usage_url,
        headers_json=json.dumps(parsed_headers, sort_keys=True),
        notes=notes.strip(),
        secret_blob=sealed_secret,
    )
    return RedirectResponse(url=f"/settings?notice={quote(provider + '-saved')}", status_code=303)


@app.post("/settings/app", dependencies=[Depends(require_admin)])
async def update_app_settings(
    poll_interval_minutes: Annotated[int, Form()],
    slow_refresh_interval_minutes: Annotated[int, Form()],
    auto_refresh_step_minutes: Annotated[int, Form()],
    auto_refresh_equal_polls_before_step: Annotated[int, Form()],
    heartbeat_interval_seconds: Annotated[int, Form()],
    db: Database = Depends(get_db),
):
    fast_seconds = max(60, poll_interval_minutes * 60)
    slow_seconds = max(fast_seconds, slow_refresh_interval_minutes * 60)
    auto_step_seconds = max(60, auto_refresh_step_minutes * 60)
    equal_polls_before_step = max(1, auto_refresh_equal_polls_before_step)
    db.update_app_setting("poll_interval_seconds", str(fast_seconds))
    db.update_app_setting("heartbeat_interval_seconds", str(max(300, heartbeat_interval_seconds)))
    db.update_app_setting("slow_refresh_interval_seconds", str(slow_seconds))
    db.update_app_setting("auto_refresh_step_seconds", str(auto_step_seconds))
    db.update_app_setting(
        "auto_refresh_equal_polls_before_step",
        str(equal_polls_before_step),
    )
    return RedirectResponse(url="/settings?notice=app-settings-saved", status_code=303)


@app.post("/settings/layout", dependencies=[Depends(require_admin)])
async def update_dashboard_layout(
    dashboard_top_provider: Annotated[str, Form()],
    db: Database = Depends(get_db),
):
    available = {spec["provider"] for spec in provider_choices()}
    if dashboard_top_provider not in available:
        return RedirectResponse(url="/settings?notice=invalid-dashboard-provider", status_code=303)
    db.update_app_setting("dashboard_top_provider", dashboard_top_provider)
    return RedirectResponse(url="/settings?notice=layout-saved", status_code=303)


@app.post("/settings/labels", dependencies=[Depends(require_admin)])
async def update_metric_labels(
    request: Request,
    db: Database = Depends(get_db),
):
    form = await request.form()
    labels: dict[str, str] = {}
    for key, value in form.items():
        if key.startswith("label_") and str(value).strip():
            metric_key = key[6:]
            labels[metric_key] = str(value).strip()
    db.update_app_setting("metric_labels", json.dumps(labels, sort_keys=True))
    return RedirectResponse(url="/settings?notice=labels-saved", status_code=303)


@app.post("/settings/poll", dependencies=[Depends(require_admin)])
async def manual_poll(
    provider: Annotated[str, Form()] = "all",
    monitor: UsageMonitorService = Depends(get_monitor),
):
    results = await monitor.run_once(provider=provider)
    status = "manual-poll-ok" if any(item["ok"] for item in results) else "manual-poll-error"
    return RedirectResponse(url=f"/settings?notice={status}", status_code=303)


@app.post("/api/test/{provider}", dependencies=[Depends(require_admin)])
async def test_provider(
    provider: str,
    monitor: UsageMonitorService = Depends(get_monitor),
) -> JSONResponse:
    if provider not in PROVIDER_SPECS:
        return JSONResponse({"ok": False, "error": "Unknown provider"}, status_code=400)
    results = await monitor.run_once(provider=provider)
    result = results[0] if results else {"ok": False, "error": "No result"}
    return JSONResponse(result)


@app.post("/api/provider/{provider}/refresh-mode")
async def update_provider_refresh_mode(
    provider: str,
    mode: Annotated[str, Form()],
    monitor: UsageMonitorService = Depends(get_monitor),
) -> JSONResponse:
    if provider not in PROVIDER_SPECS:
        return JSONResponse({"ok": False, "error": "Unknown provider"}, status_code=400)
    try:
        state = await monitor.set_refresh_mode(provider, mode)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "provider": provider, "state": state})


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
        if period == "1h":
            start_at = now - timedelta(hours=1)
        elif period == "24h":
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


def _parse_metric_labels(raw: str) -> dict[str, str]:
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items() if str(v).strip()}
    except (json.JSONDecodeError, TypeError):
        pass
    return {}


def _dashboard_provider_order(settings: dict[str, str]) -> list[str]:
    available = [spec["provider"] for spec in provider_choices()]
    if not available:
        return []
    top_provider = settings.get("dashboard_top_provider") or ("codex" if "codex" in available else available[0])
    if top_provider not in available:
        top_provider = "codex" if "codex" in available else available[0]
    return [top_provider, *[provider for provider in available if provider != top_provider]]


def _refresh_settings(settings: dict[str, str]) -> dict[str, Any]:
    fast_interval_seconds = _parse_int_setting(
        settings,
        "poll_interval_seconds",
        default=DEFAULT_POLL_INTERVAL_SECONDS,
        minimum=60,
    )
    slow_interval_seconds = _parse_int_setting(
        settings,
        "slow_refresh_interval_seconds",
        default=DEFAULT_SLOW_REFRESH_INTERVAL_SECONDS,
        minimum=fast_interval_seconds,
    )
    auto_step_seconds = _parse_int_setting(
        settings,
        "auto_refresh_step_seconds",
        default=DEFAULT_AUTO_REFRESH_STEP_SECONDS,
        minimum=60,
    )
    equal_polls_before_step = _parse_int_setting(
        settings,
        "auto_refresh_equal_polls_before_step",
        default=DEFAULT_AUTO_REFRESH_EQUAL_POLLS_BEFORE_STEP,
        minimum=1,
    )
    return {
        "fast_interval_seconds": fast_interval_seconds,
        "slow_interval_seconds": max(fast_interval_seconds, slow_interval_seconds),
        "auto_step_seconds": auto_step_seconds,
        "equal_polls_before_step": equal_polls_before_step,
        "fast_interval_minutes": max(1, fast_interval_seconds // 60),
        "slow_interval_minutes": max(1, slow_interval_seconds // 60),
        "auto_step_minutes": max(1, auto_step_seconds // 60),
        "fast_label": _interval_label(fast_interval_seconds),
        "slow_label": _interval_label(max(fast_interval_seconds, slow_interval_seconds)),
    }


def _parse_int_setting(
    settings: dict[str, str],
    key: str,
    *,
    default: int,
    minimum: int,
) -> int:
    try:
        value = int(settings.get(key, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _interval_label(seconds: int) -> str:
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours}h"
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes}m"
    return f"{seconds}s"


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
