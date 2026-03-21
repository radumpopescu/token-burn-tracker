"""Collectors for usage data from direct JSON endpoints and authenticated pages."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any
from urllib.parse import urlparse

import httpx

from .models import ProviderConfig, UsageSnapshot
from .parsers import (
    parse_claude_usage_json,
    parse_codex_usage_json,
    parse_generic_json,
)


async def collect_usage(config: ProviderConfig, secret_value: str) -> UsageSnapshot:
    recorded_at = datetime.now(timezone.utc).isoformat()
    if config.collector_type != "json_api":
        raise RuntimeError(f"Unsupported collector type: {config.collector_type}")
    return await _collect_json_api(config, secret_value, recorded_at)


async def _collect_json_api(config: ProviderConfig, secret_value: str, recorded_at: str) -> UsageSnapshot:
    headers = _load_headers(config.headers_json)
    parsed_url = urlparse(config.usage_url)
    origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
    headers.setdefault("Accept", "application/json, text/plain, */*")
    headers.setdefault(
        "User-Agent",
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36"
        ),
    )
    headers.setdefault("Accept-Language", "en-US,en;q=0.9")
    headers.setdefault("Origin", origin)
    headers.setdefault("Referer", origin + "/")
    if config.credential_type != "cookie_header":
        raise RuntimeError(f"Unsupported credential type for JSON API collector: {config.credential_type}")
    headers["Cookie"] = secret_value.strip()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(config.usage_url, headers=headers, timeout=45)
        response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if "json" in content_type:
        payload = response.json()
        if config.provider == "claude":
            snapshot = parse_claude_usage_json(payload, recorded_at)
        elif config.provider == "codex":
            snapshot = parse_codex_usage_json(payload, recorded_at)
        else:
            snapshot = parse_generic_json(
                provider=config.provider,
                payload=payload,
                recorded_at=recorded_at,
                page_title=f"{config.display_name} usage API",
            )
    else:
        raise RuntimeError("Usage endpoint did not return JSON.")

    snapshot.capture.update(
        {
            "kind": "json_api",
            "url": str(response.url),
            "status_code": response.status_code,
            "content_type": content_type,
        }
    )
    return snapshot


def _load_headers(headers_json: str) -> dict[str, str]:
    value = headers_json.strip()
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise RuntimeError("Custom headers must be a JSON object.")
    return {str(key): str(item) for key, item in parsed.items()}
