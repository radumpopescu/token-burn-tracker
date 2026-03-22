"""Collectors for usage data from direct JSON endpoints and authenticated pages."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any
from urllib.parse import urlparse

import httpx

from .models import CollectionResult, ProviderConfig, UsageSnapshot
from .parsers import (
    parse_claude_usage_json,
    parse_codex_usage_json,
    parse_generic_json,
)
from .request_imports import (
    decode_secret_payload,
    encode_secret_payload,
    format_cookie_header_for_url,
    normalize_authorization,
    parse_cookie_header,
)


async def collect_usage(config: ProviderConfig, secret_value: str) -> CollectionResult:
    recorded_at = datetime.now(timezone.utc).isoformat()
    if config.collector_type != "json_api":
        raise RuntimeError(f"Unsupported collector type: {config.collector_type}")
    return await _collect_json_api(config, secret_value, recorded_at)


async def _collect_json_api(config: ProviderConfig, secret_value: str, recorded_at: str) -> CollectionResult:
    headers = _load_headers(config.headers_json)
    secret_payload = decode_secret_payload(secret_value)
    original_secret_payload = dict(secret_payload)
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
    cookie_header = secret_payload.get("cookie_header", "").strip()
    if not cookie_header:
        raise RuntimeError("No Cookie header is stored for this provider.")
    request_headers = dict(headers)

    if config.provider == "codex":
        request_headers.setdefault("Referer", "https://chatgpt.com/codex/settings/usage")
        request_headers.setdefault("oai-language", "en-US")
        request_headers.setdefault("x-openai-target-path", parsed_url.path)
        request_headers.setdefault("x-openai-target-route", parsed_url.path)

    authorization = normalize_authorization(secret_payload.get("authorization"))
    if authorization:
        request_headers["Authorization"] = authorization

    async with httpx.AsyncClient(
        follow_redirects=True,
        cookies=parse_cookie_header(cookie_header),
    ) as client:
        if config.provider == "codex" and "Authorization" not in request_headers:
            refreshed_authorization = await _fetch_codex_authorization(client, origin, request_headers)
            if refreshed_authorization:
                request_headers["Authorization"] = refreshed_authorization
                secret_payload["authorization"] = refreshed_authorization
        response = await client.get(config.usage_url, headers=request_headers, timeout=45)
        if config.provider == "codex" and response.status_code == 401:
            refreshed_authorization = await _fetch_codex_authorization(client, origin, request_headers)
            if refreshed_authorization:
                request_headers["Authorization"] = refreshed_authorization
                secret_payload["authorization"] = refreshed_authorization
                response = await client.get(config.usage_url, headers=request_headers, timeout=45)
        if config.provider == "codex" and response.status_code == 401:
            raise RuntimeError(
                "Codex returned 401. Paste the full curl command from browser devtools so the app can store the Authorization bearer token and OpenAI request headers."
            )
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

    latest_cookie_header = format_cookie_header_for_url(client.cookies.jar, config.usage_url)
    if latest_cookie_header and latest_cookie_header != cookie_header:
        secret_payload["cookie_header"] = latest_cookie_header

    updated_secret_value = None
    if secret_payload != original_secret_payload:
        updated_secret_value = encode_secret_payload(secret_payload)

    return CollectionResult(snapshot=snapshot, updated_secret_value=updated_secret_value)


def _load_headers(headers_json: str) -> dict[str, str]:
    value = headers_json.strip()
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise RuntimeError("Custom headers must be a JSON object.")
    return {str(key): str(item) for key, item in parsed.items()}


async def _fetch_codex_authorization(
    client: httpx.AsyncClient,
    origin: str,
    headers: dict[str, str],
) -> str | None:
    session_headers = {
        key: value
        for key, value in headers.items()
        if key.lower() not in {"authorization", "x-openai-target-path", "x-openai-target-route"}
    }
    session_headers["Referer"] = origin + "/"

    try:
        response = await client.get(f"{origin}/api/auth/session", headers=session_headers, timeout=30)
    except httpx.HTTPError:
        return None

    content_type = response.headers.get("content-type", "")
    if response.status_code >= 400 or "json" not in content_type:
        return None

    try:
        payload = response.json()
    except ValueError:
        return None

    token = payload.get("accessToken") or payload.get("access_token")
    return normalize_authorization(token)
