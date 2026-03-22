"""Helpers for importing browser requests and storing provider secrets."""

from __future__ import annotations

from dataclasses import dataclass
import json
import shlex
from typing import Mapping
from urllib.parse import urlparse

IMPORTED_HEADER_ALLOWLIST = {
    "accept",
    "accept-language",
    "anthropic-anonymous-id",
    "anthropic-client-platform",
    "anthropic-client-sha",
    "anthropic-client-version",
    "anthropic-device-id",
    "content-type",
    "oai-client-build-number",
    "oai-client-version",
    "oai-device-id",
    "oai-language",
    "oai-session-id",
    "referer",
    "user-agent",
    "x-openai-target-path",
    "x-openai-target-route",
}


@dataclass
class ImportedRequest:
    url: str | None
    cookie_header: str | None
    authorization: str | None
    headers: dict[str, str]


def parse_curl_import(command: str) -> ImportedRequest:
    compact = command.replace("\\\r\n", " ").replace("\\\n", " ").strip()
    if not compact:
        raise ValueError("Request import was empty.")

    try:
        tokens = shlex.split(compact)
    except ValueError as exc:
        raise ValueError("Could not parse the pasted curl command.") from exc

    if not tokens or tokens[0] != "curl":
        raise ValueError("Paste the full curl command copied from browser devtools.")

    url: str | None = None
    cookie_header: str | None = None
    authorization: str | None = None
    headers: dict[str, str] = {}

    index = 1
    while index < len(tokens):
        token = tokens[index]

        if token in {"-H", "--header"}:
            index += 1
            if index >= len(tokens):
                break
            header_line = tokens[index]
            name, sep, value = header_line.partition(":")
            if sep:
                header_name = name.strip().lower()
                header_value = value.strip()
                if header_name == "cookie":
                    cookie_header = header_value
                elif header_name == "authorization":
                    authorization = normalize_authorization(header_value)
                elif header_name in IMPORTED_HEADER_ALLOWLIST:
                    headers[header_name] = header_value
            index += 1
            continue

        if token in {"-b", "--cookie"}:
            index += 1
            if index < len(tokens):
                cookie_header = tokens[index].strip()
            index += 1
            continue

        if token in {"-A", "--user-agent"}:
            index += 1
            if index < len(tokens):
                headers["user-agent"] = tokens[index].strip()
            index += 1
            continue

        if token == "--url":
            index += 1
            if index < len(tokens):
                url = tokens[index].strip()
            index += 1
            continue

        if token.startswith("http://") or token.startswith("https://"):
            url = token.strip()
            index += 1
            continue

        index += 1

    if not url:
        raise ValueError("Could not find a URL in the pasted curl command.")

    return ImportedRequest(
        url=url,
        cookie_header=cookie_header,
        authorization=authorization,
        headers=headers,
    )


def decode_secret_payload(raw_value: str | None) -> dict[str, str]:
    if not raw_value:
        return {}

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, dict):
        result: dict[str, str] = {}
        for key, value in parsed.items():
            if value is None:
                continue
            text = str(value).strip()
            if text:
                result[str(key)] = text
        if "authorization" in result:
            result["authorization"] = normalize_authorization(result["authorization"]) or ""
            if not result["authorization"]:
                result.pop("authorization", None)
        return result

    return {"cookie_header": raw_value.strip()}


def encode_secret_payload(values: Mapping[str, str]) -> str:
    payload = {
        str(key): str(value).strip()
        for key, value in values.items()
        if str(value).strip()
    }
    if "authorization" in payload:
        payload["authorization"] = normalize_authorization(payload["authorization"]) or ""
        if not payload["authorization"]:
            payload.pop("authorization", None)
    return json.dumps(payload, sort_keys=True)


def normalize_authorization(value: str | None) -> str | None:
    if not value:
        return None
    token = value.strip()
    if not token:
        return None
    if token.lower().startswith("bearer "):
        return "Bearer " + token[7:].strip()
    return "Bearer " + token


def parse_cookie_header(value: str | None) -> dict[str, str]:
    if not value:
        return {}
    cookies: dict[str, str] = {}
    for chunk in value.split(";"):
        name, sep, cookie_value = chunk.partition("=")
        if not sep:
            continue
        cookie_name = name.strip()
        if not cookie_name:
            continue
        cookies[cookie_name] = cookie_value.strip()
    return cookies


def format_cookie_header(cookies: Mapping[str, str]) -> str:
    return "; ".join(
        f"{name}={value}"
        for name, value in cookies.items()
        if str(name).strip() and str(value).strip()
    )


def format_cookie_header_for_url(cookie_jar: object, url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    path = parsed.path or "/"
    pairs: dict[str, str] = {}

    for cookie in cookie_jar:
        cookie_domain = getattr(cookie, "domain", "") or ""
        cookie_path = getattr(cookie, "path", "") or "/"
        if not _domain_matches(host, cookie_domain):
            continue
        if not path.startswith(cookie_path):
            continue
        pairs[str(cookie.name)] = str(cookie.value)

    return format_cookie_header(pairs)


def _domain_matches(host: str, cookie_domain: str) -> bool:
    normalized = cookie_domain.lstrip(".")
    return not normalized or host == normalized or host.endswith(f".{normalized}")
