"""Provider-specific and generic normalization helpers."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import json
import re
from typing import Any

from .models import UsageMetric, UsageSnapshot

PLAN_RE = re.compile(r"\b(free|go|plus|pro|business|team|enterprise|edu)\b", re.IGNORECASE)
PERCENT_RE = re.compile(r"(?P<value>\d{1,3}(?:\.\d+)?)\s*%")
FRACTION_RE = re.compile(r"(?P<used>\d+(?:\.\d+)?)\s*/\s*(?P<limit>\d+(?:\.\d+)?)")
RESET_RE = re.compile(r"reset(?:s)?(?:\s+at|\s+in)?\s+(?P<value>.+)$", re.IGNORECASE)

WINDOW_KEYWORDS = {
    "five_hour": ("5 hour", "5-hour", "five hour", "5h", "local"),
    "seven_day": ("7 day", "7-day", "seven day", "weekly", "week"),
    "cloud": ("cloud", "task"),
    "messages": ("message",),
}


def parse_claude_usage_json(payload: dict[str, Any], recorded_at: str) -> UsageSnapshot:
    metrics: list[UsageMetric] = []
    summary_parts: list[str] = []

    for key, label in (
        ("five_hour", "5 hour"),
        ("seven_day", "7 day"),
        ("seven_day_oauth_apps", "7 day oauth apps"),
        ("seven_day_opus", "7 day opus"),
        ("seven_day_sonnet", "7 day sonnet"),
        ("seven_day_cowork", "7 day cowork"),
    ):
        section = payload.get(key)
        if not isinstance(section, dict):
            continue
        utilization = _float(section.get("utilization"))
        resets_at = section.get("resets_at")
        metrics.append(
            UsageMetric(
                key=f"{key}_utilization",
                label=label,
                percent_value=utilization,
                unit="percent",
                resets_at=resets_at,
                raw_value=json.dumps(section, sort_keys=True),
            )
        )
        if utilization is not None:
            summary = f"{label}: {utilization:.1f}%"
            if resets_at:
                summary += f" until {resets_at}"
            summary_parts.append(summary)

    extra_usage = payload.get("extra_usage")
    if isinstance(extra_usage, dict):
        used_credits = _float(extra_usage.get("used_credits"))
        monthly_limit = _float(extra_usage.get("monthly_limit"))
        utilization = _float(extra_usage.get("utilization"))
        if any(value is not None for value in (used_credits, monthly_limit, utilization)):
            metrics.append(
                UsageMetric(
                    key="extra_usage_monthly",
                    label="Extra usage monthly",
                    percent_value=utilization,
                    used_value=used_credits,
                    limit_value=monthly_limit,
                    unit="credits",
                    raw_value=json.dumps(extra_usage, sort_keys=True),
                    extra={"is_enabled": bool(extra_usage.get("is_enabled"))},
                )
            )
            summary_parts.append(
                "extra usage: "
                + _format_fraction(used_credits, monthly_limit, utilization, unit="credits")
            )

    summary = " | ".join(summary_parts) if summary_parts else "No Claude usage metrics found in response."
    return UsageSnapshot(
        provider="claude",
        recorded_at=recorded_at,
        page_title="Claude usage API",
        summary=summary,
        raw_text=json.dumps(payload, indent=2, sort_keys=True)[:12000],
        normalized=payload,
        capture={"kind": "json_api"},
        metrics=metrics,
    )


def parse_codex_usage_json(payload: dict[str, Any], recorded_at: str) -> UsageSnapshot:
    sanitized = dict(payload)
    sanitized.pop("email", None)
    sanitized.pop("user_id", None)
    sanitized.pop("account_id", None)

    plan_name = str(payload.get("plan_type", "")).title() or None
    metrics: list[UsageMetric] = []
    summary_parts: list[str] = []

    def add_window(prefix: str, label: str, window: dict[str, Any] | None) -> None:
        if not isinstance(window, dict):
            return
        percent_value = _float(window.get("used_percent"))
        reset_at = _epoch_to_iso(window.get("reset_at"))
        metric = UsageMetric(
            key=f"{prefix}_window",
            label=label,
            percent_value=percent_value,
            unit="percent",
            resets_at=reset_at,
            raw_value=json.dumps(window, sort_keys=True),
            extra={
                "limit_window_seconds": window.get("limit_window_seconds"),
                "reset_after_seconds": window.get("reset_after_seconds"),
                "allowed": True,
            },
        )
        metrics.append(metric)
        summary_parts.append(f"{label}: {_format_fraction(None, None, percent_value, 'percent')}")

    rate_limit = payload.get("rate_limit")
    if isinstance(rate_limit, dict):
        add_window("primary", "Primary window", rate_limit.get("primary_window"))
        add_window("secondary", "Secondary window", rate_limit.get("secondary_window"))

    code_review = payload.get("code_review_rate_limit")
    if isinstance(code_review, dict):
        add_window("code_review_primary", "Code review primary", code_review.get("primary_window"))
        add_window("code_review_secondary", "Code review secondary", code_review.get("secondary_window"))

    for item in payload.get("additional_rate_limits", []) or []:
        if not isinstance(item, dict):
            continue
        rate_limit_block = item.get("rate_limit")
        if not isinstance(rate_limit_block, dict):
            continue
        base_label = str(item.get("limit_name") or item.get("metered_feature") or "Additional rate limit")
        slug = _slug(base_label)
        add_window(f"{slug}_primary", f"{base_label} primary", rate_limit_block.get("primary_window"))
        add_window(f"{slug}_secondary", f"{base_label} secondary", rate_limit_block.get("secondary_window"))

    credits = payload.get("credits")
    if isinstance(credits, dict):
        approx_local = credits.get("approx_local_messages")
        approx_cloud = credits.get("approx_cloud_messages")
        if isinstance(approx_local, list) and len(approx_local) == 2:
            metrics.append(
                UsageMetric(
                    key="credits_local_messages",
                    label="Approx local messages",
                    used_value=_float(approx_local[0]),
                    limit_value=_float(approx_local[1]),
                    unit="messages",
                    raw_value=json.dumps(approx_local),
                )
            )
        if isinstance(approx_cloud, list) and len(approx_cloud) == 2:
            metrics.append(
                UsageMetric(
                    key="credits_cloud_messages",
                    label="Approx cloud messages",
                    used_value=_float(approx_cloud[0]),
                    limit_value=_float(approx_cloud[1]),
                    unit="messages",
                    raw_value=json.dumps(approx_cloud),
                )
            )

    summary = " | ".join(summary_parts) if summary_parts else "No Codex usage metrics found in response."
    return UsageSnapshot(
        provider="codex",
        recorded_at=recorded_at,
        page_title="Codex usage API",
        plan_name=plan_name,
        summary=summary,
        raw_text=json.dumps(sanitized, indent=2, sort_keys=True)[:12000],
        normalized=sanitized,
        capture={"kind": "json_api"},
        metrics=metrics,
    )


def parse_generic_json(provider: str, payload: Any, recorded_at: str, page_title: str) -> UsageSnapshot:
    metrics: list[UsageMetric] = []
    summary_parts: list[str] = []

    for path, value in _walk_payload(payload):
        if not isinstance(value, dict):
            continue

        label = _humanize_key(path)
        utilization = _float(value.get("utilization"))
        used_value = _float(value.get("used")) or _float(value.get("used_credits"))
        limit_value = _float(value.get("limit")) or _float(value.get("monthly_limit"))
        resets_at = value.get("resets_at")

        if any(item is not None for item in (utilization, used_value, limit_value)):
            metric = UsageMetric(
                key=f"{path}_metric",
                label=label,
                percent_value=utilization,
                used_value=used_value,
                limit_value=limit_value,
                resets_at=resets_at if isinstance(resets_at, str) else None,
                raw_value=json.dumps(value, sort_keys=True),
            )
            metrics.append(metric)
            summary_parts.append(f"{label}: {_format_fraction(used_value, limit_value, utilization)}")

    raw_text = json.dumps(payload, indent=2, sort_keys=True)[:12000]
    text_blob = raw_text.lower()
    plan_match = PLAN_RE.search(text_blob)
    summary = " | ".join(summary_parts) if summary_parts else "Stored raw JSON payload."
    return UsageSnapshot(
        provider=provider,
        recorded_at=recorded_at,
        page_title=page_title,
        plan_name=plan_match.group(1).title() if plan_match else None,
        summary=summary,
        raw_text=raw_text,
        normalized=payload if isinstance(payload, dict) else {"value": payload},
        capture={"kind": "json_api"},
        metrics=metrics,
    )


def parse_page_capture(provider: str, capture: dict[str, Any], recorded_at: str) -> UsageSnapshot:
    chunks = [chunk for chunk in capture.get("chunks", []) if isinstance(chunk, str)]
    progress_bars = [item for item in capture.get("progress_bars", []) if isinstance(item, dict)]
    lines = _normalize_lines(chunks + [capture.get("body_text", "")])
    page_title = str(capture.get("title", "")).strip()
    text_blob = "\n".join(lines)
    lower_blob = text_blob.lower()

    if any(phrase in lower_blob for phrase in ("log in", "sign in", "continue with google")) and "usage" not in lower_blob:
        raise RuntimeError("Authenticated usage page appears to have redirected to a login screen.")

    plan_match = PLAN_RE.search(lower_blob)
    metrics = _extract_metrics_from_text(lines, progress_bars)
    summary = " | ".join(_metric_summary(metric) for metric in metrics) if metrics else _fallback_summary(lines)

    return UsageSnapshot(
        provider=provider,
        recorded_at=recorded_at,
        page_title=page_title or f"{provider.title()} usage page",
        plan_name=plan_match.group(1).title() if plan_match else None,
        summary=summary,
        raw_text=text_blob[:12000],
        normalized={
            "title": page_title,
            "url": capture.get("url"),
            "chunks": chunks[:100],
            "progress_bars": progress_bars[:25],
        },
        capture=capture,
        metrics=metrics,
    )


def _extract_metrics_from_text(lines: list[str], progress_bars: list[dict[str, Any]]) -> list[UsageMetric]:
    metrics: dict[str, UsageMetric] = {}

    for item in progress_bars:
        text = " ".join(
            str(item.get(key, "")).strip()
            for key in ("label", "value_text", "text")
            if item.get(key)
        )
        percent_value = _float(item.get("value_now"))
        if percent_value is None and item.get("value_text"):
            match = PERCENT_RE.search(str(item["value_text"]))
            percent_value = _float(match.group("value")) if match else None
        key = _guess_metric_key(text)
        if key and percent_value is not None and key not in metrics:
            metrics[key] = UsageMetric(
                key=key,
                label=_humanize_key(key),
                percent_value=percent_value,
                unit="percent",
                raw_value=json.dumps(item, sort_keys=True),
            )

    for line in lines:
        candidate = line.strip()
        if len(candidate) < 3:
            continue
        lower = candidate.lower()
        if not any(token in lower for token in ("% ", "%", "reset", "remaining", "used", "limit", "task", "message", "hour", "week")):
            continue
        key = _guess_metric_key(candidate)
        percent = _parse_percent(candidate)
        used_value, limit_value = _parse_fraction(candidate)
        resets_at = _parse_reset(candidate)
        if not key or all(value is None for value in (percent, used_value, limit_value)):
            continue

        current = metrics.get(key)
        if current and current.percent_value is not None and percent is None and used_value is None and limit_value is None:
            continue

        metrics[key] = UsageMetric(
            key=key,
            label=_humanize_key(key),
            percent_value=percent,
            used_value=used_value,
            limit_value=limit_value,
            unit=_guess_unit(candidate),
            resets_at=resets_at,
            raw_value=candidate,
        )

    return list(metrics.values())


def _normalize_lines(chunks: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for chunk in chunks:
        for raw_line in str(chunk).splitlines():
            line = " ".join(raw_line.split())
            if len(line) < 2:
                continue
            if line in seen:
                continue
            seen.add(line)
            results.append(line[:300])
    return results


def _walk_payload(payload: Any, path: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            next_path = f"{path}.{key}" if path else key
            yield next_path, value
            yield from _walk_payload(value, next_path)
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            next_path = f"{path}[{index}]"
            yield next_path, value
            yield from _walk_payload(value, next_path)


def _guess_metric_key(text: str) -> str | None:
    lowered = text.lower()
    tokens: list[str] = []
    for token, keywords in WINDOW_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            tokens.append(token)
    if not tokens and "%" in lowered:
        tokens.append("usage")
    if not tokens:
        return None
    return "_".join(dict.fromkeys(tokens))


def _guess_unit(text: str) -> str | None:
    lowered = text.lower()
    if "task" in lowered:
        return "tasks"
    if "message" in lowered:
        return "messages"
    if "credit" in lowered:
        return "credits"
    if "%" in lowered:
        return "percent"
    return None


def _metric_summary(metric: UsageMetric) -> str:
    return f"{metric.label}: {_format_fraction(metric.used_value, metric.limit_value, metric.percent_value, metric.unit)}"


def _fallback_summary(lines: list[str]) -> str:
    if not lines:
        return "No visible usage text was captured from the page."
    return " | ".join(lines[:3])[:240]


def _humanize_key(key: str) -> str:
    label = key.replace("_", " ").replace(".", " ").replace("[", " ").replace("]", " ")
    return " ".join(part for part in label.split() if part).title()


def _format_fraction(
    used_value: float | None,
    limit_value: float | None,
    percent_value: float | None,
    unit: str | None = None,
) -> str:
    parts: list[str] = []
    if used_value is not None or limit_value is not None:
        used = _format_number(used_value) if used_value is not None else "?"
        limit = _format_number(limit_value) if limit_value is not None else "?"
        suffix = f" {unit}" if unit and unit != "percent" else ""
        parts.append(f"{used}/{limit}{suffix}")
    if percent_value is not None:
        parts.append(f"{percent_value:.1f}%")
    return ", ".join(parts) if parts else "n/a"


def _format_number(value: float | None) -> str:
    if value is None:
        return "?"
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_percent(text: str) -> float | None:
    match = PERCENT_RE.search(text)
    return _float(match.group("value")) if match else None


def _parse_fraction(text: str) -> tuple[float | None, float | None]:
    match = FRACTION_RE.search(text)
    if not match:
        return None, None
    return _float(match.group("used")), _float(match.group("limit"))


def _parse_reset(text: str) -> str | None:
    match = RESET_RE.search(text)
    if not match:
        return None
    value = match.group("value").strip()
    try:
        return datetime.fromisoformat(value).isoformat()
    except ValueError:
        return value[:120]


def _epoch_to_iso(value: Any) -> str | None:
    raw = _float(value)
    if raw is None:
        return None
    return datetime.fromtimestamp(raw, tz=timezone.utc).isoformat()


def _slug(value: str) -> str:
    lowered = value.lower()
    return re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
