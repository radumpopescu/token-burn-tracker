"""Shared dataclasses for provider configs and normalized usage snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Any

VOLATILE_EXTRA_KEYS = {
    "reset_after_seconds",
    "seconds_until_reset",
    "remaining_seconds",
    "seconds_remaining",
}


@dataclass
class ProviderConfig:
    provider: str
    display_name: str
    enabled: bool
    collector_type: str
    credential_type: str
    usage_url: str
    headers_json: str
    notes: str
    secret_blob: str | None
    updated_at: str | None = None

    @property
    def has_secret(self) -> bool:
        return bool(self.secret_blob)


@dataclass
class UsageMetric:
    key: str
    label: str
    percent_value: float | None = None
    used_value: float | None = None
    limit_value: float | None = None
    unit: str | None = None
    resets_at: str | None = None
    raw_value: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def window_ends_at(self) -> str | None:
        return self.resets_at

    def stable_extra(self) -> dict[str, Any]:
        stable = {}
        for key, value in self.extra.items():
            if key in VOLATILE_EXTRA_KEYS:
                continue
            stable[str(key)] = value
        return stable

    def canonical_value(self) -> dict[str, Any]:
        return {
            "metric_key": self.key,
            "label": self.label,
            "unit": self.unit,
            "window_ends_at": self.window_ends_at,
            "percent_value": self.percent_value,
            "used_value": self.used_value,
            "limit_value": self.limit_value,
            "stable_extra": self.stable_extra(),
        }

    def state_item(self) -> dict[str, Any]:
        return self.canonical_value()


@dataclass
class UsageSnapshot:
    provider: str
    recorded_at: str
    page_title: str = ""
    plan_name: str | None = None
    summary: str = ""
    raw_text: str = ""
    normalized: dict[str, Any] = field(default_factory=dict)
    capture: dict[str, Any] = field(default_factory=dict)
    metrics: list[UsageMetric] = field(default_factory=list)

    def canonical_metrics(self) -> list[dict[str, Any]]:
        return sorted(
            (metric.state_item() for metric in self.metrics),
            key=lambda item: (
                item["metric_key"],
                item.get("window_ends_at") or "",
                item.get("label") or "",
                item.get("unit") or "",
            ),
        )

    def state_hash(self) -> str:
        payload = {
            "provider": self.provider,
            "plan_name": self.plan_name,
            "metrics": self.canonical_metrics(),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(encoded).hexdigest()

    def normalized_json(self) -> str:
        return json.dumps(self.normalized, sort_keys=True)

    def capture_json(self) -> str:
        return json.dumps(self.capture, sort_keys=True)

    def metrics_json(self) -> list[dict[str, Any]]:
        return [asdict(metric) for metric in self.metrics]


@dataclass
class CollectionResult:
    snapshot: UsageSnapshot
    updated_secret_value: str | None = None
