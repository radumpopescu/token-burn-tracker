"""Shared dataclasses for provider configs and normalized usage snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import json
from typing import Any


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

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "percent_value": self.percent_value,
            "used_value": self.used_value,
            "limit_value": self.limit_value,
            "unit": self.unit,
            "resets_at": self.resets_at,
            "raw_value": self.raw_value,
            "extra": self.extra,
        }


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

    def snapshot_hash(self) -> str:
        payload = {
            "provider": self.provider,
            "plan_name": self.plan_name,
            "summary": self.summary,
            "normalized": self.normalized,
            "metrics": [metric.as_dict() for metric in self.metrics],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return sha256(encoded).hexdigest()

    def normalized_json(self) -> str:
        return json.dumps(self.normalized, sort_keys=True)

    def capture_json(self) -> str:
        return json.dumps(self.capture, sort_keys=True)

    def metrics_json(self) -> list[dict[str, Any]]:
        return [asdict(metric) for metric in self.metrics]
