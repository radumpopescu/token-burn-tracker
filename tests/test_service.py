from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from token_burn.db import Database
from token_burn.models import CollectionResult, UsageMetric, UsageSnapshot
import token_burn.service as service_module
from token_burn.service import UsageMonitorService


class UsageStorageTests(unittest.IsolatedAsyncioTestCase):
    def make_db(self) -> Database:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        db = Database(Path(temp_dir.name) / "token_burn.db")
        db.init_db()
        return db

    async def test_state_hash_ignores_volatile_fields(self) -> None:
        first = _snapshot(
            provider="codex",
            recorded_at=datetime.now(timezone.utc).isoformat(),
            summary="Primary window: 10.0% in 3h",
            raw_text='{"reset_after_seconds": 900}',
            normalized={"reset_after_seconds": 900},
            metrics=[
                UsageMetric(
                    key="primary_window",
                    label="Primary window",
                    percent_value=10.0,
                    unit="percent",
                    resets_at="2026-03-22T12:00:00+00:00",
                    extra={"limit_window_seconds": 18000, "reset_after_seconds": 900, "allowed": True},
                )
            ],
        )
        second = _snapshot(
            provider="codex",
            recorded_at=datetime.now(timezone.utc).isoformat(),
            summary="Primary window: 10.0% in 2h 58m",
            raw_text='{"reset_after_seconds": 780}',
            normalized={"reset_after_seconds": 780},
            metrics=[
                UsageMetric(
                    key="primary_window",
                    label="Primary window",
                    percent_value=10.0,
                    unit="percent",
                    resets_at="2026-03-22T12:00:00+00:00",
                    extra={"limit_window_seconds": 18000, "reset_after_seconds": 780, "allowed": True},
                )
            ],
        )
        changed_value = _snapshot(
            provider="codex",
            recorded_at=datetime.now(timezone.utc).isoformat(),
            summary="Primary window: 11.0%",
            raw_text="{}",
            normalized={},
            metrics=[
                UsageMetric(
                    key="primary_window",
                    label="Primary window",
                    percent_value=11.0,
                    unit="percent",
                    resets_at="2026-03-22T12:00:00+00:00",
                    extra={"limit_window_seconds": 18000, "allowed": True},
                )
            ],
        )
        changed_window = _snapshot(
            provider="codex",
            recorded_at=datetime.now(timezone.utc).isoformat(),
            summary="Primary window: 10.0%",
            raw_text="{}",
            normalized={},
            metrics=[
                UsageMetric(
                    key="primary_window",
                    label="Primary window",
                    percent_value=10.0,
                    unit="percent",
                    resets_at="2026-03-22T13:00:00+00:00",
                    extra={"limit_window_seconds": 18000, "allowed": True},
                )
            ],
        )

        self.assertEqual(first.state_hash(), second.state_hash())
        self.assertNotEqual(first.state_hash(), changed_value.state_hash())
        self.assertNotEqual(first.state_hash(), changed_window.state_hash())

    async def test_single_metric_change_only_persists_that_metric(self) -> None:
        base = datetime.now(timezone.utc)
        snapshots = deque(
            [
                _snapshot(
                    provider="codex",
                    recorded_at=base.isoformat(),
                    metrics=[
                        _percent_metric("primary_window", "Primary window", 10.0, "2026-03-22T12:00:00+00:00"),
                        _percent_metric("secondary_window", "Secondary window", 35.0, "2026-03-29T12:00:00+00:00"),
                    ],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=1)).isoformat(),
                    summary="same values, different payload noise",
                    raw_text='{"reset_after_seconds": 120}',
                    normalized={"reset_after_seconds": 120},
                    metrics=[
                        UsageMetric(
                            key="primary_window",
                            label="Primary window",
                            percent_value=10.0,
                            unit="percent",
                            resets_at="2026-03-22T12:00:00+00:00",
                            extra={"reset_after_seconds": 120},
                        ),
                        UsageMetric(
                            key="secondary_window",
                            label="Secondary window",
                            percent_value=35.0,
                            unit="percent",
                            resets_at="2026-03-29T12:00:00+00:00",
                            extra={"reset_after_seconds": 600},
                        ),
                    ],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=2)).isoformat(),
                    metrics=[
                        _percent_metric("primary_window", "Primary window", 15.0, "2026-03-22T12:00:00+00:00"),
                        _percent_metric("secondary_window", "Secondary window", 35.0, "2026-03-29T12:00:00+00:00"),
                    ],
                ),
            ]
        )

        db = self.make_db()
        service = UsageMonitorService(db, _DummySecretBox())
        _enable_provider(db, "codex")

        async def fake_collect_usage(config, secret_value):
            return CollectionResult(snapshot=snapshots.popleft())

        with _patched_collect(fake_collect_usage):
            first = await service.run_once(provider="codex")
            second = await service.run_once(provider="codex")
            third = await service.run_once(provider="codex")

        self.assertEqual(first[0]["reason"], "initial")
        self.assertEqual(second[0]["reason"], "unchanged")
        self.assertEqual(third[0]["reason"], "changed")

        with db.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM snapshots WHERE provider = 'codex'").fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM metric_samples WHERE provider = 'codex'").fetchone()[0],
                3,
            )
            latest_event_rows = conn.execute(
                """
                SELECT metric_key, percent_value
                FROM metric_samples
                WHERE event_id = (SELECT MAX(id) FROM snapshots WHERE provider = 'codex')
                ORDER BY metric_key
                """
            ).fetchall()

        self.assertEqual(len(latest_event_rows), 1)
        self.assertEqual(latest_event_rows[0][0], "primary_window")
        self.assertEqual(latest_event_rows[0][1], 15.0)

        latest = db.latest_snapshots_by_provider()["codex"]
        self.assertEqual(len(latest["metrics"]), 2)
        latest_metrics = {item["metric_key"]: item for item in latest["metrics"]}
        self.assertEqual(latest_metrics["primary_window"]["percent_value"], 15.0)
        self.assertEqual(latest_metrics["secondary_window"]["percent_value"], 35.0)

    async def test_metric_heartbeat_persists_only_due_series(self) -> None:
        base = datetime.now(timezone.utc)
        snapshots = deque(
            [
                _snapshot(
                    provider="codex",
                    recorded_at=base.isoformat(),
                    metrics=[
                        _percent_metric("primary_window", "Primary window", 10.0, "2026-03-22T12:00:00+00:00"),
                        _percent_metric("secondary_window", "Secondary window", 35.0, "2026-03-29T12:00:00+00:00"),
                    ],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=1)).isoformat(),
                    metrics=[
                        _percent_metric("primary_window", "Primary window", 10.0, "2026-03-22T12:00:00+00:00"),
                        _percent_metric("secondary_window", "Secondary window", 35.0, "2026-03-29T12:00:00+00:00"),
                    ],
                ),
            ]
        )

        db = self.make_db()
        service = UsageMonitorService(db, _DummySecretBox())
        _enable_provider(db, "codex")

        async def fake_collect_usage(config, secret_value):
            return CollectionResult(snapshot=snapshots.popleft())

        with _patched_collect(fake_collect_usage):
            initial = await service.run_once(provider="codex")

            old_recorded_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            with db.connect() as conn:
                conn.execute(
                    """
                    UPDATE current_metrics
                    SET recorded_at = ?
                    WHERE provider = ? AND metric_key = ?
                    """,
                    (old_recorded_at, "codex", "primary_window"),
                )

            heartbeat = await service.run_once(provider="codex")

        self.assertEqual(initial[0]["reason"], "initial")
        self.assertEqual(heartbeat[0]["reason"], "heartbeat")

        with db.connect() as conn:
            latest_event_rows = conn.execute(
                """
                SELECT metric_key
                FROM metric_samples
                WHERE event_id = (SELECT MAX(id) FROM snapshots WHERE provider = 'codex')
                ORDER BY metric_key
                """
            ).fetchall()
            current_rows = conn.execute(
                """
                SELECT metric_key, recorded_at
                FROM current_metrics
                WHERE provider = 'codex'
                ORDER BY metric_key
                """
            ).fetchall()

        self.assertEqual([row[0] for row in latest_event_rows], ["primary_window"])
        current_by_key = {row[0]: row[1] for row in current_rows}
        self.assertEqual(current_by_key["primary_window"], snapshots[0].recorded_at if snapshots else (base + timedelta(minutes=1)).isoformat())
        self.assertEqual(current_by_key["secondary_window"], base.isoformat())

    async def test_auto_refresh_interval_steps_up_after_each_ten_minutes_unchanged(self) -> None:
        base = datetime.now(timezone.utc)
        snapshots = deque(
            [
                _snapshot(
                    provider="codex",
                    recorded_at=base.isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, "2026-03-24T12:00:00+00:00")],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=10)).isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, "2026-03-24T12:00:00+00:00")],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=20)).isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, "2026-03-24T12:00:00+00:00")],
                ),
            ]
        )

        db = self.make_db()
        service = UsageMonitorService(db, _DummySecretBox())
        _enable_provider(db, "codex")

        async def fake_collect_usage(config, secret_value):
            return CollectionResult(snapshot=snapshots.popleft())

        with _patched_collect(fake_collect_usage):
            await service.run_once(provider="codex")
            await service.run_once(provider="codex")
            await service.run_once(provider="codex")

        state = db.get_provider_state("codex") or {}
        self.assertEqual(state["refresh_mode"], "auto")
        self.assertEqual(state["current_poll_interval_seconds"], 180)
        self.assertEqual(state["unchanged_since_at"], (base + timedelta(minutes=20)).isoformat())

    async def test_changed_snapshot_resets_auto_refresh_interval_to_one_minute(self) -> None:
        base = datetime.now(timezone.utc)
        snapshots = deque(
            [
                _snapshot(
                    provider="codex",
                    recorded_at=base.isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, "2026-03-24T12:00:00+00:00")],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=10)).isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, "2026-03-24T12:00:00+00:00")],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=20)).isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, "2026-03-24T12:00:00+00:00")],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=21)).isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 18.0, "2026-03-24T12:00:00+00:00")],
                ),
            ]
        )

        db = self.make_db()
        service = UsageMonitorService(db, _DummySecretBox())
        _enable_provider(db, "codex")

        async def fake_collect_usage(config, secret_value):
            return CollectionResult(snapshot=snapshots.popleft())

        with _patched_collect(fake_collect_usage):
            await service.run_once(provider="codex")
            await service.run_once(provider="codex")
            await service.run_once(provider="codex")
            changed = await service.run_once(provider="codex")

        self.assertEqual(changed[0]["reason"], "changed")
        state = db.get_provider_state("codex") or {}
        self.assertEqual(state["current_poll_interval_seconds"], 60)
        self.assertIsNone(state["unchanged_since_at"])

    async def test_next_check_uses_earlier_reset_boundary_when_fixed_interval_is_longer(self) -> None:
        base = datetime.now(timezone.utc)
        reset_at = base + timedelta(minutes=2)
        snapshots = deque(
            [
                _snapshot(
                    provider="codex",
                    recorded_at=base.isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, reset_at.isoformat())],
                ),
                _snapshot(
                    provider="codex",
                    recorded_at=(base + timedelta(minutes=1)).isoformat(),
                    metrics=[_percent_metric("primary_window", "Primary window", 10.0, reset_at.isoformat())],
                ),
            ]
        )

        db = self.make_db()
        service = UsageMonitorService(db, _DummySecretBox())
        _enable_provider(db, "codex")

        async def fake_collect_usage(config, secret_value):
            return CollectionResult(snapshot=snapshots.popleft())

        with _patched_collect(fake_collect_usage):
            await service.run_once(provider="codex")
            db.update_provider_schedule(
                "codex",
                refresh_mode="10m",
                current_poll_interval_seconds=600,
                next_check_at=None,
                unchanged_since_at=None,
            )
            await service.run_once(provider="codex")

        state = db.get_provider_state("codex") or {}
        self.assertEqual(state["refresh_mode"], "10m")
        self.assertEqual(state["current_poll_interval_seconds"], 600)
        self.assertEqual(
            state["next_check_at"],
            (reset_at + timedelta(seconds=3)).isoformat(),
        )

def _enable_provider(db: Database, provider: str) -> None:
    db.update_provider_config(
        provider=provider,
        enabled=True,
        collector_type="json_api",
        credential_type="cookie_header",
        usage_url="https://example.com/usage",
        headers_json="{}",
        notes="",
        secret_blob="cookie=1",
    )


def _percent_metric(key: str, label: str, percent: float, resets_at: str) -> UsageMetric:
    return UsageMetric(
        key=key,
        label=label,
        percent_value=percent,
        unit="percent",
        resets_at=resets_at,
    )


def _snapshot(
    *,
    provider: str,
    recorded_at: str,
    metrics: list[UsageMetric],
    plan_name: str | None = "Pro",
    summary: str = "usage",
    raw_text: str = "{}",
    normalized: dict | None = None,
) -> UsageSnapshot:
    return UsageSnapshot(
        provider=provider,
        recorded_at=recorded_at,
        page_title=f"{provider.title()} usage API",
        plan_name=plan_name,
        summary=summary,
        raw_text=raw_text,
        normalized=normalized or {},
        capture={"kind": "json_api"},
        metrics=metrics,
    )


class _DummySecretBox:
    def open(self, value: str | None) -> str:
        return value or "cookie=1"

    def seal(self, value: str) -> str:
        return value


class _patched_collect:
    def __init__(self, replacement):
        self.replacement = replacement
        self.original = service_module.collect_usage

    def __enter__(self):
        service_module.collect_usage = self.replacement
        return self

    def __exit__(self, exc_type, exc, tb):
        service_module.collect_usage = self.original
        return False


if __name__ == "__main__":
    unittest.main()
