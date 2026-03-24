"""Background polling service and snapshot persistence logic."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from typing import Any

from .collectors import collect_usage
from .crypto import SecretBox
from .db import (
    DEFAULT_AUTO_REFRESH_EQUAL_POLLS_BEFORE_STEP,
    DEFAULT_AUTO_REFRESH_STEP_SECONDS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_SLOW_REFRESH_INTERVAL_SECONDS,
    Database,
)
from .models import UsageMetric

RESET_REFRESH_BUFFER_SECONDS = 3
FIXED_REFRESH_MODES = ("1m", "10m")


class UsageMonitorService:
    def __init__(self, db: Database, secret_box: SecretBox):
        self.db = db
        self.secret_box = secret_box
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            await self._task

    async def run_once(self, provider: str = "all") -> list[dict[str, Any]]:
        async with self._lock:
            results: list[dict[str, Any]] = []
            for config in self._target_configs(provider):
                results.append(await self._run_provider(config, force=True))
            return results

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                due_configs = self._due_configs()
                if due_configs:
                    async with self._lock:
                        for config in due_configs:
                            await self._run_provider(config, force=False)
                    continue
            except Exception:
                pass

            interval = self._seconds_until_next_due()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def set_refresh_mode(self, provider: str, mode: str) -> dict[str, Any]:
        normalized_mode = self._normalize_refresh_mode(mode)
        if normalized_mode not in {"auto", *FIXED_REFRESH_MODES}:
            raise ValueError(f"Unsupported refresh mode: {mode}")
        async with self._lock:
            state = self.db.get_provider_state(provider) or {}
            current_metrics = self.db.get_current_metrics(provider)
            checked_at = datetime.now(timezone.utc)
            refresh_settings = self._refresh_settings()
            interval = (
                refresh_settings["fast_interval_seconds"]
                if normalized_mode == "auto"
                else self._fixed_refresh_interval_seconds(normalized_mode, refresh_settings)
            )
            next_check_at = self._compute_next_check_at(
                checked_at=checked_at,
                interval_seconds=interval,
                metrics=current_metrics.values(),
            )
            self.db.update_provider_schedule(
                provider,
                refresh_mode=normalized_mode,
                current_poll_interval_seconds=interval,
                next_check_at=next_check_at,
                unchanged_since_at=None,
                unchanged_poll_count=0,
            )
            return self.db.get_provider_state(provider) or state

    def _heartbeat_interval_seconds(self) -> int:
        settings = self.db.get_app_settings()
        raw_value = settings.get("heartbeat_interval_seconds", str(DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
        return max(300, int(raw_value))

    def _refresh_settings(self) -> dict[str, int]:
        settings = self.db.get_app_settings()
        fast_interval_seconds = self._read_int_setting(
            settings,
            "poll_interval_seconds",
            default=DEFAULT_POLL_INTERVAL_SECONDS,
            minimum=60,
        )
        slow_interval_seconds = self._read_int_setting(
            settings,
            "slow_refresh_interval_seconds",
            default=DEFAULT_SLOW_REFRESH_INTERVAL_SECONDS,
            minimum=fast_interval_seconds,
        )
        auto_step_seconds = self._read_int_setting(
            settings,
            "auto_refresh_step_seconds",
            default=DEFAULT_AUTO_REFRESH_STEP_SECONDS,
            minimum=60,
        )
        unchanged_polls_before_step = self._read_int_setting(
            settings,
            "auto_refresh_equal_polls_before_step",
            default=DEFAULT_AUTO_REFRESH_EQUAL_POLLS_BEFORE_STEP,
            minimum=1,
        )
        return {
            "fast_interval_seconds": fast_interval_seconds,
            "slow_interval_seconds": max(fast_interval_seconds, slow_interval_seconds),
            "auto_step_seconds": auto_step_seconds,
            "unchanged_polls_before_step": unchanged_polls_before_step,
        }

    def _read_int_setting(
        self,
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

    def _metric_heartbeat_due(self, recorded_at: str | None) -> bool:
        if not recorded_at:
            return True
        last_seen = datetime.fromisoformat(recorded_at)
        elapsed = datetime.now(timezone.utc) - last_seen
        return elapsed.total_seconds() >= self._heartbeat_interval_seconds()

    def _metrics_to_persist(
        self,
        metrics: list[UsageMetric],
        current_metrics: dict[str, dict[str, Any]],
        *,
        recorded_at: str,
    ) -> list[UsageMetric]:
        metrics_to_persist: list[UsageMetric] = []
        for metric in metrics:
            previous = current_metrics.get(metric.key)
            if previous is None:
                metrics_to_persist.append(metric)
                continue
            if self._metric_changed(metric, previous, recorded_at=recorded_at):
                metrics_to_persist.append(metric)
                continue
            if self._metric_heartbeat_due(previous.get("recorded_at")):
                metrics_to_persist.append(metric)
        return metrics_to_persist

    def _metric_changed(self, metric: UsageMetric, previous: dict[str, Any], *, recorded_at: str) -> bool:
        previous_metric = UsageMetric(
            key=str(previous.get("metric_key") or metric.key),
            label=str(previous.get("label") or metric.label),
            percent_value=previous.get("percent_value"),
            used_value=previous.get("used_value"),
            limit_value=previous.get("limit_value"),
            unit=previous.get("unit"),
            resets_at=previous.get("window_ends_at"),
            extra=self._normalize_extra(previous.get("stable_extra")),
        )
        return metric.canonical_value(recorded_at) != previous_metric.canonical_value(previous.get("recorded_at"))

    def _normalize_extra(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value:
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    async def _run_provider(self, config, *, force: bool) -> dict[str, Any]:
        state = self.db.get_provider_state(config.provider) or {}
        current_metrics = self.db.get_current_metrics(config.provider)
        checked_at = datetime.now(timezone.utc)
        refresh_mode = self._refresh_mode(state)
        refresh_settings = self._refresh_settings()
        current_interval_seconds = self._current_interval_seconds(state, refresh_settings)
        unchanged_since_at = state.get("unchanged_since_at")
        unchanged_poll_count = self._unchanged_poll_count(state)

        if not force and not self._provider_is_due(state, checked_at):
            return {
                "provider": config.provider,
                "ok": True,
                "reason": "skipped",
                "summary": "Not due yet",
            }

        try:
            secret_value = self.secret_box.open(config.secret_blob)
            if not secret_value:
                raise RuntimeError("No credentials have been stored for this provider.")

            collection = await collect_usage(config, secret_value)
            snapshot = collection.snapshot
            checked_at = self._safe_datetime(snapshot.recorded_at) or checked_at
            if collection.updated_secret_value and collection.updated_secret_value != secret_value:
                self.db.update_provider_secret(
                    config.provider,
                    self.secret_box.seal(collection.updated_secret_value),
                )
            previous_event = self.db.get_latest_snapshot(config.provider) or {}
            state_hash = snapshot.state_hash()
            last_state_hash = state.get("last_hash")
            metrics_to_persist = self._metrics_to_persist(
                snapshot.metrics,
                current_metrics,
                recorded_at=snapshot.recorded_at,
            )
            metadata_changed = previous_event.get("plan_name") != snapshot.plan_name
            removed_metrics = set(current_metrics) - {metric.key for metric in snapshot.metrics}

            reason: str | None = None
            if not last_state_hash:
                reason = "initial"
                metrics_to_persist = list(snapshot.metrics)
            elif state_hash != last_state_hash or metadata_changed or removed_metrics:
                reason = "changed"
            elif metrics_to_persist:
                reason = "heartbeat"

            recorded_snapshot_at = None
            event_id: int | None = None
            if reason:
                event_id = self.db.persist_provider_event(
                    snapshot,
                    reason=reason,
                    state_hash=state_hash,
                    metrics_to_persist=metrics_to_persist,
                )
                recorded_snapshot_at = snapshot.recorded_at

            self.db.sync_current_metrics(
                snapshot,
                event_id=event_id,
                persisted_metric_keys={metric.key for metric in metrics_to_persist},
            )

            (
                current_interval_seconds,
                unchanged_since_at,
                unchanged_poll_count,
            ) = self._next_refresh_strategy(
                refresh_settings=refresh_settings,
                refresh_mode=refresh_mode,
                current_interval_seconds=current_interval_seconds,
                unchanged_since_at=unchanged_since_at,
                unchanged_poll_count=unchanged_poll_count,
                last_checked_at=state.get("last_checked_at"),
                checked_at=checked_at,
                reason=reason,
            )
            next_check_at = self._compute_next_check_at(
                checked_at=checked_at,
                interval_seconds=current_interval_seconds,
                metrics=snapshot.metrics,
            )

            self.db.record_success(
                config.provider,
                checked_at=checked_at.isoformat(),
                state_hash=state_hash,
                summary=snapshot.summary,
                recorded_snapshot_at=recorded_snapshot_at,
                next_check_at=next_check_at,
                current_poll_interval_seconds=current_interval_seconds,
                refresh_mode=refresh_mode,
                unchanged_since_at=unchanged_since_at,
                unchanged_poll_count=unchanged_poll_count,
            )
            return {
                "provider": config.provider,
                "ok": True,
                "reason": reason or "unchanged",
                "summary": snapshot.summary,
            }
        except Exception as exc:
            next_check_at = self._compute_next_check_at(
                checked_at=checked_at,
                interval_seconds=current_interval_seconds,
                metrics=current_metrics.values(),
            )
            self.db.record_failure(
                config.provider,
                str(exc),
                checked_at=checked_at.isoformat(),
                next_check_at=next_check_at,
                current_poll_interval_seconds=current_interval_seconds,
                refresh_mode=refresh_mode,
                unchanged_since_at=unchanged_since_at,
                unchanged_poll_count=unchanged_poll_count,
            )
            return {
                "provider": config.provider,
                "ok": False,
                "reason": "error",
                "summary": str(exc),
            }

    def _target_configs(self, provider: str) -> list[Any]:
        configs = []
        for config in self.db.list_provider_configs():
            if provider != "all" and config.provider != provider:
                continue
            if not config.enabled:
                continue
            configs.append(config)
        return configs

    def _due_configs(self) -> list[Any]:
        now = datetime.now(timezone.utc)
        states = self.db.get_provider_states()
        due: list[Any] = []
        for config in self._target_configs("all"):
            if self._provider_is_due(states.get(config.provider) or {}, now):
                due.append(config)
        return due

    def _seconds_until_next_due(self) -> float:
        now = datetime.now(timezone.utc)
        delays: list[float] = []
        states = self.db.get_provider_states()
        for config in self._target_configs("all"):
            state = states.get(config.provider) or {}
            next_check = self._safe_datetime(state.get("next_check_at"))
            if next_check is None:
                return 0.5
            delays.append(max(0.5, (next_check - now).total_seconds()))
        return min(delays) if delays else 30.0

    def _provider_is_due(self, state: dict[str, Any], now: datetime) -> bool:
        next_check = self._safe_datetime(state.get("next_check_at"))
        return next_check is None or next_check <= now

    def _refresh_mode(self, state: dict[str, Any]) -> str:
        return self._normalize_refresh_mode(state.get("refresh_mode"))

    def _normalize_refresh_mode(self, mode: Any) -> str:
        value = str(mode or "auto")
        return value if value in {"auto", *FIXED_REFRESH_MODES} else "auto"

    def _fixed_refresh_interval_seconds(self, mode: str, refresh_settings: dict[str, int]) -> int:
        if mode == "1m":
            return refresh_settings["fast_interval_seconds"]
        if mode == "10m":
            return refresh_settings["slow_interval_seconds"]
        return refresh_settings["fast_interval_seconds"]

    def _current_interval_seconds(self, state: dict[str, Any], refresh_settings: dict[str, int]) -> int:
        raw_value = state.get("current_poll_interval_seconds")
        try:
            interval = int(raw_value)
        except (TypeError, ValueError):
            interval = refresh_settings["fast_interval_seconds"]
        return max(
            refresh_settings["fast_interval_seconds"],
            min(refresh_settings["slow_interval_seconds"], interval),
        )

    def _unchanged_poll_count(self, state: dict[str, Any]) -> int:
        raw_value = state.get("unchanged_poll_count")
        try:
            count = int(raw_value)
        except (TypeError, ValueError):
            count = 0
        return max(0, count)

    def _next_refresh_strategy(
        self,
        *,
        refresh_settings: dict[str, int],
        refresh_mode: str,
        current_interval_seconds: int,
        unchanged_since_at: str | None,
        unchanged_poll_count: int,
        last_checked_at: str | None,
        checked_at: datetime,
        reason: str | None,
    ) -> tuple[int, str | None, int]:
        if refresh_mode in FIXED_REFRESH_MODES:
            return self._fixed_refresh_interval_seconds(refresh_mode, refresh_settings), None, 0

        if reason in {"initial", "changed"}:
            return refresh_settings["fast_interval_seconds"], None, 0

        stable_since = self._safe_datetime(unchanged_since_at) or self._safe_datetime(last_checked_at) or checked_at
        next_equal_count = unchanged_poll_count + 1
        if next_equal_count >= refresh_settings["unchanged_polls_before_step"]:
            next_interval = min(
                current_interval_seconds + refresh_settings["auto_step_seconds"],
                refresh_settings["slow_interval_seconds"],
            )
            return next_interval, checked_at.isoformat(), 0
        return current_interval_seconds, stable_since.isoformat(), next_equal_count

    def _compute_next_check_at(
        self,
        *,
        checked_at: datetime,
        interval_seconds: int,
        metrics: Any,
    ) -> str:
        next_check = checked_at + timedelta(seconds=interval_seconds)
        reset_candidates: list[datetime] = []
        for metric in metrics:
            window_ends_at = self._metric_window_end_at(metric)
            if not window_ends_at:
                continue
            reset_at = self._safe_datetime(window_ends_at)
            if reset_at is None:
                continue
            reset_ready_at = reset_at + timedelta(seconds=RESET_REFRESH_BUFFER_SECONDS)
            if reset_ready_at > checked_at:
                reset_candidates.append(reset_ready_at)
        if reset_candidates:
            next_check = min(next_check, min(reset_candidates))
        return next_check.isoformat()

    def _metric_window_end_at(self, metric: Any) -> str | None:
        if isinstance(metric, UsageMetric):
            return metric.window_ends_at
        if isinstance(metric, dict):
            value = metric.get("window_ends_at")
            if value:
                return str(value)
            resets_at = metric.get("resets_at")
            if resets_at:
                return str(resets_at)
        return None

    def _safe_datetime(self, value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None
