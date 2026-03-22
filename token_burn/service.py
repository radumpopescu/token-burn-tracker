"""Background polling service and snapshot persistence logic."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .collectors import collect_usage
from .crypto import SecretBox
from .db import DEFAULT_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_POLL_INTERVAL_SECONDS, Database


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
            for config in self.db.list_provider_configs():
                if provider != "all" and config.provider != provider:
                    continue
                if not config.enabled:
                    continue

                try:
                    secret_value = self.secret_box.open(config.secret_blob)
                    if not secret_value:
                        raise RuntimeError("No credentials have been stored for this provider.")

                    collection = await collect_usage(config, secret_value)
                    snapshot = collection.snapshot
                    if collection.updated_secret_value and collection.updated_secret_value != secret_value:
                        self.db.update_provider_secret(
                            config.provider,
                            self.secret_box.seal(collection.updated_secret_value),
                        )
                    state = self.db.get_provider_state(config.provider) or {}
                    last_hash = state.get("last_hash")
                    last_snapshot_at = state.get("last_snapshot_at")
                    snapshot_hash = snapshot.snapshot_hash()

                    reason: str | None = None
                    if not last_hash:
                        reason = "initial"
                    elif snapshot_hash != last_hash:
                        reason = "changed"
                    elif self._heartbeat_due(last_snapshot_at):
                        reason = "heartbeat"

                    recorded_snapshot_at = None
                    if reason:
                        self.db.insert_snapshot(snapshot, reason)
                        recorded_snapshot_at = snapshot.recorded_at

                    self.db.record_success(
                        config.provider,
                        snapshot_hash=snapshot_hash,
                        summary=snapshot.summary,
                        recorded_snapshot_at=recorded_snapshot_at,
                    )
                    results.append(
                        {
                            "provider": config.provider,
                            "ok": True,
                            "reason": reason or "unchanged",
                            "summary": snapshot.summary,
                        }
                    )
                except Exception as exc:
                    self.db.record_failure(config.provider, str(exc))
                    results.append(
                        {
                            "provider": config.provider,
                            "ok": False,
                            "reason": "error",
                            "summary": str(exc),
                        }
                    )
            return results

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except Exception:
                pass

            interval = self._poll_interval_seconds()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    def _poll_interval_seconds(self) -> int:
        settings = self.db.get_app_settings()
        raw_value = settings.get("poll_interval_seconds", str(DEFAULT_POLL_INTERVAL_SECONDS))
        return max(DEFAULT_POLL_INTERVAL_SECONDS, int(raw_value))

    def _heartbeat_interval_seconds(self) -> int:
        settings = self.db.get_app_settings()
        raw_value = settings.get("heartbeat_interval_seconds", str(DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
        return max(300, int(raw_value))

    def _heartbeat_due(self, last_snapshot_at: str | None) -> bool:
        if not last_snapshot_at:
            return True
        last_seen = datetime.fromisoformat(last_snapshot_at)
        elapsed = datetime.now(timezone.utc) - last_seen
        return elapsed.total_seconds() >= self._heartbeat_interval_seconds()
