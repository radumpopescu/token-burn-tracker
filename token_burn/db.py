"""SQLite access helpers for config, state, snapshots, and chart queries."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from .models import ProviderConfig, UsageMetric, UsageSnapshot
from .providers import provider_choices

DB_SENTINEL = object()
DEFAULT_POLL_INTERVAL_SECONDS = 60
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 3600
RAW_PAYLOAD_LOG_RETENTION = timedelta(days=1)
EMPTY_JSON = "{}"


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.raw_payload_dir = self.path.parent / "raw-payloads"

    @contextmanager
    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_configs (
                    provider TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    collector_type TEXT NOT NULL,
                    credential_type TEXT NOT NULL,
                    usage_url TEXT NOT NULL,
                    headers_json TEXT NOT NULL DEFAULT '{}',
                    secret_blob TEXT,
                    notes TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_state (
                    provider TEXT PRIMARY KEY,
                    last_checked_at TEXT,
                    last_success_at TEXT,
                    last_snapshot_at TEXT,
                    last_hash TEXT,
                    last_error TEXT,
                    last_summary TEXT,
                    next_check_at TEXT,
                    current_poll_interval_seconds INTEGER NOT NULL DEFAULT 60,
                    refresh_mode TEXT NOT NULL DEFAULT 'auto',
                    unchanged_since_at TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    snapshot_hash TEXT NOT NULL,
                    page_title TEXT,
                    plan_name TEXT,
                    summary TEXT NOT NULL,
                    raw_text TEXT,
                    normalized_json TEXT NOT NULL,
                    capture_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_snapshots_provider_recorded_at
                    ON snapshots(provider, recorded_at);

                CREATE TABLE IF NOT EXISTS metric_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER REFERENCES snapshots(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    metric_key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    window_ends_at TEXT,
                    recorded_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    percent_value REAL,
                    used_value REAL,
                    limit_value REAL,
                    unit TEXT,
                    stable_extra_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_metric_samples_provider_key_recorded_at
                    ON metric_samples(provider, metric_key, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_metric_samples_provider_recorded_at
                    ON metric_samples(provider, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_metric_samples_provider_key_window_recorded_at
                    ON metric_samples(provider, metric_key, window_ends_at, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_metric_samples_event_id
                    ON metric_samples(event_id);

                CREATE TABLE IF NOT EXISTS current_metrics (
                    provider TEXT NOT NULL,
                    metric_key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    window_ends_at TEXT,
                    recorded_at TEXT NOT NULL,
                    percent_value REAL,
                    used_value REAL,
                    limit_value REAL,
                    unit TEXT,
                    stable_extra_json TEXT NOT NULL DEFAULT '{}',
                    event_id INTEGER REFERENCES snapshots(id) ON DELETE SET NULL,
                    PRIMARY KEY (provider, metric_key)
                );
                CREATE INDEX IF NOT EXISTS idx_current_metrics_provider_recorded_at
                    ON current_metrics(provider, recorded_at);
                """
            )

            _ensure_column(conn, "provider_state", "next_check_at", "TEXT")
            _ensure_column(
                conn,
                "provider_state",
                "current_poll_interval_seconds",
                f"INTEGER NOT NULL DEFAULT {DEFAULT_POLL_INTERVAL_SECONDS}",
            )
            _ensure_column(conn, "provider_state", "refresh_mode", "TEXT NOT NULL DEFAULT 'auto'")
            _ensure_column(conn, "provider_state", "unchanged_since_at", "TEXT")

            now = _utcnow()
            for key, value in (
                ("poll_interval_seconds", str(DEFAULT_POLL_INTERVAL_SECONDS)),
                ("heartbeat_interval_seconds", str(DEFAULT_HEARTBEAT_INTERVAL_SECONDS)),
                ("dashboard_top_provider", "codex"),
            ):
                conn.execute(
                    """
                    INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
                    ON CONFLICT(key) DO NOTHING
                    """,
                    (key, value, now),
                )

            for spec in provider_choices():
                conn.execute(
                    """
                    INSERT INTO provider_configs (
                        provider,
                        display_name,
                        enabled,
                        collector_type,
                        credential_type,
                        usage_url,
                        headers_json,
                        notes,
                        updated_at
                    ) VALUES (?, ?, 0, ?, ?, ?, '{}', '', ?)
                    ON CONFLICT(provider) DO NOTHING
                    """,
                    (
                        spec["provider"],
                        spec["display_name"],
                        spec["default_collector_type"],
                        spec["default_credential_type"],
                        spec["default_usage_url"],
                        now,
                    ),
                )

                conn.execute(
                    """
                    INSERT INTO provider_state (provider, consecutive_failures, updated_at)
                    VALUES (?, 0, ?)
                    ON CONFLICT(provider) DO NOTHING
                    """,
                    (spec["provider"], now),
                )

        self.compact_snapshot_payload_storage()

    def get_app_settings(self) -> dict[str, str]:
        with self.connect() as conn:
            rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
        return {row["key"]: row["value"] for row in rows}

    def update_app_setting(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, _utcnow()),
            )

    def list_distinct_metrics(self) -> list[dict[str, str]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT provider, metric_key, label, recorded_at
                FROM current_metrics
                UNION ALL
                SELECT provider, metric_key, label, recorded_at
                FROM metric_samples
                ORDER BY provider, metric_key, recorded_at DESC
                """
            ).fetchall()
        metrics: dict[tuple[str, str], dict[str, str]] = {}
        for row in rows:
            key = (row["provider"], row["metric_key"])
            metrics.setdefault(
                key,
                {
                    "provider": row["provider"],
                    "key": row["metric_key"],
                    "label": row["label"],
                },
            )
        return list(metrics.values())

    def list_provider_configs(self) -> list[ProviderConfig]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM provider_configs ORDER BY provider").fetchall()
        return [_row_to_config(row) for row in rows]

    def get_provider_config(self, provider: str) -> ProviderConfig:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM provider_configs WHERE provider = ?", (provider,)).fetchone()
        if row is None:
            raise KeyError(provider)
        return _row_to_config(row)

    def update_provider_config(
        self,
        *,
        provider: str,
        enabled: bool,
        collector_type: str,
        credential_type: str,
        usage_url: str,
        headers_json: str,
        notes: str,
        secret_blob: str | object = DB_SENTINEL,
    ) -> None:
        assignments = [
            "enabled = ?",
            "collector_type = ?",
            "credential_type = ?",
            "usage_url = ?",
            "headers_json = ?",
            "notes = ?",
            "updated_at = ?",
        ]
        params: list[Any] = [
            int(enabled),
            collector_type,
            credential_type,
            usage_url,
            headers_json,
            notes,
            _utcnow(),
        ]
        if secret_blob is not DB_SENTINEL:
            assignments.append("secret_blob = ?")
            params.append(secret_blob)
        params.append(provider)

        with self.connect() as conn:
            conn.execute(
                f"UPDATE provider_configs SET {', '.join(assignments)} WHERE provider = ?",
                params,
            )

    def update_provider_secret(self, provider: str, secret_blob: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE provider_configs
                SET secret_blob = ?, updated_at = ?
                WHERE provider = ?
                """,
                (secret_blob, _utcnow(), provider),
            )

    def get_provider_states(self) -> dict[str, dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM provider_state ORDER BY provider").fetchall()
        return {row["provider"]: dict(row) for row in rows}

    def get_latest_snapshot(self, provider: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM snapshots WHERE provider = ? ORDER BY id DESC LIMIT 1",
                (provider,),
            ).fetchone()
        return dict(row) if row else None

    def get_current_metrics(self, provider: str) -> dict[str, dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT provider, metric_key, label, window_ends_at, recorded_at,
                       percent_value, used_value, limit_value, unit, stable_extra_json
                FROM current_metrics
                WHERE provider = ?
                ORDER BY metric_key
                """,
                (provider,),
            ).fetchall()
        return {
            row["metric_key"]: {
                **{
                    key: value
                    for key, value in dict(row).items()
                    if key != "stable_extra_json"
                },
                "stable_extra": json.loads(row["stable_extra_json"] or "{}"),
            }
            for row in rows
        }

    def record_success(
        self,
        provider: str,
        *,
        checked_at: str,
        state_hash: str,
        summary: str,
        recorded_snapshot_at: str | None,
        next_check_at: str | None,
        current_poll_interval_seconds: int,
        refresh_mode: str,
        unchanged_since_at: str | None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO provider_state (
                    provider,
                    last_checked_at,
                    last_success_at,
                    last_snapshot_at,
                    last_hash,
                    last_error,
                    last_summary,
                    next_check_at,
                    current_poll_interval_seconds,
                    refresh_mode,
                    unchanged_since_at,
                    consecutive_failures,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    last_checked_at=excluded.last_checked_at,
                    last_success_at=excluded.last_success_at,
                    last_snapshot_at=COALESCE(excluded.last_snapshot_at, provider_state.last_snapshot_at),
                    last_hash=excluded.last_hash,
                    last_error=NULL,
                    last_summary=excluded.last_summary,
                    next_check_at=excluded.next_check_at,
                    current_poll_interval_seconds=excluded.current_poll_interval_seconds,
                    refresh_mode=excluded.refresh_mode,
                    unchanged_since_at=excluded.unchanged_since_at,
                    consecutive_failures=0,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    checked_at,
                    checked_at,
                    recorded_snapshot_at,
                    state_hash,
                    summary,
                    next_check_at,
                    current_poll_interval_seconds,
                    refresh_mode,
                    unchanged_since_at,
                    _utcnow(),
                ),
            )

    def record_failure(
        self,
        provider: str,
        error: str,
        *,
        checked_at: str,
        next_check_at: str | None,
        current_poll_interval_seconds: int,
        refresh_mode: str,
        unchanged_since_at: str | None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO provider_state (
                    provider,
                    last_checked_at,
                    last_error,
                    next_check_at,
                    current_poll_interval_seconds,
                    refresh_mode,
                    unchanged_since_at,
                    consecutive_failures,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    last_checked_at=excluded.last_checked_at,
                    last_error=excluded.last_error,
                    next_check_at=excluded.next_check_at,
                    current_poll_interval_seconds=excluded.current_poll_interval_seconds,
                    refresh_mode=excluded.refresh_mode,
                    unchanged_since_at=excluded.unchanged_since_at,
                    consecutive_failures=provider_state.consecutive_failures + 1,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    checked_at,
                    error[:500],
                    next_check_at,
                    current_poll_interval_seconds,
                    refresh_mode,
                    unchanged_since_at,
                    _utcnow(),
                ),
            )

    def update_provider_schedule(
        self,
        provider: str,
        *,
        refresh_mode: str,
        current_poll_interval_seconds: int,
        next_check_at: str | None,
        unchanged_since_at: str | None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO provider_state (
                    provider,
                    refresh_mode,
                    current_poll_interval_seconds,
                    next_check_at,
                    unchanged_since_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    refresh_mode=excluded.refresh_mode,
                    current_poll_interval_seconds=excluded.current_poll_interval_seconds,
                    next_check_at=excluded.next_check_at,
                    unchanged_since_at=excluded.unchanged_since_at,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    refresh_mode,
                    current_poll_interval_seconds,
                    next_check_at,
                    unchanged_since_at,
                    _utcnow(),
                ),
            )

    def sync_current_metrics(
        self,
        snapshot: UsageSnapshot,
        *,
        event_id: int | None,
        persisted_metric_keys: set[str],
    ) -> None:
        event_ids_by_metric_key = {
            metric.key: event_id if metric.key in persisted_metric_keys else None
            for metric in snapshot.metrics
        }
        with self.connect() as conn:
            self._upsert_current_metrics(
                conn,
                snapshot.provider,
                snapshot.recorded_at,
                snapshot.metrics,
                event_ids_by_metric_key,
            )
            self._delete_missing_current_metrics(
                conn,
                snapshot.provider,
                {metric.key for metric in snapshot.metrics},
            )

    def insert_snapshot(self, snapshot: UsageSnapshot, reason: str, state_hash: str) -> int:
        with self.connect() as conn:
            return self._insert_snapshot(conn, snapshot, reason, state_hash)

    def persist_provider_event(
        self,
        snapshot: UsageSnapshot,
        *,
        reason: str,
        state_hash: str,
        metrics_to_persist: list[UsageMetric],
    ) -> int:
        with self.connect() as conn:
            event_id = self._insert_snapshot(conn, snapshot, reason, state_hash)
            self._insert_metric_samples(conn, snapshot.provider, snapshot.recorded_at, event_id, reason, metrics_to_persist)
        self._append_raw_payload_log(snapshot, reason=reason, state_hash=state_hash)
        return event_id

    def latest_snapshots_by_provider(self) -> dict[str, dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT s.*
                FROM snapshots s
                JOIN (
                    SELECT provider, MAX(id) AS max_id
                    FROM snapshots
                    GROUP BY provider
                ) latest
                    ON latest.provider = s.provider
                   AND latest.max_id = s.id
                ORDER BY s.provider
                """
            ).fetchall()
            metrics = self._current_metrics_by_provider(conn, [row["provider"] for row in rows])

        return {
            row["provider"]: {
                **dict(row),
                "metrics": metrics.get(row["provider"], []),
            }
            for row in rows
        }

    def list_snapshots(
        self,
        *,
        provider: str,
        start_at: str | None,
        end_at: str | None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where = ["1 = 1"]
        params: list[Any] = []
        if provider != "all":
            where.append("provider = ?")
            params.append(provider)
        if start_at:
            where.append("recorded_at >= ?")
            params.append(start_at)
        if end_at:
            where.append("recorded_at <= ?")
            params.append(end_at)

        query = f"""
            SELECT *
            FROM snapshots
            WHERE {' AND '.join(where)}
            ORDER BY recorded_at DESC
            LIMIT ?
        """
        params.append(limit)

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
            metrics = self._metrics_by_event(conn, [row["id"] for row in rows])

        return [
            {
                **dict(row),
                "metrics": metrics.get(row["id"], []),
            }
            for row in rows
        ]

    def list_metric_series(
        self,
        *,
        provider: str,
        start_at: str | None,
        end_at: str | None,
    ) -> list[dict[str, Any]]:
        where = ["1 = 1"]
        params: list[Any] = []
        if provider != "all":
            where.append("ms.provider = ?")
            params.append(provider)
        if start_at:
            where.append("ms.recorded_at >= ?")
            params.append(start_at)
        if end_at:
            where.append("ms.recorded_at <= ?")
            params.append(end_at)

        query = f"""
            SELECT
                ms.provider,
                ms.metric_key,
                ms.label,
                ms.recorded_at,
                ms.percent_value,
                ms.used_value,
                ms.limit_value,
                ms.unit
            FROM metric_samples ms
            WHERE {' AND '.join(where)}
            ORDER BY ms.provider, ms.metric_key, ms.recorded_at
        """

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()

        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            key = (row["provider"], row["metric_key"])
            series = grouped.setdefault(
                key,
                {
                    "provider": row["provider"],
                    "metric_key": row["metric_key"],
                    "label": row["label"],
                    "unit": row["unit"],
                    "points": [],
                },
            )
            series["label"] = row["label"]
            series["points"].append(
                {
                    "recorded_at": row["recorded_at"],
                    "percent_value": row["percent_value"],
                    "used_value": row["used_value"],
                    "limit_value": row["limit_value"],
                }
            )
        return list(grouped.values())

    def list_change_counts(
        self,
        *,
        provider: str,
        start_at: str | None,
        end_at: str | None,
    ) -> list[dict[str, Any]]:
        where = ["1 = 1"]
        params: list[Any] = []
        if provider != "all":
            where.append("provider = ?")
            params.append(provider)
        if start_at:
            where.append("recorded_at >= ?")
            params.append(start_at)
        if end_at:
            where.append("recorded_at <= ?")
            params.append(end_at)

        query = f"""
            SELECT
                substr(recorded_at, 1, 10) AS bucket,
                provider,
                COUNT(*) AS change_count
            FROM snapshots
            WHERE {' AND '.join(where)}
            GROUP BY substr(recorded_at, 1, 10), provider
            ORDER BY bucket
        """

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_provider_state(self, provider: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM provider_state WHERE provider = ?", (provider,)).fetchone()
        return dict(row) if row else None

    def _insert_snapshot(
        self,
        conn: sqlite3.Connection,
        snapshot: UsageSnapshot,
        reason: str,
        state_hash: str,
    ) -> int:
        cursor = conn.execute(
            """
            INSERT INTO snapshots (
                provider,
                recorded_at,
                reason,
                snapshot_hash,
                page_title,
                plan_name,
                summary,
                raw_text,
                normalized_json,
                capture_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.provider,
                snapshot.recorded_at,
                reason,
                state_hash,
                snapshot.page_title,
                snapshot.plan_name,
                snapshot.summary,
                None,
                EMPTY_JSON,
                EMPTY_JSON,
            ),
        )
        return int(cursor.lastrowid)

    def _insert_metric_samples(
        self,
        conn: sqlite3.Connection,
        provider: str,
        recorded_at: str,
        event_id: int,
        reason: str,
        metrics: list[UsageMetric],
    ) -> None:
        for metric in metrics:
            conn.execute(
                """
                INSERT INTO metric_samples (
                    event_id,
                    provider,
                    metric_key,
                    label,
                    window_ends_at,
                    recorded_at,
                    reason,
                    percent_value,
                    used_value,
                    limit_value,
                    unit,
                    stable_extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    provider,
                    metric.key,
                    metric.label,
                    metric.window_ends_at,
                    recorded_at,
                    reason,
                    metric.percent_value,
                    metric.used_value,
                    metric.limit_value,
                    metric.unit,
                    json.dumps(metric.stable_extra(), sort_keys=True),
                ),
            )

    def _upsert_current_metrics(
        self,
        conn: sqlite3.Connection,
        provider: str,
        recorded_at: str,
        metrics: list[UsageMetric],
        event_ids_by_metric_key: dict[str, int | None],
    ) -> None:
        for metric in metrics:
            event_id = event_ids_by_metric_key.get(metric.key)
            conn.execute(
                """
                INSERT INTO current_metrics (
                    provider,
                    metric_key,
                    label,
                    window_ends_at,
                    recorded_at,
                    percent_value,
                    used_value,
                    limit_value,
                    unit,
                    stable_extra_json,
                    event_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, metric_key) DO UPDATE SET
                    label=excluded.label,
                    window_ends_at=excluded.window_ends_at,
                    recorded_at=excluded.recorded_at,
                    percent_value=excluded.percent_value,
                    used_value=excluded.used_value,
                    limit_value=excluded.limit_value,
                    unit=excluded.unit,
                    stable_extra_json=excluded.stable_extra_json,
                    event_id=COALESCE(excluded.event_id, current_metrics.event_id)
                """,
                (
                    provider,
                    metric.key,
                    metric.label,
                    metric.window_ends_at,
                    recorded_at,
                    metric.percent_value,
                    metric.used_value,
                    metric.limit_value,
                    metric.unit,
                    json.dumps(metric.stable_extra(), sort_keys=True),
                    event_id,
                ),
            )

    def compact_snapshot_payload_storage(self) -> bool:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE snapshots
                SET raw_text = NULL,
                    normalized_json = ?,
                    capture_json = ?
                WHERE COALESCE(raw_text, '') <> ''
                   OR normalized_json <> ?
                   OR capture_json <> ?
                """,
                (EMPTY_JSON, EMPTY_JSON, EMPTY_JSON, EMPTY_JSON),
            )
            changed = cursor.rowcount > 0
        if changed:
            self.vacuum()
        return changed

    def vacuum(self) -> None:
        conn = sqlite3.connect(str(self.path))
        try:
            conn.execute("VACUUM")
        finally:
            conn.close()

    def _append_raw_payload_log(self, snapshot: UsageSnapshot, *, reason: str, state_hash: str) -> None:
        if not snapshot.raw_text and not snapshot.normalized and not snapshot.capture:
            return
        self.raw_payload_dir.mkdir(parents=True, exist_ok=True)
        recorded_at = _parse_timestamp(snapshot.recorded_at) or datetime.now(timezone.utc)
        log_path = self.raw_payload_dir / f"{snapshot.provider}-{recorded_at.date().isoformat()}.ndjson"
        payload = {
            "provider": snapshot.provider,
            "recorded_at": snapshot.recorded_at,
            "reason": reason,
            "state_hash": state_hash,
            "page_title": snapshot.page_title,
            "plan_name": snapshot.plan_name,
            "summary": snapshot.summary,
            "raw_text": snapshot.raw_text or None,
            "normalized": snapshot.normalized or None,
            "capture": snapshot.capture or None,
        }
        try:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
            self._prune_raw_payload_logs()
        except OSError:
            return

    def _prune_raw_payload_logs(self) -> None:
        if not self.raw_payload_dir.exists():
            return
        cutoff = datetime.now(timezone.utc) - RAW_PAYLOAD_LOG_RETENTION
        for path in self.raw_payload_dir.glob("*.ndjson"):
            try:
                modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
            if modified_at >= cutoff:
                continue
            try:
                path.unlink()
            except OSError:
                continue

    def _delete_missing_current_metrics(
        self,
        conn: sqlite3.Connection,
        provider: str,
        current_metric_keys: set[str],
    ) -> None:
        if current_metric_keys:
            placeholders = ",".join("?" for _ in current_metric_keys)
            conn.execute(
                f"""
                DELETE FROM current_metrics
                WHERE provider = ?
                  AND metric_key NOT IN ({placeholders})
                """,
                [provider, *sorted(current_metric_keys)],
            )
            return
        conn.execute("DELETE FROM current_metrics WHERE provider = ?", (provider,))

    def _current_metrics_by_provider(
        self, conn: sqlite3.Connection, providers: list[str]
    ) -> dict[str, list[dict[str, Any]]]:
        if not providers:
            return {}
        placeholders = ",".join("?" for _ in providers)
        rows = conn.execute(
            f"""
            SELECT provider, metric_key, label, percent_value, used_value,
                   limit_value, unit, window_ends_at, stable_extra_json
            FROM current_metrics
            WHERE provider IN ({placeholders})
            ORDER BY provider, metric_key
            """,
            providers,
        ).fetchall()

        grouped: dict[str, list[dict[str, Any]]] = {provider: [] for provider in providers}
        for row in rows:
            item = dict(row)
            item["resets_at"] = item.pop("window_ends_at")
            item["extra"] = json.loads(item.pop("stable_extra_json") or "{}")
            grouped[row["provider"]].append(item)
        return grouped

    def _metrics_by_event(
        self, conn: sqlite3.Connection, event_ids: list[int]
    ) -> dict[int, list[dict[str, Any]]]:
        if not event_ids:
            return {}
        placeholders = ",".join("?" for _ in event_ids)
        rows = conn.execute(
            f"""
            SELECT event_id, provider, metric_key, label, percent_value, used_value,
                   limit_value, unit, window_ends_at, stable_extra_json
            FROM metric_samples
            WHERE event_id IN ({placeholders})
            ORDER BY id
            """,
            event_ids,
        ).fetchall()

        grouped: dict[int, list[dict[str, Any]]] = {event_id: [] for event_id in event_ids}
        for row in rows:
            item = dict(row)
            item["resets_at"] = item.pop("window_ends_at")
            item["extra"] = json.loads(item.pop("stable_extra_json") or "{}")
            grouped[row["event_id"]].append(item)
        return grouped


def _row_to_config(row: sqlite3.Row) -> ProviderConfig:
    return ProviderConfig(
        provider=row["provider"],
        display_name=row["display_name"],
        enabled=bool(row["enabled"]),
        collector_type=row["collector_type"],
        credential_type=row["credential_type"],
        usage_url=row["usage_url"],
        headers_json=row["headers_json"],
        notes=row["notes"],
        secret_blob=row["secret_blob"],
        updated_at=row["updated_at"],
    )


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
