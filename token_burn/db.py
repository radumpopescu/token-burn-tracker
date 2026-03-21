"""SQLite access helpers for config, state, snapshots, and chart queries."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from .models import ProviderConfig, UsageSnapshot
from .providers import provider_choices

DB_SENTINEL = object()


class Database:
    def __init__(self, path: Path):
        self.path = path

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

    def init_db(self, poll_interval_seconds: int, heartbeat_interval_seconds: int) -> None:
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

                CREATE TABLE IF NOT EXISTS snapshot_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    metric_key TEXT NOT NULL,
                    label TEXT NOT NULL,
                    percent_value REAL,
                    used_value REAL,
                    limit_value REAL,
                    unit TEXT,
                    resets_at TEXT,
                    raw_value TEXT,
                    extra_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_snapshot_metrics_provider_key
                    ON snapshot_metrics(provider, metric_key);
                CREATE INDEX IF NOT EXISTS idx_snapshot_metrics_snapshot_id
                    ON snapshot_metrics(snapshot_id);
                """
            )

            now = _utcnow()
            for key, value in (
                ("poll_interval_seconds", str(poll_interval_seconds)),
                ("heartbeat_interval_seconds", str(heartbeat_interval_seconds)),
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

    def get_provider_states(self) -> dict[str, dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM provider_state ORDER BY provider").fetchall()
        return {row["provider"]: dict(row) for row in rows}

    def record_success(
        self,
        provider: str,
        *,
        snapshot_hash: str,
        summary: str,
        recorded_snapshot_at: str | None,
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
                    consecutive_failures,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, 0, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    last_checked_at=excluded.last_checked_at,
                    last_success_at=excluded.last_success_at,
                    last_snapshot_at=COALESCE(excluded.last_snapshot_at, provider_state.last_snapshot_at),
                    last_hash=excluded.last_hash,
                    last_error=NULL,
                    last_summary=excluded.last_summary,
                    consecutive_failures=0,
                    updated_at=excluded.updated_at
                """,
                (
                    provider,
                    _utcnow(),
                    _utcnow(),
                    recorded_snapshot_at,
                    snapshot_hash,
                    summary,
                    _utcnow(),
                ),
            )

    def record_failure(self, provider: str, error: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO provider_state (
                    provider,
                    last_checked_at,
                    last_error,
                    consecutive_failures,
                    updated_at
                ) VALUES (?, ?, ?, 1, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    last_checked_at=excluded.last_checked_at,
                    last_error=excluded.last_error,
                    consecutive_failures=provider_state.consecutive_failures + 1,
                    updated_at=excluded.updated_at
                """,
                (provider, _utcnow(), error[:500], _utcnow()),
            )

    def insert_snapshot(self, snapshot: UsageSnapshot, reason: str) -> int:
        with self.connect() as conn:
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
                    snapshot.snapshot_hash(),
                    snapshot.page_title,
                    snapshot.plan_name,
                    snapshot.summary,
                    snapshot.raw_text,
                    snapshot.normalized_json(),
                    snapshot.capture_json(),
                ),
            )
            snapshot_id = int(cursor.lastrowid)
            for metric in snapshot.metrics:
                conn.execute(
                    """
                    INSERT INTO snapshot_metrics (
                        snapshot_id,
                        provider,
                        metric_key,
                        label,
                        percent_value,
                        used_value,
                        limit_value,
                        unit,
                        resets_at,
                        raw_value,
                        extra_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        snapshot.provider,
                        metric.key,
                        metric.label,
                        metric.percent_value,
                        metric.used_value,
                        metric.limit_value,
                        metric.unit,
                        metric.resets_at,
                        metric.raw_value,
                        json.dumps(metric.extra, sort_keys=True),
                    ),
                )
        return snapshot_id

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
            metrics = self._metrics_by_snapshot(conn, [row["id"] for row in rows])

        return {
            row["provider"]: {
                **dict(row),
                "metrics": metrics.get(row["id"], []),
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
            metrics = self._metrics_by_snapshot(conn, [row["id"] for row in rows])

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
            where.append("sm.provider = ?")
            params.append(provider)
        if start_at:
            where.append("s.recorded_at >= ?")
            params.append(start_at)
        if end_at:
            where.append("s.recorded_at <= ?")
            params.append(end_at)

        query = f"""
            SELECT
                sm.provider,
                sm.metric_key,
                sm.label,
                s.recorded_at,
                sm.percent_value,
                sm.used_value,
                sm.limit_value,
                sm.unit
            FROM snapshot_metrics sm
            JOIN snapshots s ON s.id = sm.snapshot_id
            WHERE {' AND '.join(where)}
            ORDER BY sm.provider, sm.metric_key, s.recorded_at
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

    def _metrics_by_snapshot(
        self, conn: sqlite3.Connection, snapshot_ids: list[int]
    ) -> dict[int, list[dict[str, Any]]]:
        if not snapshot_ids:
            return {}
        placeholders = ",".join("?" for _ in snapshot_ids)
        rows = conn.execute(
            f"""
            SELECT snapshot_id, provider, metric_key, label, percent_value, used_value,
                   limit_value, unit, resets_at, raw_value, extra_json
            FROM snapshot_metrics
            WHERE snapshot_id IN ({placeholders})
            ORDER BY id
            """,
            snapshot_ids,
        ).fetchall()

        grouped: dict[int, list[dict[str, Any]]] = {snapshot_id: [] for snapshot_id in snapshot_ids}
        for row in rows:
            item = dict(row)
            item["extra"] = json.loads(item.pop("extra_json") or "{}")
            grouped[row["snapshot_id"]].append(item)
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
