"""Postgres access for agent_endpoint_liveness."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("agent_endpoint_liveness")

CLAIM_MAX_ATTEMPTS = 3
CLAIM_RETRY_BASE_SECONDS = 2.0
RETRYABLE_DB_EXCEPTIONS = (psycopg.OperationalError, psycopg.InterfaceError)
_NO_RECONNECT_EXCEPTIONS = (
    psycopg.errors.QueryCanceled,
    psycopg.errors.DeadlockDetected,
)

SYNC_SQL = "SELECT erc_8004.agent_endpoint_health_sync() AS result"
CLAIM_SQL = """
SELECT agent_id, endpoint_normalized, endpoint, internal_type
FROM erc_8004.agent_endpoint_health_claim(
  %(limit)s,
  %(worker_id)s,
  %(stale_seconds)s
)
"""
COMPLETE_BATCH_SQL = (
    "SELECT erc_8004.agent_endpoint_health_complete_batch(%(rows)s::jsonb) AS updated"
)

T = TypeVar("T")


class Database:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    def connect(self) -> None:
        self._conn = psycopg.connect(self._dsn, row_factory=dict_row)
        with self._conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _reconnect(self) -> None:
        logger.warning("Reconnecting to Postgres after connection failure")
        self.close()
        self.connect()

    def ensure_connected(self) -> None:
        if self._conn is None or self._conn.closed:
            self._reconnect()

    def _safe_rollback(self) -> None:
        if self._conn is None or self._conn.closed:
            return
        try:
            self._conn.rollback()
        except Exception:
            pass

    def _run_with_db_retry(self, operation: str, fn: Callable[[], T]) -> T:
        last_exc: Exception | None = None
        for attempt in range(1, CLAIM_MAX_ATTEMPTS + 1):
            try:
                self.ensure_connected()
                return fn()
            except RETRYABLE_DB_EXCEPTIONS as exc:
                last_exc = exc
                self._safe_rollback()
                if attempt >= CLAIM_MAX_ATTEMPTS:
                    break
                delay = CLAIM_RETRY_BASE_SECONDS * attempt
                if isinstance(exc, _NO_RECONNECT_EXCEPTIONS):
                    logger.warning(
                        "%s attempt %s/%s retryable DB error (%s); retrying in %.1fs",
                        operation,
                        attempt,
                        CLAIM_MAX_ATTEMPTS,
                        exc.__class__.__name__,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    logger.warning(
                        "%s attempt %s/%s connection error (%s); reconnecting in %.1fs",
                        operation,
                        attempt,
                        CLAIM_MAX_ATTEMPTS,
                        exc.__class__.__name__,
                        delay,
                    )
                    time.sleep(delay)
                    self._reconnect()
            except Exception:
                self._safe_rollback()
                raise

        assert last_exc is not None
        raise last_exc

    def sync(self) -> dict[str, Any]:
        def _sync() -> dict[str, Any]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute("SET statement_timeout = '600s'")
                cur.execute(SYNC_SQL)
                row = cur.fetchone()
                cur.execute("SET statement_timeout = '300s'")
            self._conn.commit()
            if not row or row.get("result") is None:
                return {}
            result = row["result"]
            if isinstance(result, dict):
                return result
            return json.loads(result)

        return self._run_with_db_retry("sync", _sync)

    def claim_rows(
        self,
        worker_id: str,
        limit: int,
        stale_seconds: int,
    ) -> list[dict[str, Any]]:
        def _claim() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    CLAIM_SQL,
                    {
                        "limit": limit,
                        "worker_id": worker_id,
                        "stale_seconds": stale_seconds,
                    },
                )
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("claim", _claim)

    def complete_batch(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        def _complete() -> int:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(COMPLETE_BATCH_SQL, {"rows": json.dumps(rows)})
                result = cur.fetchone()
            self._conn.commit()
            if result is None:
                return 0
            return int(result["updated"])

        return self._run_with_db_retry("complete_batch", _complete)
