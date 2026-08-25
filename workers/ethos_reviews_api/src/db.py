"""Postgres access for ethos_reviews_api."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("ethos_reviews_api")

CLAIM_MAX_ATTEMPTS = 3
CLAIM_RETRY_BASE_SECONDS = 2.0
RETRYABLE_DB_EXCEPTIONS = (psycopg.OperationalError, psycopg.InterfaceError)
_NO_RECONNECT_EXCEPTIONS = (
    psycopg.errors.QueryCanceled,
    psycopg.errors.DeadlockDetected,
)

CLAIM_SQL = """
SELECT profile_id, reviews_fetched_at
FROM ethos.claim_reviews_fetch(
  %(limit)s,
  %(worker_id)s,
  %(stale_seconds)s
)
"""
COMPLETE_SQL = """
SELECT ethos.complete_reviews_fetch(%(profile_id)s, %(ok)s, %(status)s)
"""
UPSERT_SQL = """
INSERT INTO ethos.reviews (
  graph_id, review_id, score, author_address, subject_address, attestation_hash,
  comment, metadata, created_at_on_chain, archived,
  author_profile_id, subject_profile_id, imported_at, updated_at
) VALUES (
  %(graph_id)s, %(review_id)s, %(score)s, %(author_address)s, %(subject_address)s,
  %(attestation_hash)s, %(comment)s, %(metadata)s, to_timestamp(%(created_at)s),
  %(archived)s, %(author_profile_id)s, %(subject_profile_id)s, NOW(), NOW()
)
ON CONFLICT (graph_id) DO UPDATE SET
  review_id = EXCLUDED.review_id, score = EXCLUDED.score,
  author_address = EXCLUDED.author_address, subject_address = EXCLUDED.subject_address,
  attestation_hash = EXCLUDED.attestation_hash, comment = EXCLUDED.comment,
  metadata = EXCLUDED.metadata, created_at_on_chain = EXCLUDED.created_at_on_chain,
  archived = EXCLUDED.archived, author_profile_id = EXCLUDED.author_profile_id,
  subject_profile_id = EXCLUDED.subject_profile_id, updated_at = NOW()
"""

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

    def complete(self, profile_id: int, ok: bool, status: str) -> None:
        def _complete() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    COMPLETE_SQL,
                    {
                        "profile_id": profile_id,
                        "ok": ok,
                        "status": status,
                    },
                )
            self._conn.commit()

        self._run_with_db_retry("complete", _complete)

    def upsert_reviews(self, rows: list[dict[str, Any]], chunk_size: int = 200) -> int:
        if not rows:
            return 0
        size = max(int(chunk_size), 1)

        def _upsert() -> int:
            assert self._conn is not None
            total = 0
            with self._conn.cursor() as cur:
                for start in range(0, len(rows), size):
                    chunk = rows[start : start + size]
                    cur.executemany(UPSERT_SQL, chunk)
                    total += len(chunk)
            self._conn.commit()
            return total

        return self._run_with_db_retry("upsert_reviews", _upsert)
