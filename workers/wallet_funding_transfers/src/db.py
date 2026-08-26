"""Postgres access for wallet_funding_transfers."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("wallet_funding_transfers")

CLAIM_ROWS_SQL = """
WITH ranked AS MATERIALIZED (
  SELECT wt.id, wt.wallet_id, wt.funding_transfers_next_eligible_at, wt.wallet_category
  FROM erc_8004.wallet_transactions wt
  JOIN erc_8004.chains c ON c.id = wt.chain_id
  WHERE c.chain_id = ANY(%(evm_ids)s)
    AND wt.is_valid_funding_transfers IS TRUE
    AND wt.funding_transfers_next_eligible_at IS NOT NULL
    AND wt.funding_transfers_next_eligible_at <= NOW()
    AND (
      wt.funding_transfers_claimed_at IS NULL
      OR wt.funding_transfers_claimed_at
           < NOW() - make_interval(secs => %(stale_seconds)s)
    )
  ORDER BY
    CASE WHEN COALESCE(wt.wallet_category, '') LIKE 'Dormant_%%' THEN 1 ELSE 0 END,
    wt.funding_transfers_next_eligible_at,
    wt.id
  LIMIT GREATEST(%(limit)s * 5, %(limit)s)
),
filtered AS MATERIALIZED (
  SELECT r.id, r.funding_transfers_next_eligible_at, r.wallet_category
  FROM ranked r
  WHERE EXISTS (
    SELECT 1
    FROM erc_8004.agent_wallet_tx awt
    JOIN erc_8004.agents a
      ON a.id = awt.agent_id
     AND a.validation_realness_status = 'valid'
    WHERE awt.wallet_id = r.wallet_id
      AND awt.is_valid
      AND awt.deleted_at IS NULL
  )
  ORDER BY
    CASE WHEN COALESCE(r.wallet_category, '') LIKE 'Dormant_%%' THEN 1 ELSE 0 END,
    r.funding_transfers_next_eligible_at,
    r.id
  LIMIT %(limit)s
),
candidates AS (
  SELECT wt.id
  FROM erc_8004.wallet_transactions wt
  JOIN filtered f ON f.id = wt.id
  ORDER BY
    CASE WHEN COALESCE(f.wallet_category, '') LIKE 'Dormant_%%' THEN 1 ELSE 0 END,
    f.funding_transfers_next_eligible_at,
    wt.id
  FOR UPDATE OF wt SKIP LOCKED
),
updated AS (
  UPDATE erc_8004.wallet_transactions wt
  SET
    funding_transfers_claimed_at = NOW(),
    funding_transfers_claimed_by = %(worker_id)s,
    funding_transfers_next_eligible_at =
      NOW() + make_interval(secs => %(stale_seconds)s)
  FROM candidates c
  WHERE wt.id = c.id
  RETURNING wt.id, wt.wallet_id, wt.chain_id
)
SELECT
  u.id,
  u.wallet_id,
  u.chain_id,
  c.chain_id AS evm_chain_id,
  lower(w.address) AS address
FROM updated u
JOIN erc_8004.wallets w ON w.id = u.wallet_id
JOIN erc_8004.chains c ON c.id = u.chain_id
"""

MARK_DONE_SQL = """
UPDATE erc_8004.wallet_transactions
SET
  funding_transfers_completed_at = NOW(),
  funding_transfers_claimed_at = NOW(),
  has_funding_transfers_error = FALSE,
  funding_transfers_message_error = NULL,
  funding_transfers_next_eligible_at = 'infinity'::timestamptz
WHERE id = %(row_id)s
"""

MARK_ERROR_SQL = """
UPDATE erc_8004.wallet_transactions
SET
  funding_transfers_claimed_at = NOW(),
  has_funding_transfers_error = TRUE,
  funding_transfers_message_error = %(error_message)s,
  funding_transfers_next_eligible_at = NOW() + interval '1 hour'
WHERE id = %(row_id)s
"""

UNLOCK_SQL = """
UPDATE erc_8004.wallet_transactions
SET
  funding_transfers_claimed_at = NULL,
  funding_transfers_claimed_by = NULL,
  funding_transfers_next_eligible_at = NOW()
WHERE id = ANY(%(ids)s)
"""

INSERT_SQL = """
SELECT wallets.wallet_funding_transfers_insert(%(rows)s::jsonb)
"""

CLAIM_MAX_ATTEMPTS = 3
CLAIM_RETRY_BASE_SECONDS = 2.0
ERROR_MESSAGE_MAX_LEN = 2000
RETRYABLE_DB_EXCEPTIONS = (psycopg.OperationalError, psycopg.InterfaceError)
_NO_RECONNECT_EXCEPTIONS = (
    psycopg.errors.QueryCanceled,
    psycopg.errors.DeadlockDetected,
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
                logger.warning(
                    "%s attempt %s/%s retryable DB error (%s); retrying in %.1fs",
                    operation,
                    attempt,
                    CLAIM_MAX_ATTEMPTS,
                    exc.__class__.__name__,
                    delay,
                )
                time.sleep(delay)
                if not isinstance(exc, _NO_RECONNECT_EXCEPTIONS):
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
        evm_ids: list[int],
    ) -> list[dict[str, Any]]:
        def _claim() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    CLAIM_ROWS_SQL,
                    {
                        "worker_id": worker_id,
                        "limit": limit,
                        "stale_seconds": stale_seconds,
                        "evm_ids": evm_ids,
                    },
                )
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("claim", _claim)

    def insert_and_mark_done(self, row_id: int, transfers: list[dict[str, Any]]) -> str:
        def _save() -> str:
            assert self._conn is not None
            result_text = "skipped_empty"
            with self._conn.cursor() as cur:
                if transfers:
                    cur.execute(INSERT_SQL, {"rows": json.dumps(transfers)})
                    result = cur.fetchone()
                    if result is not None:
                        result_text = str(next(iter(result.values())))
                cur.execute(MARK_DONE_SQL, {"row_id": row_id})
            self._conn.commit()
            return result_text

        return self._run_with_db_retry("insert_and_mark_done", _save)

    def mark_error(self, row_id: int, error_message: str) -> None:
        msg = (error_message or "unknown error").strip()
        if len(msg) > ERROR_MESSAGE_MAX_LEN:
            msg = msg[: ERROR_MESSAGE_MAX_LEN - 3] + "..."

        def _mark() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(MARK_ERROR_SQL, {"row_id": row_id, "error_message": msg})
            self._conn.commit()

        self._run_with_db_retry("mark_error", _mark)

    def unlock_rows(self, ids: list[int]) -> None:
        if not ids:
            return

        def _unlock() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(UNLOCK_SQL, {"ids": ids})
            self._conn.commit()

        self._run_with_db_retry("unlock", _unlock)
