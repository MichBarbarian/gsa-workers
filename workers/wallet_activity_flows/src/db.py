"""Supabase Postgres access for wallet_activity_flows."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("wallet_activity_flows")

# Null/burn address: alchemy_getAssetTransfers paginates forever and OOMs the
# GHA runner on insert_and_mark_done (seen BSC wt with 0x0, 2026-09-10).
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

CLAIM_ROWS_SQL = f"""
WITH candidates AS (
  SELECT wt.id
  FROM erc_8004.wallet_transactions wt
  JOIN erc_8004.chains c ON c.id = wt.chain_id
  JOIN erc_8004.wallets w ON w.id = wt.wallet_id
  WHERE c.chain_id = ANY(%(evm_ids)s)
    AND wt.is_valid_activity_flows IS TRUE
    AND wt.activity_flows_agent_ok IS TRUE
    AND wt.activity_flows_next_eligible_at IS NOT NULL
    AND wt.activity_flows_next_eligible_at <= NOW()
    AND COALESCE(wt.wallet_category, '') NOT LIKE 'Dormant_%%'
    AND lower(w.address) <> '{ZERO_ADDRESS}'
    AND (
      wt.activity_flows_claimed_at IS NULL
      OR wt.activity_flows_claimed_at
           < NOW() - make_interval(secs => %(stale_seconds)s)
    )
  LIMIT %(limit)s
  FOR UPDATE OF wt SKIP LOCKED
),
updated AS (
  UPDATE erc_8004.wallet_transactions wt
  SET
    activity_flows_claimed_at = NOW(),
    activity_flows_claimed_by = %(worker_id)s,
    activity_flows_next_eligible_at =
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
  c.subdomain_alchemy,
  lower(w.address) AS address
FROM updated u
JOIN erc_8004.wallets w ON w.id = u.wallet_id
JOIN erc_8004.chains c ON c.id = u.chain_id
"""

MARK_DONE_SQL = """
UPDATE erc_8004.wallet_transactions
SET
  activity_flows_completed_at = NOW(),
  activity_flows_claimed_at = NOW(),
  has_activity_flows_error = FALSE,
  activity_flows_message_error = NULL,
  activity_flows_next_eligible_at = CASE
    WHEN EXTRACT(DAY FROM timezone('utc', NOW())) < 15
      THEN (date_trunc('month', timezone('utc', NOW())) + interval '14 days')
           AT TIME ZONE 'utc'
    ELSE (date_trunc('month', timezone('utc', NOW())) + interval '1 month')
         AT TIME ZONE 'utc'
  END
WHERE id = %(row_id)s
"""

MARK_ERROR_SQL = """
UPDATE erc_8004.wallet_transactions
SET
  activity_flows_claimed_at = NOW(),
  has_activity_flows_error = TRUE,
  activity_flows_message_error = %(error_message)s,
  activity_flows_next_eligible_at = NOW() + interval '1 hour'
WHERE id = %(row_id)s
"""

INSERT_SQL = """
SELECT wallets.wallet_activity_transfers_insert(%(rows)s::jsonb)
"""

LOOKUP_BLOCKS_SQL = """
SELECT block_number, block_time
FROM erc_8004.block_cache
WHERE chain_id = %(chain_id)s
  AND block_number = ANY(%(blocks)s)
"""

UPSERT_BLOCK_SQL = """
INSERT INTO erc_8004.block_cache (chain_id, block_number, block_time)
VALUES (%(chain_id)s, %(block_number)s, %(block_time)s)
ON CONFLICT (chain_id, block_number) DO NOTHING
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
                        exc,
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
                cur.execute(
                    MARK_ERROR_SQL,
                    {"row_id": row_id, "error_message": msg},
                )
            self._conn.commit()

        self._run_with_db_retry("mark_error", _mark)

    def lookup_block_times(
        self, chain_pk: int, blocks: list[int]
    ) -> dict[int, datetime]:
        if not blocks:
            return {}

        def _lookup() -> dict[int, datetime]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    LOOKUP_BLOCKS_SQL,
                    {"chain_id": chain_pk, "blocks": blocks},
                )
                rows = list(cur.fetchall())
            self._conn.commit()
            out: dict[int, datetime] = {}
            for row in rows:
                out[int(row["block_number"])] = row["block_time"]
            return out

        return self._run_with_db_retry("lookup_block_times", _lookup)

    def upsert_block_times(
        self, chain_pk: int, times: dict[int, datetime]
    ) -> None:
        if not times:
            return

        def _upsert() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                for block_number, block_time in times.items():
                    cur.execute(
                        UPSERT_BLOCK_SQL,
                        {
                            "chain_id": chain_pk,
                            "block_number": block_number,
                            "block_time": block_time,
                        },
                    )
            self._conn.commit()

        self._run_with_db_retry("upsert_block_times", _upsert)
