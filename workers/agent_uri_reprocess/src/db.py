"""Postgres access for agent_uri_reprocess."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row

from documents import LOOKUP_SQL, TOUCH_SQL, UPSERT_SQL, dumps_document, uri_hash

logger = logging.getLogger("agent_uri_reprocess.db")

CLAIM_MAX_ATTEMPTS = 3
CLAIM_RETRY_BASE_SECONDS = 2.0
ERROR_MESSAGE_MAX_LEN = 2000
RETRYABLE_DB_EXCEPTIONS = (psycopg.OperationalError, psycopg.InterfaceError)
_NO_RECONNECT_EXCEPTIONS = (
    psycopg.errors.QueryCanceled,
    psycopg.errors.DeadlockDetected,
)

T = TypeVar("T")

CLAIM_ERROR_MANIFESTS_SQL = """
WITH candidates AS (
  SELECT m.id
  FROM erc_8004.agent_manifest m
  WHERE (
    (m.has_download_error = true AND m.reprocess_count IS NULL)
    OR m.does_need_manual_reprocess = TRUE
    OR (
      m.has_download_error = true
      AND m.reprocess_count IS NOT NULL
      AND m.reprocess_count < 3
      AND m.updated_at < (NOW() - INTERVAL '3 day')
    )
  )
  ORDER BY m.id
  LIMIT %(limit)s
  FOR UPDATE OF m SKIP LOCKED
)
SELECT
  m.id,
  m.agent_id,
  m.provider,
  m.uri_document_id,
  m.reprocess_count,
  m.does_need_manual_reprocess,
  m.has_download_error
FROM erc_8004.agent_manifest m
JOIN candidates c ON c.id = m.id
"""

CLAIM_REFRESH_DOCS_SQL = """
WITH candidates AS (
  SELECT ud.id
  FROM erc_8004.uri_documents ud
  WHERE ud.status = 'valid'
    AND ud.fetched_at < (NOW() - INTERVAL '15 days')
    AND ud.uri ~* '^(https?://|ipfs://)'
    AND NOT starts_with(ud.uri, 'internal_on_chain_id_')
  ORDER BY ud.fetched_at NULLS FIRST, ud.id
  LIMIT %(limit)s
  FOR UPDATE OF ud SKIP LOCKED
)
SELECT ud.id, ud.uri, ud.document, ud.source_gateway
FROM erc_8004.uri_documents ud
JOIN candidates c ON c.id = ud.id
"""

MARK_REPROCESS_SUCCESS_SQL = """
UPDATE erc_8004.agent_manifest
SET
  uri_document_id = %(uri_document_id)s,
  has_download_error = FALSE,
  download_error_message = NULL,
  reprocess_count = 0,
  does_need_manual_reprocess = FALSE,
  is_processed = FALSE,
  is_processed_with_error = FALSE,
  processed_error_message = NULL,
  is_processed_missing_function = FALSE,
  is_active = TRUE,
  url_type = %(url_type)s,
  updated_at = NOW()
WHERE id = %(id)s
"""

MARK_REPROCESS_FAILURE_SQL = """
UPDATE erc_8004.agent_manifest
SET
  has_download_error = TRUE,
  download_error_message = %(download_error_message)s,
  reprocess_count = COALESCE(reprocess_count, 0) + 1,
  is_active = FALSE,
  url_type = %(url_type)s,
  updated_at = NOW()
WHERE id = %(id)s
"""

RENEW_DOCUMENT_TTL_SQL = """
UPDATE erc_8004.uri_documents
SET
  fetched_at = NOW(),
  expires_at = NOW() + interval '15 days',
  last_accessed_at = NOW(),
  fetch_count = COALESCE(fetch_count, 0) + 1,
  updated_at = NOW()
WHERE id = %(id)s
"""

# Failed refresh: keep previous document/status, but stamp the attempt so the
# 15d claim cursor advances (fetched_at = last try, including errors).
STAMP_REFRESH_ATTEMPT_SQL = """
UPDATE erc_8004.uri_documents
SET
  fetched_at = NOW(),
  expires_at = NOW() + interval '15 days',
  last_accessed_at = NOW(),
  fetch_count = COALESCE(fetch_count, 0) + 1,
  source_gateway = %(source_gateway)s,
  updated_at = NOW()
WHERE id = %(id)s
"""

RESET_MANIFESTS_IF_DOC_CHANGED_SQL = """
UPDATE erc_8004.agent_manifest
SET
  is_processed = FALSE,
  is_processed_with_error = FALSE,
  processed_error_message = NULL,
  updated_at = NOW()
WHERE uri_document_id = %(uri_document_id)s
  AND is_processed IS DISTINCT FROM FALSE
"""

LOOKUP_AGENT_URI_SQL = """
SELECT agent_uri_raw
FROM erc_8004.agents
WHERE id = %(agent_id)s
LIMIT 1
"""

LOOKUP_FEEDBACK_SQL = """
SELECT
  id,
  feedback_type,
  feedback_uri_raw,
  end_point
FROM erc_8004.registration_feedbacks
WHERE id = %(feedback_id)s
LIMIT 1
"""

ENDPOINT_URI_RE = re.compile(r"https?://[^\s,]+|ipfs://[^\s,]+", re.I)
PROVIDER_FEEDBACK_RE = re.compile(r"^feedback_erc_8004_id_(\d+)$")


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
                        "%s attempt %s/%s retryable (%s); sleep %.1fs",
                        operation,
                        attempt,
                        CLAIM_MAX_ATTEMPTS,
                        exc.__class__.__name__,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    logger.warning(
                        "%s attempt %s/%s connection error; reconnect in %.1fs",
                        operation,
                        attempt,
                        CLAIM_MAX_ATTEMPTS,
                        delay,
                    )
                    time.sleep(delay)
                    self._reconnect()
            except Exception:
                self._safe_rollback()
                raise
        assert last_exc is not None
        raise last_exc

    def claim_error_manifests(self, limit: int) -> list[dict[str, Any]]:
        def _claim() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(CLAIM_ERROR_MANIFESTS_SQL, {"limit": limit})
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("claim_error_manifests", _claim)

    def claim_refresh_docs(self, limit: int) -> list[dict[str, Any]]:
        def _claim() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(CLAIM_REFRESH_DOCS_SQL, {"limit": limit})
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("claim_refresh_docs", _claim)

    def lookup_document(self, uri: str) -> dict[str, Any] | None:
        def _lookup() -> dict[str, Any] | None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(LOOKUP_SQL, {"uri_hash": uri_hash(uri)})
                row = cur.fetchone()
            self._conn.commit()
            return dict(row) if row else None

        return self._run_with_db_retry("lookup_document", _lookup)

    def touch_document(self, doc_id: int) -> None:
        def _touch() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(TOUCH_SQL, {"id": doc_id})
            self._conn.commit()

        self._run_with_db_retry("touch_document", _touch)

    def upsert_document(
        self,
        *,
        uri: str,
        document: Any,
        source_gateway: str,
        cid: str | None,
    ) -> int:
        def _upsert() -> int:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    UPSERT_SQL,
                    {
                        "uri_hash": uri_hash(uri),
                        "uri": uri,
                        "cid": cid,
                        "document": dumps_document(document),
                        "source_gateway": source_gateway,
                    },
                )
                row = cur.fetchone()
            self._conn.commit()
            assert row is not None
            return int(row["id"])

        return self._run_with_db_retry("upsert_document", _upsert)

    def mark_reprocess_success(
        self,
        *,
        manifest_id: int,
        uri_document_id: int,
        url_type: str,
    ) -> None:
        def _mark() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    MARK_REPROCESS_SUCCESS_SQL,
                    {
                        "id": manifest_id,
                        "uri_document_id": uri_document_id,
                        "url_type": url_type,
                    },
                )
            self._conn.commit()

        self._run_with_db_retry("mark_reprocess_success", _mark)

    def mark_reprocess_failure(
        self,
        *,
        manifest_id: int,
        error_message: str,
        url_type: str = "error",
    ) -> None:
        msg = error_message
        if len(msg) > ERROR_MESSAGE_MAX_LEN:
            msg = msg[: ERROR_MESSAGE_MAX_LEN - 3] + "..."

        def _mark() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    MARK_REPROCESS_FAILURE_SQL,
                    {
                        "id": manifest_id,
                        "download_error_message": msg,
                        "url_type": url_type,
                    },
                )
            self._conn.commit()

        self._run_with_db_retry("mark_reprocess_failure", _mark)

    def renew_document_ttl(self, doc_id: int) -> None:
        def _renew() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(RENEW_DOCUMENT_TTL_SQL, {"id": doc_id})
            self._conn.commit()

        self._run_with_db_retry("renew_document_ttl", _renew)

    def stamp_refresh_attempt_failed(self, doc_id: int, *, error_message: str) -> None:
        """Record a failed fetch without replacing `document` / `status`."""
        gateway = error_message.strip() or "refresh_error"
        if not gateway.startswith("refresh_error"):
            gateway = f"refresh_error:{gateway}"
        if len(gateway) > 200:
            gateway = gateway[:197] + "..."

        def _stamp() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    STAMP_REFRESH_ATTEMPT_SQL,
                    {"id": doc_id, "source_gateway": gateway},
                )
            self._conn.commit()

        self._run_with_db_retry("stamp_refresh_attempt_failed", _stamp)

    def reset_manifests_for_document(self, uri_document_id: int) -> int:
        def _reset() -> int:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    RESET_MANIFESTS_IF_DOC_CHANGED_SQL,
                    {"uri_document_id": uri_document_id},
                )
                count = cur.rowcount
            self._conn.commit()
            return int(count)

        return self._run_with_db_retry("reset_manifests_for_document", _reset)

    def lookup_agent_uri(self, agent_id: int) -> str | None:
        def _lookup() -> str | None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(LOOKUP_AGENT_URI_SQL, {"agent_id": agent_id})
                row = cur.fetchone()
            self._conn.commit()
            if not row:
                return None
            raw = row.get("agent_uri_raw")
            return (raw or "").strip() or None

        return self._run_with_db_retry("lookup_agent_uri", _lookup)

    def lookup_feedback(self, feedback_id: int) -> dict[str, Any] | None:
        def _lookup() -> dict[str, Any] | None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(LOOKUP_FEEDBACK_SQL, {"feedback_id": feedback_id})
                row = cur.fetchone()
            self._conn.commit()
            return dict(row) if row else None

        return self._run_with_db_retry("lookup_feedback", _lookup)


def feedback_uri_from_row(row: dict[str, Any]) -> str | None:
    if row.get("feedback_type") == "feedback_uri":
        return (row.get("feedback_uri_raw") or "").strip() or None
    if row.get("feedback_type") == "feedback_on_chain":
        return None
    end_point = row.get("end_point") or ""
    match = ENDPOINT_URI_RE.search(end_point)
    return match.group(0) if match else None


def parse_feedback_id(provider: str) -> int | None:
    match = PROVIDER_FEEDBACK_RE.match(provider or "")
    return int(match.group(1)) if match else None
