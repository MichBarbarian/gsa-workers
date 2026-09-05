"""Supabase Postgres access for ai_agent_classifier job."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

logger = logging.getLogger("ai_agent_classifier")

PROCESS_CODE = "agent-classifier"

CLAIM_AGENTS_SQL = """
WITH candidates AS (
  SELECT a.id
  FROM web_dashboard.agents a
  WHERE a.does_need_ai_category_process IS TRUE
    AND COALESCE(a.has_ai_category_process_error, false) IS NOT TRUE
  ORDER BY a.id
  LIMIT %(limit)s
  FOR UPDATE OF a SKIP LOCKED
)
SELECT
  a.id,
  a.name,
  a.description,
  a.skills,
  a.tags,
  a.capabilites,
  a.services,
  a.oasf_skills,
  a.oasf_domains,
  a.web
FROM web_dashboard.agents a
JOIN candidates c ON c.id = a.id
"""

LOAD_CATEGORIES_SQL = """
SELECT category_name
FROM web_dashboard.agent_ai_categories
WHERE is_active IS TRUE
ORDER BY id
"""

LOAD_SYSTEM_PROMPT_SQL = """
SELECT system_prompt
FROM llm.process
WHERE process_code = %(process_code)s
"""

LOAD_MODELS_SQL = """
SELECT
  m.id AS model_id,
  m.name AS model_name,
  m.slug AS model_slug,
  m.request_per_day,
  m.request_per_minute,
  m.tokens_per_minute,
  m.tokents_per_day,
  m.has_limits,
  m.does_need_thinking_off_parameter,
  p.id AS provider_id,
  p.name AS provider_name,
  p.secret AS provider_secret,
  p.base_url,
  p.temperature,
  p.max_completion_tokens,
  p.response_format,
  COALESCE(mr.request_total, 0) AS request_total_today,
  COALESCE(mr.token_total, 0) AS token_total_today
FROM llm.process proc
JOIN llm.procees_llm_providers plp ON plp.process_id = proc.id
JOIN llm.llm_provider p ON p.id = plp.llm_provider_id
JOIN llm.models m ON m.llm_provider_id = p.id
LEFT JOIN llm.models_requests mr
  ON mr.model_id = m.id
 AND mr.date = CURRENT_DATE
WHERE proc.process_code = %(process_code)s
  AND p.is_active IS TRUE
  AND m.is_active IS TRUE
ORDER BY m.id
"""

INCREMENT_REQUEST_SQL = """
INSERT INTO llm.models_requests (model_id, date, request_total, token_total)
VALUES (%(model_id)s, CURRENT_DATE, 1, %(tokens)s)
ON CONFLICT (model_id, date)
DO UPDATE SET
  request_total = llm.models_requests.request_total + 1,
  token_total = COALESCE(llm.models_requests.token_total, 0) + EXCLUDED.token_total
RETURNING request_total, token_total
"""

FIND_DONOR_SQL = """
SELECT
  a.id,
  a.ai_category_primary,
  a.ai_category_secondary,
  a.ai_category_confidence,
  a.ai_category_reasoning,
  a.ai_category_purpose,
  a.llm_model_id
FROM web_dashboard.agents a
WHERE a.ai_category_input_hash = %(input_hash)s
  AND a.id <> %(agent_id)s
  AND a.ai_category_primary IS NOT NULL
  AND COALESCE(a.has_ai_category_process_error, false) IS NOT TRUE
ORDER BY a.ai_category_process_calculated_at DESC NULLS LAST, a.id
LIMIT 1
"""

MARK_SUCCESS_SQL = """
UPDATE web_dashboard.agents
SET
  ai_category_primary = %(primary_category)s,
  ai_category_secondary = %(secondary_categories)s,
  ai_category_confidence = %(confidence)s,
  ai_category_reasoning = %(reasoning)s,
  ai_category_purpose = %(agent_purpose)s,
  llm_model_id = %(llm_model_id)s,
  ai_category_input_hash = %(input_hash)s,
  ai_category_process_calculated_at = NOW(),
  does_need_ai_category_process = FALSE,
  has_ai_category_process_error = FALSE,
  ai_category_process_error_message = NULL
WHERE id = %(agent_id)s
"""

MARK_ERROR_SQL = """
UPDATE web_dashboard.agents
SET
  does_need_ai_category_process = FALSE,
  has_ai_category_process_error = TRUE,
  ai_category_process_error_message = %(error_message)s,
  ai_category_process_calculated_at = NOW(),
  llm_model_id = %(llm_model_id)s
WHERE id = %(agent_id)s
"""

REQUEUE_ERRORS_SQL = """
WITH picked AS (
  SELECT id
  FROM web_dashboard.agents
  WHERE has_ai_category_process_error IS TRUE
  ORDER BY id
  LIMIT %(limit)s
  FOR UPDATE SKIP LOCKED
)
UPDATE web_dashboard.agents a
SET
  does_need_ai_category_process = TRUE,
  has_ai_category_process_error = NULL,
  ai_category_process_error_message = NULL
FROM picked
WHERE a.id = picked.id
RETURNING a.id
"""

FETCH_MISSING_HASH_SQL = """
SELECT
  a.id,
  a.name,
  a.description,
  a.skills,
  a.tags,
  a.capabilites,
  a.services,
  a.oasf_skills,
  a.oasf_domains,
  a.web
FROM web_dashboard.agents a
WHERE a.ai_category_primary IS NOT NULL
  AND COALESCE(a.has_ai_category_process_error, false) IS NOT TRUE
  AND a.ai_category_input_hash IS NULL
ORDER BY a.id
LIMIT %(limit)s
"""

SET_INPUT_HASH_SQL = """
UPDATE web_dashboard.agents
SET ai_category_input_hash = %(input_hash)s
WHERE id = %(agent_id)s
  AND ai_category_input_hash IS NULL
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

    def claim_agents(self, limit: int) -> list[dict[str, Any]]:
        def _claim() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(CLAIM_AGENTS_SQL, {"limit": limit})
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("claim", _claim)

    def load_active_categories(self) -> list[str]:
        def _load() -> list[str]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(LOAD_CATEGORIES_SQL)
                rows = list(cur.fetchall())
            self._conn.commit()
            return [str(r["category_name"]) for r in rows]

        return self._run_with_db_retry("load_categories", _load)

    def load_system_prompt(self) -> str | None:
        def _load() -> str | None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(LOAD_SYSTEM_PROMPT_SQL, {"process_code": PROCESS_CODE})
                row = cur.fetchone()
            self._conn.commit()
            if row is None:
                return None
            raw = row.get("system_prompt")
            if raw is None:
                return None
            text = str(raw).strip()
            return text or None

        return self._run_with_db_retry("load_system_prompt", _load)

    def load_process_models(self) -> list[dict[str, Any]]:
        def _load() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(LOAD_MODELS_SQL, {"process_code": PROCESS_CODE})
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("load_models", _load)

    def increment_model_request(self, model_id: int, tokens: int = 0) -> dict[str, int]:
        def _inc() -> dict[str, int]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    INCREMENT_REQUEST_SQL,
                    {"model_id": model_id, "tokens": max(int(tokens), 0)},
                )
                row = cur.fetchone()
            self._conn.commit()
            if row is None:
                return {"request_total": 0, "token_total": 0}
            return {
                "request_total": int(row["request_total"] or 0),
                "token_total": int(row["token_total"] or 0),
            }

        return self._run_with_db_retry("increment_request", _inc)

    def find_classification_donor(
        self,
        *,
        agent_id: int,
        input_hash: str,
    ) -> dict[str, Any] | None:
        def _find() -> dict[str, Any] | None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    FIND_DONOR_SQL,
                    {"agent_id": agent_id, "input_hash": input_hash},
                )
                row = cur.fetchone()
            self._conn.commit()
            return row

        return self._run_with_db_retry("find_donor", _find)

    def mark_success(
        self,
        *,
        agent_id: int,
        llm_model_id: int | None,
        primary_category: str,
        secondary_categories: list[str] | Any,
        confidence: float | None,
        reasoning: str | None,
        agent_purpose: str | None,
        input_hash: str,
    ) -> None:
        secondary = secondary_categories
        if not isinstance(secondary, Json):
            secondary = Json(secondary_categories)

        def _mark() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    MARK_SUCCESS_SQL,
                    {
                        "agent_id": agent_id,
                        "llm_model_id": llm_model_id,
                        "primary_category": primary_category,
                        "secondary_categories": secondary,
                        "confidence": confidence,
                        "reasoning": reasoning,
                        "agent_purpose": agent_purpose,
                        "input_hash": input_hash,
                    },
                )
            self._conn.commit()

        self._run_with_db_retry("mark_success", _mark)

    def fetch_agents_missing_input_hash(self, limit: int) -> list[dict[str, Any]]:
        def _fetch() -> list[dict[str, Any]]:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(FETCH_MISSING_HASH_SQL, {"limit": limit})
                rows = list(cur.fetchall())
            self._conn.commit()
            return rows

        return self._run_with_db_retry("fetch_missing_hash", _fetch)

    def set_ai_category_input_hash(self, *, agent_id: int, input_hash: str) -> None:
        def _set() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    SET_INPUT_HASH_SQL,
                    {"agent_id": agent_id, "input_hash": input_hash},
                )
            self._conn.commit()

        self._run_with_db_retry("set_input_hash", _set)

    def mark_error(
        self,
        *,
        agent_id: int,
        error_message: str,
        llm_model_id: int | None = None,
    ) -> None:
        msg = (error_message or "unknown error").strip()
        if len(msg) > ERROR_MESSAGE_MAX_LEN:
            msg = msg[: ERROR_MESSAGE_MAX_LEN - 3] + "..."

        def _mark() -> None:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(
                    MARK_ERROR_SQL,
                    {
                        "agent_id": agent_id,
                        "error_message": msg,
                        "llm_model_id": llm_model_id,
                    },
                )
            self._conn.commit()

        self._run_with_db_retry("mark_error", _mark)

    def requeue_errors(self, limit: int = 1000) -> int:
        """Re-open up to `limit` sticky-error agents onto the claim queue."""

        batch = max(1, min(int(limit), 5000))

        def _requeue() -> int:
            assert self._conn is not None
            with self._conn.cursor() as cur:
                cur.execute(REQUEUE_ERRORS_SQL, {"limit": batch})
                rows = list(cur.fetchall())
            self._conn.commit()
            return len(rows)

        return self._run_with_db_retry("requeue_errors", _requeue)
