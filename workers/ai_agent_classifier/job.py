#!/usr/bin/env python3
"""Classify web_dashboard.agents with LLM categories (process_code=agent-classifier)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
import time
import traceback
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Literal

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from db import CLAIM_RETRY_BASE_SECONDS, Database
from fingerprint import agent_input_hash
from llm_client import chat_completion
from prompt import (
    build_user_prompt,
    parse_classification_json,
    validate_classification,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ai_agent_classifier")

Outcome = Literal["ok", "error", "capacity"]
LLM_429_MAX_ATTEMPTS = 3
# Overall attempt ceiling for a single LLM call (covers 429 + transient network).
LLM_MAX_ATTEMPTS = 4
# NVIDIA NIM account RPM is shared across all slugs on the same API key.
NVIDIA_PROVIDER_ID = 7
# Base backoff (seconds) between retries on transient network errors.
NETWORK_RETRY_BASE_SECONDS = 1.5
# Connect timeout (seconds); short so a ConnectTimeout fails fast and retries cheaply.
LLM_CONNECT_TIMEOUT_SECONDS = 10.0
_RETRY_AFTER_MS_RE = re.compile(r"try again in\s+(\d+(?:\.\d+)?)\s*ms", re.IGNORECASE)
_RETRY_AFTER_S_RE = re.compile(
    r"try again in\s+(\d+(?:\.\d+)?)\s*s(?:ec(?:onds?)?)?",
    re.IGNORECASE,
)
_RETRY_AFTER_M_RE = re.compile(
    r"try again in\s+(\d+)m\s*(\d+(?:\.\d+)?)s",
    re.IGNORECASE,
)


def env_int(name: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        value = int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return value


def estimate_tokens(
    system_prompt: str,
    user_prompt: str,
    max_completion_tokens: int | None,
) -> int:
    prompt_chars = len(system_prompt) + len(user_prompt)
    prompt_est = max(prompt_chars // 4, 1)
    completion_est = max(int(max_completion_tokens or 0), 0)
    return prompt_est + completion_est


def model_has_capacity(
    model: dict[str, Any],
    *,
    estimate: int = 0,
    exhausted_ids: set[int] | None = None,
) -> bool:
    model_id = int(model["model_id"])
    if exhausted_ids and model_id in exhausted_ids:
        return False
    if not bool(model.get("has_limits")):
        return True
    used_req = int(model.get("request_total_today") or 0)
    limit_req = int(model.get("request_per_day") or 0)
    if used_req >= limit_req:
        return False
    tpd = model.get("tokents_per_day")
    if tpd is not None:
        used_tok = int(model.get("token_total_today") or 0)
        if used_tok >= int(tpd):
            return False
        if estimate > 0 and (used_tok + estimate) > int(tpd):
            return False
    return True


def pick_model(
    models: list[dict[str, Any]],
    *,
    estimate: int = 0,
    exhausted_ids: set[int] | None = None,
) -> dict[str, Any] | None:
    for model in models:
        if model_has_capacity(model, estimate=estimate, exhausted_ids=exhausted_ids):
            return model
    return None


def models_for_provider(
    models: list[dict[str, Any]], provider_id: int
) -> list[dict[str, Any]]:
    return [m for m in models if int(m["provider_id"]) == int(provider_id)]


def unique_providers(models: list[dict[str, Any]]) -> list[tuple[int, str]]:
    seen: dict[int, str] = {}
    for m in models:
        pid = int(m["provider_id"])
        if pid not in seen:
            seen[pid] = str(m["provider_name"])
    return list(seen.items())


def parse_provider_filter() -> set[str] | None:
    """Optional PROVIDERS=Groq,GEMINI filter (case-insensitive provider names)."""
    raw = os.environ.get("PROVIDERS")
    if raw is None or not raw.strip():
        return None
    names = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return names or None


def resolve_api_key(secret_name: str) -> str:
    key = os.environ.get(secret_name)
    if key is None or not str(key).strip():
        raise RuntimeError(f"Missing API key env for provider secret={secret_name!r}")
    return str(key).strip()


def nvidia_account_rpm() -> int:
    return env_int("NVIDIA_ACCOUNT_RPM", default=30, minimum=1, maximum=60)


def provider_concurrency(provider_name: str, default: int) -> int:
    if provider_name.lower() == "nvidia":
        return env_int("NVIDIA_CONCURRENCY", default=1, minimum=1, maximum=5)
    return default


class RateLimiter:
    """Hardcap: at most request_per_minute calls per model in any rolling 60s window."""

    WINDOW_SECONDS = 60.0

    def __init__(self) -> None:
        self._windows: dict[int, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def wait(self, model_id: int, request_per_minute: int) -> None:
        rpm = max(1, int(request_per_minute or 1))
        while True:
            async with self._lock:
                now = time.monotonic()
                window = self._windows[model_id]
                while window and (now - window[0]) >= self.WINDOW_SECONDS:
                    window.popleft()
                if len(window) < rpm:
                    window.append(now)
                    return
                sleep_for = self.WINDOW_SECONDS - (now - window[0]) + 0.01
                logger.info(
                    "RPM hardcap model_id=%s rpm=%s in_window=%s; sleeping %.2fs",
                    model_id,
                    rpm,
                    len(window),
                    sleep_for,
                )
            await asyncio.sleep(max(sleep_for, 0.05))


class ProviderRateLimiter:
    """Hardcap: shared account RPM across all models of one provider (NVIDIA NIM)."""

    WINDOW_SECONDS = 60.0

    def __init__(self) -> None:
        self._windows: dict[int, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def wait(self, provider_id: int, request_per_minute: int) -> None:
        rpm = max(1, int(request_per_minute or 1))
        while True:
            async with self._lock:
                now = time.monotonic()
                window = self._windows[provider_id]
                while window and (now - window[0]) >= self.WINDOW_SECONDS:
                    window.popleft()
                if len(window) < rpm:
                    window.append(now)
                    return
                sleep_for = self.WINDOW_SECONDS - (now - window[0]) + 0.01
                logger.info(
                    "Provider RPM hardcap provider_id=%s rpm=%s in_window=%s; "
                    "sleeping %.2fs",
                    provider_id,
                    rpm,
                    len(window),
                    sleep_for,
                )
            await asyncio.sleep(max(sleep_for, 0.05))


class TokenMinuteLimiter:
    """Hardcap sliding window of token usage per model (TPM)."""

    WINDOW_SECONDS = 60.0

    def __init__(self) -> None:
        self._windows: dict[int, deque[tuple[float, int]]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def wait(
        self,
        model_id: int,
        tokens_per_minute: int | None,
        estimate: int,
    ) -> None:
        if tokens_per_minute is None:
            return
        tpm = int(tokens_per_minute)
        if tpm <= 0:
            return
        est = max(int(estimate), 1)
        while True:
            async with self._lock:
                now = time.monotonic()
                window = self._windows[model_id]
                while window and (now - window[0][0]) >= self.WINDOW_SECONDS:
                    window.popleft()
                used = sum(t for _, t in window)
                if used + est <= tpm:
                    # Reserve estimate; settled later with record().
                    window.append((now, est))
                    return
                sleep_for = self.WINDOW_SECONDS - (now - window[0][0]) + 0.01
                logger.info(
                    "TPM hardcap model_id=%s tpm=%s used=%s est=%s; sleeping %.2fs",
                    model_id,
                    tpm,
                    used,
                    est,
                    sleep_for,
                )
            await asyncio.sleep(max(sleep_for, 0.05))

    async def record(self, model_id: int, actual_tokens: int, reserved_estimate: int) -> None:
        """Replace last reserved estimate entry with actual usage when possible."""
        async with self._lock:
            window = self._windows[model_id]
            if not window:
                window.append((time.monotonic(), max(actual_tokens, 0)))
                return
            # Prefer adjusting the most recent reservation matching estimate.
            ts, prev = window[-1]
            if prev == reserved_estimate:
                window[-1] = (ts, max(actual_tokens, 0))
            else:
                window.append((time.monotonic(), max(actual_tokens, 0)))


_NETWORK_EXC = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.ReadError,
    httpx.WriteError,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
)


def _is_network_error(exc: BaseException) -> bool:
    """Transient transport failure (connect/read/write timeout, reset, pool)."""
    return isinstance(exc, _NETWORK_EXC)


def _is_http_429(exc: BaseException) -> bool:
    return "HTTP 429" in str(exc)


def _is_tpd_429(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "tokens per day" in text or "(tpd)" in text or "token per day" in text


def _is_tpm_429(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "tokens per minute" in text or "(tpm)" in text or "token per minute" in text


def _retry_after_seconds(exc: BaseException, attempt: int) -> float:
    text = str(exc)
    m_m = _RETRY_AFTER_M_RE.search(text)
    if m_m:
        return max(float(m_m.group(1)) * 60.0 + float(m_m.group(2)), 0.05)
    m_ms = _RETRY_AFTER_MS_RE.search(text)
    if m_ms:
        return max(float(m_ms.group(1)) / 1000.0, 0.05)
    m_s = _RETRY_AFTER_S_RE.search(text)
    if m_s:
        return max(float(m_s.group(1)), 0.05)
    return float(2 ** (attempt - 1))


class DailyTokenExhausted(Exception):
    """Model hit provider TPD; skip for this run."""

    def __init__(self, model_id: int, message: str):
        super().__init__(message)
        self.model_id = model_id


class ModelRateLimited(Exception):
    """Model hit account RPM after retries; skip for this run."""

    def __init__(self, model_id: int, message: str):
        super().__init__(message)
        self.model_id = model_id


def _is_rpm_429(exc: BaseException) -> bool:
    return (
        _is_http_429(exc)
        and not _is_tpd_429(exc)
        and not _is_tpm_429(exc)
    )


async def call_llm_with_retries(
    http_client: httpx.AsyncClient,
    *,
    model: dict[str, Any],
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    estimate: int,
    rate_limiter: RateLimiter,
    provider_rate_limiter: ProviderRateLimiter,
    token_minute_limiter: TokenMinuteLimiter,
    bump_model_usage,
) -> str:
    model_id = int(model["model_id"])
    provider_id = int(model["provider_id"])
    rpm = int(model["request_per_minute"] or 1)
    tpm = model.get("tokens_per_minute")
    tpm_i = int(tpm) if tpm is not None else None
    last_exc: Exception | None = None

    for attempt in range(1, LLM_MAX_ATTEMPTS + 1):
        if provider_id == NVIDIA_PROVIDER_ID:
            await provider_rate_limiter.wait(
                provider_id, nvidia_account_rpm()
            )
        else:
            await rate_limiter.wait(model_id, rpm)
        await token_minute_limiter.wait(model_id, tpm_i, estimate)
        try:
            raw, total_tokens = await chat_completion(
                http_client,
                base_url=str(model["base_url"]),
                api_key=api_key,
                model_slug=str(model["model_slug"]),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=(
                    None
                    if model.get("temperature") is None
                    else float(model["temperature"])
                ),
                max_completion_tokens=(
                    None
                    if model.get("max_completion_tokens") is None
                    else int(model["max_completion_tokens"])
                ),
                response_format=(
                    None
                    if model.get("response_format") is None
                    else str(model["response_format"])
                ),
                thinking_off=bool(model.get("does_need_thinking_off_parameter")),
            )
            await token_minute_limiter.record(model_id, total_tokens, estimate)
            await bump_model_usage(model_id, total_tokens)
            return raw
        except Exception as exc:
            last_exc = exc
            if _is_http_429(exc) and _is_tpd_429(exc):
                logger.warning(
                    "LLM TPD 429 model_id=%s; marking exhausted for this run",
                    model_id,
                )
                raise DailyTokenExhausted(model_id, str(exc)) from exc
            if (
                _is_http_429(exc)
                and _is_tpm_429(exc)
                and attempt < LLM_429_MAX_ATTEMPTS
            ):
                delay = _retry_after_seconds(exc, attempt)
                # Cap TPM retries; avoid multi-minute sleeps from misclassified errors.
                delay = min(delay, 15.0)
                logger.warning(
                    "LLM TPM 429 model_id=%s attempt=%s/%s; sleeping %.2fs",
                    model_id,
                    attempt,
                    LLM_429_MAX_ATTEMPTS,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            if _is_http_429(exc) and attempt < LLM_429_MAX_ATTEMPTS:
                delay = min(_retry_after_seconds(exc, attempt), 15.0)
                logger.warning(
                    "LLM HTTP 429 model_id=%s attempt=%s/%s; sleeping %.2fs",
                    model_id,
                    attempt,
                    LLM_429_MAX_ATTEMPTS,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            if _is_network_error(exc) and attempt < LLM_MAX_ATTEMPTS:
                delay = min(NETWORK_RETRY_BASE_SECONDS * attempt, 8.0)
                logger.warning(
                    "LLM network error model_id=%s attempt=%s/%s (%s); "
                    "retrying in %.1fs",
                    model_id,
                    attempt,
                    LLM_MAX_ATTEMPTS,
                    exc.__class__.__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            if _is_rpm_429(exc):
                logger.warning(
                    "LLM RPM 429 model_id=%s after %s attempts; "
                    "marking exhausted for this run",
                    model_id,
                    attempt,
                )
                raise ModelRateLimited(model_id, str(exc)) from exc
            raise
    assert last_exc is not None
    if _is_rpm_429(last_exc):
        raise ModelRateLimited(model_id, str(last_exc)) from last_exc
    raise last_exc


async def run_job() -> int:
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        logger.error("SUPABASE_DB_URL is required")
        return 1

    claim_batch_size = env_int("CLAIM_BATCH_SIZE", default=20, minimum=1, maximum=200)
    requeue_batch_size = env_int(
        "REQUEUE_ERROR_BATCH_SIZE", default=1000, minimum=1, maximum=5000
    )
    concurrency = env_int("CONCURRENCY", default=1, minimum=1, maximum=5)
    max_runtime_seconds = env_int("MAX_RUNTIME_SECONDS", default=19800, minimum=60)
    provider_filter = parse_provider_filter()

    db = Database(dsn)
    db.connect()

    try:
        categories = db.load_active_categories()
        system_prompt = db.load_system_prompt()
    except Exception as exc:
        logger.error("Failed to load categories/system_prompt: %s", exc)
        db.close()
        return 1

    if not categories:
        logger.error("No active categories in web_dashboard.agent_ai_categories")
        db.close()
        return 1

    if not system_prompt:
        logger.error(
            "Missing llm.process.system_prompt for process_code=agent-classifier"
        )
        db.close()
        return 1

    allowed = set(categories)
    start = time.monotonic()
    stats_lock = asyncio.Lock()
    processed = 0
    completed = 0
    errors = 0
    db_lock = asyncio.Lock()
    rate_limiter = RateLimiter()
    provider_rate_limiter = ProviderRateLimiter()
    token_minute_limiter = TokenMinuteLimiter()
    models_cache: list[dict[str, Any]] = []
    exhausted_models: set[int] = set()
    http_limits = httpx.Limits(max_connections=40, max_keepalive_connections=20)

    async def refresh_models() -> list[dict[str, Any]]:
        nonlocal models_cache
        async with db_lock:
            models_cache = db.load_process_models()
        return models_cache

    async def bump_model_usage(model_id: int, tokens: int) -> None:
        async with db_lock:
            totals = db.increment_model_request(model_id, tokens=tokens)
            for i, m in enumerate(models_cache):
                if int(m["model_id"]) == model_id:
                    models_cache[i] = {
                        **m,
                        "request_total_today": totals["request_total"],
                        "token_total_today": totals["token_total"],
                    }
                    break

    async def record_outcome(outcome: Outcome) -> None:
        nonlocal processed, completed, errors
        async with stats_lock:
            if outcome == "ok":
                processed += 1
                completed += 1
            elif outcome == "error":
                processed += 1
                errors += 1

    try:
        await refresh_models()
        if not models_cache:
            logger.error(
                "No active models for process_code=agent-classifier. "
                "Check llm.procees_llm_providers + llm.models."
            )
            return 1

        providers = unique_providers(models_cache)
        if provider_filter is not None:
            providers = [
                (pid, pname)
                for pid, pname in providers
                if pname.lower() in provider_filter
            ]
            if not providers:
                logger.error(
                    "PROVIDERS filter matched no active providers: %s",
                    sorted(provider_filter),
                )
                return 1

        for model in models_cache:
            if provider_filter is not None and str(model["provider_name"]).lower() not in provider_filter:
                continue
            resolve_api_key(str(model["provider_secret"]))

        logger.info(
            "Started categories=%s system_prompt_chars=%s requeue_errors=lazy "
            "requeue_batch_size=%s providers=%s claim_batch_size=%s "
            "concurrency_per_provider=%s max_runtime=%ss",
            len(categories),
            len(system_prompt),
            requeue_batch_size,
            [pname for _, pname in providers],
            claim_batch_size,
            concurrency,
            max_runtime_seconds,
        )

        # NIM frontier models (MiniMax / Kimi) often need 60–90s; keep connect short.
        http_timeout = httpx.Timeout(120.0, connect=LLM_CONNECT_TIMEOUT_SECONDS)
        async with httpx.AsyncClient(
            timeout=http_timeout, limits=http_limits
        ) as http_client:

            async def provider_loop(provider_id: int, provider_name: str) -> None:
                lane_concurrency = provider_concurrency(provider_name, concurrency)
                sem = asyncio.Semaphore(lane_concurrency)
                provider_processed = 0
                provider_completed = 0
                provider_errors = 0
                logger.info(
                    "Provider worker start name=%s provider_id=%s concurrency=%s",
                    provider_name,
                    provider_id,
                    lane_concurrency,
                )

                while True:
                    elapsed = time.monotonic() - start
                    if elapsed >= max_runtime_seconds:
                        logger.info(
                            "Provider %s time budget reached (%.0fs). "
                            "local_processed=%s completed=%s errors=%s",
                            provider_name,
                            elapsed,
                            provider_processed,
                            provider_completed,
                            provider_errors,
                        )
                        break

                    await refresh_models()
                    provider_models = models_for_provider(models_cache, provider_id)
                    models_available = (
                        pick_model(provider_models, exhausted_ids=exhausted_models)
                        is not None
                    )
                    if not models_available:
                        logger.info(
                            "Provider %s: no LLM capacity; exact-match copies only",
                            provider_name,
                        )

                    async with db_lock:
                        try:
                            rows = db.claim_agents(limit=claim_batch_size)
                            if not rows:
                                try:
                                    n = db.requeue_errors(limit=requeue_batch_size)
                                except Exception as requeue_exc:
                                    logger.warning(
                                        "Provider %s requeue_errors failed; "
                                        "treating queue as empty: %s",
                                        provider_name,
                                        requeue_exc,
                                    )
                                    n = 0
                                if n > 0:
                                    logger.info(
                                        "Provider %s requeued prior errors batch=%s",
                                        provider_name,
                                        n,
                                    )
                                    rows = db.claim_agents(limit=claim_batch_size)
                        except Exception as exc:
                            logger.error(
                                "Provider %s claim failed; retry: %s",
                                provider_name,
                                exc,
                            )
                            await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                            continue

                    if not rows:
                        if provider_processed == 0:
                            logger.info(
                                "Provider %s: no pending agents. Exiting.",
                                provider_name,
                            )
                        else:
                            logger.info(
                                "Provider %s: no more pending agents.",
                                provider_name,
                            )
                        break

                    logger.info(
                        "Provider %s claimed batch size=%s first_id=%s last_id=%s",
                        provider_name,
                        len(rows),
                        rows[0]["id"],
                        rows[-1]["id"],
                    )

                    async def handle_agent(agent: dict[str, Any]) -> Outcome:
                        agent_id = int(agent["id"])
                        async with sem:
                            model: dict[str, Any] | None = None
                            try:
                                input_hash = agent_input_hash(agent)
                                async with db_lock:
                                    donor = db.find_classification_donor(
                                        agent_id=agent_id,
                                        input_hash=input_hash,
                                    )
                                    if donor is not None:
                                        secondary = donor.get("ai_category_secondary")
                                        if secondary is None:
                                            secondary = []
                                        db.mark_success(
                                            agent_id=agent_id,
                                            llm_model_id=(
                                                None
                                                if donor.get("llm_model_id") is None
                                                else int(donor["llm_model_id"])
                                            ),
                                            primary_category=str(
                                                donor["ai_category_primary"]
                                            ),
                                            secondary_categories=secondary,
                                            confidence=(
                                                None
                                                if donor.get("ai_category_confidence")
                                                is None
                                                else float(
                                                    donor["ai_category_confidence"]
                                                )
                                            ),
                                            reasoning=donor.get(
                                                "ai_category_reasoning"
                                            ),
                                            agent_purpose=donor.get(
                                                "ai_category_purpose"
                                            ),
                                            input_hash=input_hash,
                                        )
                                        logger.info(
                                            "Copied agent_id=%s from=%s primary=%s "
                                            "provider=%s",
                                            agent_id,
                                            donor["id"],
                                            donor["ai_category_primary"],
                                            provider_name,
                                        )
                                        return "ok"

                                user_prompt = build_user_prompt(
                                    categories=categories,
                                    agent=agent,
                                )
                                async with db_lock:
                                    current = models_for_provider(
                                        db.load_process_models(), provider_id
                                    )
                                    model = pick_model(
                                        current,
                                        exhausted_ids=exhausted_models,
                                    )
                                    if model is not None:
                                        est = estimate_tokens(
                                            system_prompt,
                                            user_prompt,
                                            (
                                                None
                                                if model.get("max_completion_tokens")
                                                is None
                                                else int(
                                                    model["max_completion_tokens"]
                                                )
                                            ),
                                        )
                                        if not model_has_capacity(
                                            model,
                                            estimate=est,
                                            exhausted_ids=exhausted_models,
                                        ):
                                            model = pick_model(
                                                current,
                                                estimate=est,
                                                exhausted_ids=exhausted_models,
                                            )

                                if model is None:
                                    logger.info(
                                        "No model capacity provider=%s agent_id=%s; "
                                        "leaving pending",
                                        provider_name,
                                        agent_id,
                                    )
                                    return "capacity"

                                est = estimate_tokens(
                                    system_prompt,
                                    user_prompt,
                                    (
                                        None
                                        if model.get("max_completion_tokens") is None
                                        else int(model["max_completion_tokens"])
                                    ),
                                )
                                api_key = resolve_api_key(
                                    str(model["provider_secret"])
                                )
                                raw = await call_llm_with_retries(
                                    http_client,
                                    model=model,
                                    api_key=api_key,
                                    system_prompt=system_prompt,
                                    user_prompt=user_prompt,
                                    estimate=est,
                                    rate_limiter=rate_limiter,
                                    provider_rate_limiter=provider_rate_limiter,
                                    token_minute_limiter=token_minute_limiter,
                                    bump_model_usage=bump_model_usage,
                                )

                                parsed = parse_classification_json(raw)
                                validated = validate_classification(
                                    parsed,
                                    allowed_categories=allowed,
                                )
                                async with db_lock:
                                    db.mark_success(
                                        agent_id=agent_id,
                                        llm_model_id=int(model["model_id"]),
                                        primary_category=validated[
                                            "primary_category"
                                        ],
                                        secondary_categories=validated[
                                            "secondary_categories"
                                        ],
                                        confidence=validated["confidence"],
                                        reasoning=validated["reasoning"],
                                        agent_purpose=validated["agent_purpose"],
                                        input_hash=input_hash,
                                    )
                                logger.info(
                                    "Done agent_id=%s model_id=%s primary=%s "
                                    "provider=%s",
                                    agent_id,
                                    model["model_id"],
                                    validated["primary_category"],
                                    provider_name,
                                )
                                return "ok"
                            except (DailyTokenExhausted, ModelRateLimited) as exc:
                                exhausted_models.add(int(exc.model_id))
                                reason = (
                                    "TPD exhausted"
                                    if isinstance(exc, DailyTokenExhausted)
                                    else "RPM rate-limited"
                                )
                                logger.info(
                                    "Agent id=%s deferred; model_id=%s %s "
                                    "provider=%s",
                                    agent_id,
                                    exc.model_id,
                                    reason,
                                    provider_name,
                                )
                                return "capacity"
                            except Exception as exc:
                                err_text = f"{exc.__class__.__name__}: {exc}"
                                logger.warning(
                                    "Agent id=%s failed provider=%s: %s",
                                    agent_id,
                                    provider_name,
                                    err_text,
                                )
                                try:
                                    async with db_lock:
                                        db.mark_error(
                                            agent_id=agent_id,
                                            error_message=err_text,
                                            llm_model_id=(
                                                None
                                                if model is None
                                                else int(model["model_id"])
                                            ),
                                        )
                                except Exception as mark_exc:
                                    logger.error(
                                        "mark_error failed agent_id=%s: %s",
                                        agent_id,
                                        mark_exc,
                                    )
                                return "error"

                    outcomes = await asyncio.gather(
                        *(handle_agent(row) for row in rows)
                    )
                    capacity_hit = False
                    batch_ok = 0
                    for outcome in outcomes:
                        await record_outcome(outcome)
                        if outcome == "ok":
                            provider_processed += 1
                            provider_completed += 1
                            batch_ok += 1
                        elif outcome == "error":
                            provider_processed += 1
                            provider_errors += 1
                        else:
                            capacity_hit = True

                    await refresh_models()
                    provider_models = models_for_provider(models_cache, provider_id)
                    models_available = (
                        pick_model(provider_models, exhausted_ids=exhausted_models)
                        is not None
                    )
                    if not models_available and batch_ok == 0 and capacity_hit:
                        logger.info(
                            "Provider %s: no LLM capacity and no copies in batch. "
                            "Exiting worker.",
                            provider_name,
                        )
                        break

                logger.info(
                    "Provider worker done name=%s processed=%s completed=%s errors=%s",
                    provider_name,
                    provider_processed,
                    provider_completed,
                    provider_errors,
                )

            await asyncio.gather(
                *(provider_loop(pid, pname) for pid, pname in providers)
            )

    except Exception:
        logger.error("Critical job failure:\n%s", traceback.format_exc())
        return 1
    finally:
        db.close()

    logger.info(
        "Finished processed=%s completed=%s errors=%s elapsed=%.0fs",
        processed,
        completed,
        errors,
        time.monotonic() - start,
    )
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run_job()))


if __name__ == "__main__":
    main()
