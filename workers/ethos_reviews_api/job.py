#!/usr/bin/env python3
"""Fetch Ethos v2 reviews for GSA-linked profiles into ethos.reviews."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from activities_api import DEFAULT_ETHOS_API_BASE, ActivitiesClient
from db import CLAIM_RETRY_BASE_SECONDS, Database
from map_reviews import map_activities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("ethos_reviews_api")

CLAIMED_BY_PREFIX = "ethos_reviews_api/gha"
ETHOS_CLIENT_UA = "gsa-ethos-reviews/1.0"


def load_dotenv_if_present() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


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


def env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def build_claimed_by(worker_suffix: str) -> str:
    suffix = worker_suffix.strip() or "reviews-a"
    if suffix.startswith(CLAIMED_BY_PREFIX):
        return suffix
    return f"{CLAIMED_BY_PREFIX}:{suffix}"


def _since_unix(fetched_at: datetime | None) -> float | None:
    if fetched_at is None:
        return None
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    return fetched_at.timestamp()


async def run_job() -> int:
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        logger.error("SUPABASE_DB_URL is required")
        return 1

    claimed_by = build_claimed_by(env_str("WORKER_ID", "reviews-a"))
    concurrency = env_int("CONCURRENCY", default=3, minimum=1, maximum=20)
    claim_batch_size = env_int("CLAIM_BATCH_SIZE", default=10, minimum=1)
    claim_stale_seconds = env_int("CLAIM_STALE_SECONDS", default=7200, minimum=60)
    max_runtime_seconds = env_int("MAX_RUNTIME_SECONDS", default=19800, minimum=60)
    throttle_ms = env_int("THROTTLE_MS", default=200, minimum=0)
    upsert_chunk = env_int("UPSERT_CHUNK_SIZE", default=200, minimum=1)
    api_base = env_str("ETHOS_API_BASE", DEFAULT_ETHOS_API_BASE)

    db = Database(dsn)
    db.connect()
    logger.info(
        "Started claimed_by=%s concurrency=%s claim_batch_size=%s "
        "claim_stale_seconds=%s throttle_ms=%s max_runtime=%ss",
        claimed_by,
        concurrency,
        claim_batch_size,
        claim_stale_seconds,
        throttle_ms,
        max_runtime_seconds,
    )

    start = time.monotonic()
    processed = 0
    errors = 0
    upserted = 0
    sem = asyncio.Semaphore(concurrency)
    db_lock = asyncio.Lock()
    http_limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    try:
        async with httpx.AsyncClient(
            headers={"User-Agent": ETHOS_CLIENT_UA, "Accept": "application/json"},
            limits=http_limits,
            timeout=httpx.Timeout(60.0),
        ) as http_client:
            api = ActivitiesClient(
                http_client, base_url=api_base, throttle_ms=throttle_ms
            )
            while True:
                elapsed = time.monotonic() - start
                if elapsed >= max_runtime_seconds:
                    logger.info(
                        "Time budget reached (%.0fs). processed=%s errors=%s upserted=%s",
                        elapsed,
                        processed,
                        errors,
                        upserted,
                    )
                    break

                async with db_lock:
                    try:
                        rows = db.claim_rows(
                            worker_id=claimed_by,
                            limit=claim_batch_size,
                            stale_seconds=claim_stale_seconds,
                        )
                    except Exception as exc:
                        logger.error("Claim failed; will retry next loop: %s", exc)
                        await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                        continue

                if not rows:
                    if processed == 0:
                        logger.info("queue empty")
                    else:
                        logger.info("queue empty")
                    break

                logger.info(
                    "Claimed batch size=%s first=%s last=%s",
                    len(rows),
                    rows[0]["profile_id"],
                    rows[-1]["profile_id"],
                )

                async def handle(row: dict) -> tuple[int, bool, int]:
                    pid = int(row["profile_id"])
                    since = _since_unix(row.get("reviews_fetched_at"))
                    async with sem:
                        try:
                            raw = await api.fetch_reviews(pid, since_unix=since)
                            mapped = map_activities(raw)
                            n = 0
                            async with db_lock:
                                n = db.upsert_reviews(mapped, chunk_size=upsert_chunk)
                                db.complete(pid, True, "ok")
                            logger.info(
                                "Done profile_id=%s activities=%s upserted=%s incremental=%s",
                                pid,
                                len(raw),
                                n,
                                since is not None,
                            )
                            return pid, True, n
                        except Exception as exc:
                            logger.warning(
                                "profile_id=%s failed: %s: %s",
                                pid,
                                exc.__class__.__name__,
                                exc,
                            )
                            try:
                                async with db_lock:
                                    db.complete(pid, False, "error")
                            except Exception as complete_exc:
                                logger.error(
                                    "complete error profile_id=%s failed: %s",
                                    pid,
                                    complete_exc,
                                )
                            return pid, False, 0

                outcomes = await asyncio.gather(*(handle(row) for row in rows))
                for _pid, ok, n in outcomes:
                    processed += 1
                    upserted += n
                    if not ok:
                        errors += 1

    except Exception:
        logger.error("Critical job failure:\n%s", traceback.format_exc())
        return 1
    finally:
        db.close()

    logger.info(
        "Finished claimed_by=%s processed=%s errors=%s upserted=%s elapsed=%.0fs",
        claimed_by,
        processed,
        errors,
        upserted,
        time.monotonic() - start,
    )
    return 0


def main() -> None:
    load_dotenv_if_present()
    raise SystemExit(asyncio.run(run_job()))


if __name__ == "__main__":
    main()
