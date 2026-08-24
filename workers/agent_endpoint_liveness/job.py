#!/usr/bin/env python3
"""15d HTTP reachability census for agent_metadata_services locators."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from db import CLAIM_RETRY_BASE_SECONDS, Database
from probe import USER_AGENT, ProbeResult, probe_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("agent_endpoint_liveness")

CLAIMED_BY_PREFIX = "agent_endpoint_liveness/gha"


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
    suffix = worker_suffix.strip() or "liveness-a"
    if suffix.startswith(CLAIMED_BY_PREFIX):
        return suffix
    return f"{CLAIMED_BY_PREFIX}:{suffix}"


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower() or "_"


def _complete_row(row: dict, result: ProbeResult) -> dict:
    return {
        "agent_id": int(row["agent_id"]),
        "endpoint_normalized": row["endpoint_normalized"],
        "is_reachable": result.is_reachable,
        "latency_ms": result.latency_ms,
        "http_status": result.http_status,
        "error_message": result.error_message,
        "check_method": result.check_method,
    }


async def run_job() -> int:
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        logger.error("SUPABASE_DB_URL is required")
        return 1

    claimed_by = build_claimed_by(env_str("WORKER_ID", "liveness-a"))
    concurrency = env_int("CONCURRENCY", default=20, minimum=1, maximum=60)
    per_host = env_int("PER_HOST_CONCURRENCY", default=2, minimum=1, maximum=8)
    claim_batch_size = env_int("CLAIM_BATCH_SIZE", default=40, minimum=1)
    claim_stale_seconds = env_int("CLAIM_STALE_SECONDS", default=7200, minimum=60)
    max_runtime_seconds = env_int("MAX_RUNTIME_SECONDS", default=19800, minimum=60)

    db = Database(dsn)
    db.connect()
    logger.info(
        "Started claimed_by=%s concurrency=%s per_host=%s claim_batch_size=%s "
        "claim_stale_seconds=%s max_runtime=%ss",
        claimed_by,
        concurrency,
        per_host,
        claim_batch_size,
        claim_stale_seconds,
        max_runtime_seconds,
    )

    start = time.monotonic()
    processed = 0
    reachable = 0
    errors = 0
    global_sem = asyncio.Semaphore(concurrency)
    host_sems: dict[str, asyncio.Semaphore] = {}
    db_lock = asyncio.Lock()
    http_limits = httpx.Limits(max_connections=80, max_keepalive_connections=20)

    try:
        try:
            sync_stats = db.sync()
            logger.info("Queue sync %s", sync_stats)
        except Exception as exc:
            logger.error("Queue sync failed: %s", exc)
            return 1

        headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
        async with httpx.AsyncClient(
            headers=headers,
            limits=http_limits,
            max_redirects=5,
        ) as http_client:
            while True:
                elapsed = time.monotonic() - start
                if elapsed >= max_runtime_seconds:
                    logger.info(
                        "Time budget reached (%.0fs). processed=%s reachable=%s errors=%s",
                        elapsed,
                        processed,
                        reachable,
                        errors,
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

                groups: dict[str, list[dict]] = defaultdict(list)
                for row in rows:
                    groups[str(row["endpoint_normalized"])].append(row)

                logger.info(
                    "Claimed batch size=%s unique_urls=%s",
                    len(rows),
                    len(groups),
                )

                async def probe_group(
                    url: str, group_rows: list[dict]
                ) -> tuple[str, ProbeResult]:
                    host = _host(url)
                    if host not in host_sems:
                        host_sems[host] = asyncio.Semaphore(per_host)
                    async with global_sem, host_sems[host]:
                        result = await probe_url(http_client, url)
                    return url, result

                outcomes = await asyncio.gather(
                    *(probe_group(url, group_rows) for url, group_rows in groups.items()),
                    return_exceptions=True,
                )

                complete_rows: list[dict] = []
                for item in outcomes:
                    if isinstance(item, BaseException):
                        logger.error("Probe group failed: %s", item)
                        continue
                    url, result = item
                    for row in groups[url]:
                        complete_rows.append(_complete_row(row, result))
                        processed += 1
                        if result.is_reachable:
                            reachable += 1
                        else:
                            errors += 1

                if complete_rows:
                    async with db_lock:
                        try:
                            updated = db.complete_batch(complete_rows)
                            logger.info("Completed batch updated=%s", updated)
                        except Exception as exc:
                            logger.error("complete_batch failed: %s", exc)

    except Exception:
        logger.error("Critical job failure:\n%s", traceback.format_exc())
        return 1
    finally:
        db.close()

    logger.info(
        "Finished claimed_by=%s processed=%s reachable=%s errors=%s elapsed=%.0fs",
        claimed_by,
        processed,
        reachable,
        errors,
        time.monotonic() - start,
    )
    return 0


def main() -> None:
    load_dotenv_if_present()
    raise SystemExit(asyncio.run(run_job()))


if __name__ == "__main__":
    main()
