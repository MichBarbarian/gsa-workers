"""Step: Ethos Proceso 2 — claim needs_history_fetch → skip Goldsky signals → complete.

Goldsky 1.1.0 is Profile+Address only. Reviews are filled by ethos_reviews_api.
This step drains the late-link history queue without GraphQL signal fetches.
"""

from __future__ import annotations

import asyncio
import logging
import time

from db_common import CLAIM_RETRY_BASE_SECONDS
from ethos.goldsky import DEFAULT_GOLDSKY_URL, fetch_all_signals
from ethos.signal_upsert import map_all
from steps.base import StepContext, StepResult

logger = logging.getLogger("on_demand_backfill")


class EthosHistoryStep:
    name = "ethos_history"

    async def run(self, ctx: StepContext) -> StepResult:
        goldsky_url = str(ctx.env.get("goldsky_ethos_url") or DEFAULT_GOLDSKY_URL)
        claim_batch_size = int(ctx.env.get("ethos_claim_batch_size", 10))
        claim_stale_seconds = int(ctx.env.get("ethos_claim_stale_seconds", 7200))
        concurrency = int(ctx.env.get("ethos_concurrency", 3))
        claimed_by = ctx.worker_id

        processed = 0
        errors = 0
        any_batch = False
        sem = asyncio.Semaphore(concurrency)

        while True:
            if time.monotonic() >= ctx.deadline:
                logger.info("Time budget reached during ethos_history")
                break

            async with ctx.db_lock:
                try:
                    profile_ids = ctx.db.claim_history(
                        worker_id=claimed_by,
                        limit=claim_batch_size,
                        stale_seconds=claim_stale_seconds,
                    )
                except Exception as exc:
                    logger.error("claim_history failed; retrying: %s", exc)
                    await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                    continue

            if not profile_ids:
                if not any_batch:
                    logger.info("No pending history fetch. Skipping step.")
                    return StepResult(skipped_empty=True)
                logger.info("History queue empty.")
                break

            any_batch = True
            logger.info(
                "Claimed history batch size=%s first=%s last=%s",
                len(profile_ids),
                profile_ids[0],
                profile_ids[-1],
            )

            async def handle(pid: int) -> bool:
                async with sem:
                    try:
                        raw = await fetch_all_signals(
                            ctx.http, url=goldsky_url, profile_id=pid
                        )
                        mapped = map_all(raw)
                        async with ctx.db_lock:
                            for entity, rows in mapped.items():
                                n = ctx.db.upsert_ethos_entity_rows(entity, rows)
                                logger.info(
                                    "Upserted entity=%s profile_id=%s rows=%s",
                                    entity,
                                    pid,
                                    n,
                                )
                            ctx.db.complete_history(pid)
                        logger.info("Done history profile_id=%s", pid)
                        return True
                    except Exception as exc:
                        logger.warning(
                            "History profile_id=%s failed: %s: %s",
                            pid,
                            exc.__class__.__name__,
                            exc,
                        )
                        return False

            outcomes = await asyncio.gather(*(handle(pid) for pid in profile_ids))
            for ok in outcomes:
                processed += 1
                if not ok:
                    errors += 1

        return StepResult(processed=processed, errors=errors, skipped_empty=False)
