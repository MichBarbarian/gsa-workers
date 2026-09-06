#!/usr/bin/env python3
"""First ~500 incoming transfers per wallet×chain — INSERT staging only."""

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

from ankr import AnkrClient
from blockscout import BlockscoutClient
from canonical import keep_oldest_incoming
from db import Database
from etherscan import EtherscanClient
from networks import CHAINS, GROUP_EVM_IDS
from okx import OkxDataClient
from quota import QuotaExhausted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("wallet_funding_transfers")

CLAIMED_BY_PREFIX = "wallet_funding_transfers/gha"
GENESIS = datetime(2015, 7, 30, tzinfo=timezone.utc)
CLAIM_RETRY_BASE_SECONDS = 2.0
XLAYER_RPC = "https://rpc.xlayer.tech"
# Active UTC hours: [18, 24) U [0, 12). Closed [12, 18).
SCHEDULE_WINDOW_START_HOUR = 18
SCHEDULE_WINDOW_END_HOUR = 12


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


def env_str(name: str, default: str = "") -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def ignore_schedule_window() -> bool:
    return env_str("IGNORE_SCHEDULE_WINDOW", "").lower() in ("1", "true", "yes")


def in_schedule_window(now: datetime | None = None) -> bool:
    """True during UTC 18:00→12:00 (cross-midnight); false during 12:00–18:00."""
    if ignore_schedule_window():
        return True
    hour = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).hour
    return hour >= SCHEDULE_WINDOW_START_HOUR or hour < SCHEDULE_WINDOW_END_HOUR


def build_claimed_by(worker_suffix: str) -> str:
    suffix = worker_suffix.strip() or "funding"
    if suffix.startswith(CLAIMED_BY_PREFIX):
        return suffix
    return f"{CLAIMED_BY_PREFIX}:{suffix}"


async def xlayer_latest_block(client: httpx.AsyncClient) -> int:
    resp = await client.post(
        XLAYER_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
    )
    resp.raise_for_status()
    return int(resp.json()["result"], 16)


async def fetch_for_row(
    row: dict,
    *,
    etherscan: EtherscanClient | None,
    blockscout: BlockscoutClient | None,
    ankr: AnkrClient | None,
    okx: OkxDataClient | None,
    http_client: httpx.AsyncClient,
    now: datetime,
    max_transfers: int,
) -> list[dict]:
    evm_id = int(row["evm_chain_id"])
    meta = CHAINS[evm_id]
    chain_pk = int(row["chain_id"])
    wallet_id = int(row["wallet_id"])
    address = str(row["address"])
    window_start = GENESIS
    window_end = now
    group = meta["group"]
    rows: list[dict] = []

    if group == "etherscan":
        if etherscan is None:
            raise RuntimeError("ETHERSCAN_FUNDING_KEY required")
        rows = await etherscan.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            evm_chain_id=evm_id,
            address=address,
            window_start=window_start,
            window_end=window_end,
        )
    elif group == "blockscout":
        if blockscout is None:
            raise RuntimeError("BLOCKSCOUT_FUNDING_KEY required")
        rows = await blockscout.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            evm_chain_id=evm_id,
            address=address,
            window_start=window_start,
            window_end=window_end,
        )
    elif group == "bsc":
        if ankr is None:
            raise RuntimeError("ANKR_FUNDING_KEY required")
        rows = await ankr.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            address=address,
            window_start=window_start,
            window_end=window_end,
        )
    elif group == "xlayer":
        if okx is None:
            raise RuntimeError("OKX HMAC secrets required")
        latest = await xlayer_latest_block(http_client)
        rows = await okx.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            address=address,
            from_block=0,
            to_block=latest,
            window_start=window_start,
            window_end=window_end,
        )
    else:
        raise RuntimeError(f"unsupported evm chain {evm_id}")

    return keep_oldest_incoming(rows, max_transfers)


async def run_job() -> int:
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        logger.error("SUPABASE_DB_URL is required")
        return 1

    now_utc = datetime.now(timezone.utc)
    if not in_schedule_window(now_utc):
        logger.info(
            "Outside UTC schedule window 18:00→12:00 (hour=%s); exiting",
            now_utc.hour,
        )
        return 0

    group = env_str("PROVIDER_GROUP", "etherscan")
    if group not in GROUP_EVM_IDS:
        logger.error("PROVIDER_GROUP must be one of %s", ",".join(GROUP_EVM_IDS))
        return 1
    evm_ids = list(GROUP_EVM_IDS[group])

    etherscan_key = env_str("ETHERSCAN_FUNDING_KEY")
    blockscout_key = env_str("BLOCKSCOUT_FUNDING_KEY")
    ankr_key = env_str("ANKR_FUNDING_KEY")
    okx_key = env_str("OKX_API_KEY")
    okx_secret = env_str("OKX_SECRET_KEY")
    okx_pass = env_str("OKX_PASSPHRASE")

    claimed_by = build_claimed_by(env_str("WORKER_ID", group))
    claim_batch_size = env_int("CLAIM_BATCH_SIZE", default=20, minimum=1)
    claim_stale_seconds = env_int("CLAIM_STALE_SECONDS", default=7200, minimum=60)
    max_runtime_seconds = env_int("MAX_RUNTIME_SECONDS", default=19800, minimum=60)
    max_transfers = env_int("FUNDING_MAX_TRANSFERS", default=500, minimum=1, maximum=2000)

    db = Database(dsn)
    db.connect()
    logger.info(
        "Started claimed_by=%s group=%s evm_ids=%s claim_batch=%s max_runtime=%ss max_transfers=%s",
        claimed_by,
        group,
        evm_ids,
        claim_batch_size,
        max_runtime_seconds,
        max_transfers,
    )

    start = time.monotonic()
    processed = 0
    completed = 0
    errors = 0

    try:
        async with httpx.AsyncClient(timeout=60.0) as http_client:
            etherscan = EtherscanClient(http_client, etherscan_key) if etherscan_key else None
            blockscout = BlockscoutClient(http_client, blockscout_key) if blockscout_key else None
            ankr = AnkrClient(http_client, ankr_key) if ankr_key else None
            okx = (
                OkxDataClient(
                    http_client,
                    api_key=okx_key,
                    secret=okx_secret,
                    passphrase=okx_pass,
                )
                if okx_key and okx_secret and okx_pass
                else None
            )

            while True:
                elapsed = time.monotonic() - start
                if elapsed >= max_runtime_seconds:
                    logger.info(
                        "Time budget reached (%.0fs). processed=%s completed=%s errors=%s",
                        elapsed,
                        processed,
                        completed,
                        errors,
                    )
                    return 0
                if not in_schedule_window():
                    logger.info(
                        "UTC schedule window closed (12:00–18:00). "
                        "processed=%s completed=%s errors=%s",
                        processed,
                        completed,
                        errors,
                    )
                    return 0

                try:
                    batch = db.claim_rows(
                        claimed_by,
                        claim_batch_size,
                        claim_stale_seconds,
                        evm_ids,
                    )
                except Exception:
                    logger.exception("Claim failed; retrying")
                    await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                    continue

                if not batch:
                    logger.info(
                        "Queue empty. processed=%s completed=%s errors=%s",
                        processed,
                        completed,
                        errors,
                    )
                    return 0

                logger.info("Claimed batch size=%s", len(batch))
                now = datetime.now(timezone.utc)
                pending_ids = [int(r["id"]) for r in batch]
                for row in batch:
                    processed += 1
                    row_id = int(row["id"])
                    try:
                        transfers = await fetch_for_row(
                            row,
                            etherscan=etherscan,
                            blockscout=blockscout,
                            ankr=ankr,
                            okx=okx,
                            http_client=http_client,
                            now=now,
                            max_transfers=max_transfers,
                        )
                        msg = db.insert_and_mark_done(row_id, transfers)
                        pending_ids.remove(row_id)
                        completed += 1
                        logger.info(
                            "Done wt_id=%s wallet_id=%s chain=%s rows=%s %s",
                            row_id,
                            row["wallet_id"],
                            row["evm_chain_id"],
                            len(transfers),
                            msg,
                        )
                    except QuotaExhausted as exc:
                        logger.warning("%s_QUOTA_EXHAUSTED stop_claim=1 detail=%s", exc.provider.upper(), exc)
                        leftover = [row_id, *pending_ids]
                        leftover = list(dict.fromkeys(leftover))
                        try:
                            db.unlock_rows(leftover)
                        except Exception:
                            logger.exception("unlock after quota failed")
                        return 0
                    except Exception as exc:
                        errors += 1
                        pending_ids.remove(row_id)
                        logger.error(
                            "Error wt_id=%s: %s\n%s",
                            row_id,
                            exc,
                            traceback.format_exc(),
                        )
                        try:
                            db.mark_error(row_id, str(exc))
                        except Exception:
                            logger.exception("mark_error failed wt_id=%s", row_id)
    finally:
        db.close()

    return 0


def main() -> int:
    try:
        return asyncio.run(run_job())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
