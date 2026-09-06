#!/usr/bin/env python3
"""Wallet activity transfers 15d — INSERT staging only."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from alchemy import AlchemyClient
from ankr import AnkrClient
from db import CLAIM_RETRY_BASE_SECONDS, Database
from etherscan import EtherscanClient
from networks import CHAINS, GROUP_EVM_IDS, LOOKBACK_DAYS, bsc_provider
from okx import OkxDataClient
from timestamps import fill_missing_timestamps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("wallet_activity_flows")

CLAIMED_BY_PREFIX = "wallet_activity_flows/gha"
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
    suffix = worker_suffix.strip() or "activity"
    if suffix.startswith(CLAIMED_BY_PREFIX):
        return suffix
    return f"{CLAIMED_BY_PREFIX}:{suffix}"


def lookback_from_block(latest: int, block_time_sec: float) -> int:
    span = int((LOOKBACK_DAYS * 86400) / max(block_time_sec, 0.1))
    return max(0, latest - span)


async def xlayer_latest_block(client: httpx.AsyncClient) -> int:
    resp = await client.post(
        XLAYER_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
    )
    resp.raise_for_status()
    return int(resp.json()["result"], 16)


async def fetch_for_row(
    http_client: httpx.AsyncClient,
    db: Database,
    row: dict,
    *,
    etherscan: EtherscanClient | None,
    alchemy_k1: AlchemyClient | None,
    alchemy_k2: AlchemyClient | None,
    ankr: AnkrClient | None,
    okx: OkxDataClient | None,
    now: datetime,
) -> list[dict]:
    evm_id = int(row["evm_chain_id"])
    meta = CHAINS[evm_id]
    chain_pk = int(row["chain_id"])
    wallet_id = int(row["wallet_id"])
    address = str(row["address"])
    subdomain = (row.get("subdomain_alchemy") or meta.get("alchemy_subdomain") or "").strip()
    window_end = now
    window_start = now - timedelta(days=LOOKBACK_DAYS)

    if meta["group"] == "etherscan":
        if etherscan is None:
            raise RuntimeError("ETHERSCAN_API_KEY required")
        latest = await etherscan.latest_block(evm_id)
        from_block = lookback_from_block(latest, meta["block_time_sec"])
        return await etherscan.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            evm_chain_id=evm_id,
            address=address,
            from_block=from_block,
            to_block=latest,
            window_start=window_start,
            window_end=window_end,
        )

    if meta["group"] == "alchemy_k1":
        if alchemy_k1 is None or not subdomain:
            raise RuntimeError("ALCHEMY_ACTIVITY_KEY_1 / subdomain required")
        latest = await alchemy_k1.latest_block(subdomain)
        from_block = lookback_from_block(latest, meta["block_time_sec"])
        rows = await alchemy_k1.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            address=address,
            subdomain=subdomain,
            from_block=from_block,
            window_start=window_start,
            window_end=window_end,
            with_metadata=evm_id != 100,
        )
        if evm_id == 100:
            await fill_missing_timestamps(
                rows,
                alchemy=alchemy_k1,
                subdomain=subdomain,
                db=db,
                chain_pk=chain_pk,
            )
        return rows

    if meta["group"] == "bsc":
        vendor = bsc_provider(now.day, now.date())
        if vendor == "alchemy":
            if alchemy_k2 is None:
                raise RuntimeError("ALCHEMY_ACTIVITY_KEY_2 required for BSC first cut")
            sub = subdomain or "bnb-mainnet"
            latest = await alchemy_k2.latest_block(sub)
            from_block = lookback_from_block(latest, meta["block_time_sec"])
            return await alchemy_k2.fetch_transfers(
                wallet_id=wallet_id,
                chain_pk=chain_pk,
                address=address,
                subdomain=sub,
                from_block=from_block,
                window_start=window_start,
                window_end=window_end,
                with_metadata=True,
            )
        if ankr is None:
            raise RuntimeError("ANKR_API_KEY required for BSC second cut")
        return await ankr.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            address=address,
            window_start=window_start,
            window_end=window_end,
        )

    if meta["group"] == "xlayer":
        if okx is None:
            raise RuntimeError("OKX HMAC secrets required")
        latest = await xlayer_latest_block(http_client)
        from_block = lookback_from_block(latest, meta["block_time_sec"])
        return await okx.fetch_transfers(
            wallet_id=wallet_id,
            chain_pk=chain_pk,
            address=address,
            from_block=from_block,
            to_block=latest,
            window_start=window_start,
            window_end=window_end,
        )

    raise RuntimeError(f"unsupported evm chain {evm_id}")


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

    etherscan_key = env_str("ETHERSCAN_API_KEY")
    alchemy_k1 = env_str("ALCHEMY_ACTIVITY_KEY_1")
    alchemy_k2 = env_str("ALCHEMY_ACTIVITY_KEY_2")
    ankr_key = env_str("ANKR_API_KEY")
    okx_key = env_str("OKX_API_KEY")
    okx_secret = env_str("OKX_SECRET_KEY")
    okx_pass = env_str("OKX_PASSPHRASE")

    claimed_by = build_claimed_by(env_str("WORKER_ID", group))
    claim_batch_size = env_int("CLAIM_BATCH_SIZE", default=20, minimum=1)
    claim_stale_seconds = env_int("CLAIM_STALE_SECONDS", default=7200, minimum=60)
    max_runtime_seconds = env_int("MAX_RUNTIME_SECONDS", default=19800, minimum=60)

    db = Database(dsn)
    db.connect()
    logger.info(
        "Started claimed_by=%s group=%s evm_ids=%s claim_batch=%s max_runtime=%ss",
        claimed_by,
        group,
        evm_ids,
        claim_batch_size,
        max_runtime_seconds,
    )
    if group == "bsc":
        logger.info(
            "BSC vendor=%s utc_date=%s (Alchemy through 2026-08-31 UTC)",
            bsc_provider(now_utc.day, now_utc.date()),
            now_utc.date().isoformat(),
        )

    start = time.monotonic()
    processed = 0
    completed = 0
    errors = 0

    try:
        async with httpx.AsyncClient(timeout=60.0) as http_client:
            etherscan = EtherscanClient(http_client, etherscan_key) if etherscan_key else None
            alk1 = AlchemyClient(http_client, alchemy_k1) if alchemy_k1 else None
            alk2 = AlchemyClient(http_client, alchemy_k2) if alchemy_k2 else None
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
                    claim_t0 = time.monotonic()
                    batch = db.claim_rows(
                        claimed_by,
                        claim_batch_size,
                        claim_stale_seconds,
                        evm_ids,
                    )
                    claim_ms = (time.monotonic() - claim_t0) * 1000.0
                except Exception:
                    logger.exception("Claim failed; retrying")
                    await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                    continue

                if not batch:
                    logger.info(
                        "Queue empty. processed=%s completed=%s errors=%s claim_ms=%.0f",
                        processed,
                        completed,
                        errors,
                        claim_ms,
                    )
                    return 0

                logger.info(
                    "Claimed batch size=%s claim_ms=%.0f",
                    len(batch),
                    claim_ms,
                )
                now = datetime.now(timezone.utc)
                for row in batch:
                    processed += 1
                    row_id = int(row["id"])
                    try:
                        transfers = await fetch_for_row(
                            http_client,
                            db,
                            row,
                            etherscan=etherscan,
                            alchemy_k1=alk1,
                            alchemy_k2=alk2,
                            ankr=ankr,
                            okx=okx,
                            now=now,
                        )
                        save_t0 = time.monotonic()
                        msg = db.insert_and_mark_done(row_id, transfers)
                        save_ms = (time.monotonic() - save_t0) * 1000.0
                        completed += 1
                        logger.info(
                            "Done wt_id=%s wallet_id=%s chain=%s rows=%s save_ms=%.0f %s",
                            row_id,
                            row["wallet_id"],
                            row["evm_chain_id"],
                            len(transfers),
                            save_ms,
                            msg,
                        )
                    except Exception as exc:
                        errors += 1
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
