"""EVM chain metadata for wallet_activity_flows."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

# Keyed by EVM chain_id.
CHAINS: dict[int, dict[str, Any]] = {
    1: {
        "slug": "ethereum",
        "block_time_sec": 12.0,
        "alchemy_subdomain": "eth-mainnet",
        "group": "etherscan",
        "provider": "etherscan",
    },
    42161: {
        "slug": "arbitrum",
        "block_time_sec": 0.25,
        "alchemy_subdomain": "arb-mainnet",
        "group": "etherscan",
        "provider": "etherscan",
    },
    137: {
        "slug": "polygon",
        "block_time_sec": 2.0,
        "alchemy_subdomain": "polygon-mainnet",
        "group": "etherscan",
        "provider": "etherscan",
    },
    42220: {
        "slug": "celo",
        "block_time_sec": 5.0,
        "alchemy_subdomain": "celo-mainnet",
        "group": "etherscan",
        "provider": "etherscan",
    },
    8453: {
        "slug": "base",
        "block_time_sec": 2.0,
        "alchemy_subdomain": "base-mainnet",
        "group": "alchemy_k1",
        "provider": "alchemy",
    },
    100: {
        "slug": "gnosis",
        "block_time_sec": 5.0,
        "alchemy_subdomain": "gnosis-mainnet",
        "group": "alchemy_k1",
        "provider": "alchemy",
    },
    56: {
        "slug": "bsc",
        "block_time_sec": 3.0,
        "alchemy_subdomain": "bnb-mainnet",
        "group": "bsc",
        "provider": "alchemy_or_ankr",
    },
    196: {
        "slug": "xlayer",
        "block_time_sec": 3.0,
        "alchemy_subdomain": None,
        "group": "xlayer",
        "provider": "okx",
    },
}

GROUP_EVM_IDS: dict[str, tuple[int, ...]] = {
    "etherscan": (1, 42161, 137, 42220),
    "alchemy_k1": (8453, 100),
    "bsc": (56,),
    "xlayer": (196,),
}

LOOKBACK_DAYS = 15

# First calendar month in prod: Alchemy key_2 barely used on the day-1 cut.
# Ankr Freemium 200M burned 2026-08-24 (pageSize 100). Drain remaining BSC with
# Alchemy until 2026-09-01 00:00 UTC, then resume day>=15 → Ankr.
ALCHEMY_BSC_THROUGH = date(2026, 9, 1)


def bsc_provider(utc_day: int, utc_date: date | None = None) -> str:
    """Day-1 cut (and drain while day < 15): Alchemy key_2. Day-15 cut onward: Ankr.

    Through 2026-08-31 UTC, always Alchemy so the first 15d cut can finish.
    """
    today = utc_date or datetime.now(timezone.utc).date()
    if today < ALCHEMY_BSC_THROUGH:
        return "alchemy"
    if utc_day < 15:
        return "alchemy"
    return "ankr"
