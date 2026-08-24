"""Ankr Advanced API — BSC second cut of the month."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from canonical import parse_int, row_dict, synth_unique_id

logger = logging.getLogger("wallet_activity_flows")

MIN_INTERVAL_S = 2.05  # ~30 req/min
# Ankr max is 10_000; 1000 covers ~15d for almost all wallets (avg ~129 rows)
# without the extra pages that burned Freemium credits at pageSize=100.
PAGE_SIZE = 1000


class AnkrClient:
    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        self._client = client
        self._api_key = api_key
        self._url = f"https://rpc.ankr.com/multichain/{api_key}"
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def _throttle(self) -> None:
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            wait = MIN_INTERVAL_S - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = loop.time()

    async def _rpc(self, method: str, params: dict[str, Any]) -> Any:
        await self._throttle()
        resp = await self._client.post(
            self._url,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("error"):
            raise RuntimeError(f"ankr {method}: {payload['error']}")
        return payload.get("result") or {}

    async def fetch_transfers(
        self,
        *,
        wallet_id: int,
        chain_pk: int,
        address: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        from_ts = int(window_start.timestamp())
        to_ts = int(window_end.timestamp())
        rows: list[dict[str, Any]] = []
        rows.extend(
            await self._paged(
                "ankr_getTransactionsByAddress",
                {
                    "blockchain": "bsc",
                    "address": address,
                    "fromTimestamp": from_ts,
                    "toTimestamp": to_ts,
                    "descOrder": True,
                    "pageSize": PAGE_SIZE,
                },
                mapper=lambda item: _map_native(
                    item,
                    wallet_id=wallet_id,
                    chain_pk=chain_pk,
                    address=address,
                    window_start=window_start,
                    window_end=window_end,
                ),
            )
        )
        rows.extend(
            await self._paged(
                "ankr_getTokenTransfers",
                {
                    "blockchain": "bsc",
                    "address": [address],
                    "fromTimestamp": from_ts,
                    "toTimestamp": to_ts,
                    "descOrder": True,
                    "pageSize": PAGE_SIZE,
                },
                mapper=lambda item: _map_token(
                    item,
                    category="erc20",
                    wallet_id=wallet_id,
                    chain_pk=chain_pk,
                    address=address,
                    window_start=window_start,
                    window_end=window_end,
                ),
            )
        )
        rows.extend(
            await self._paged(
                "ankr_getNftTransfers",
                {
                    "blockchain": "bsc",
                    "address": [address],
                    "fromTimestamp": from_ts,
                    "toTimestamp": to_ts,
                    "descOrder": True,
                    "pageSize": PAGE_SIZE,
                },
                mapper=lambda item: _map_nft(
                    item,
                    wallet_id=wallet_id,
                    chain_pk=chain_pk,
                    address=address,
                    window_start=window_start,
                    window_end=window_end,
                ),
            )
        )
        return rows

    async def _paged(
        self,
        method: str,
        params: dict[str, Any],
        mapper,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            call = dict(params)
            if page_token:
                call["pageToken"] = page_token
            result = await self._rpc(method, call)
            transfers = (
                result.get("transactions")
                or result.get("transfers")
                or result.get("nfts")
                or []
            )
            for item in transfers:
                mapped = mapper(item)
                if mapped is not None:
                    out.append(mapped)
            page_token = result.get("nextPageToken") or result.get("pageToken")
            if not page_token:
                break
        return out


def _ts(item: dict[str, Any]) -> datetime | None:
    raw = item.get("timestamp") or item.get("blockTimestamp")
    n = parse_int(raw)
    if n is None:
        return None
    if n > 10_000_000_000:
        n = n // 1000
    return datetime.fromtimestamp(n, tz=timezone.utc)


def _map_native(
    item: dict[str, Any],
    *,
    wallet_id: int,
    chain_pk: int,
    address: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any] | None:
    tx_hash = str(item.get("hash") or item.get("transactionHash") or "").strip()
    if not tx_hash:
        return None
    return row_dict(
        wallet_id=wallet_id,
        chain_id=chain_pk,
        tx_hash=tx_hash,
        block_number=parse_int(item.get("blockNumber") or item.get("blockHeight")),
        block_timestamp=_ts(item),
        from_address=item.get("from") or item.get("fromAddress"),
        to_address=item.get("to") or item.get("toAddress"),
        category="external",
        asset="BNB",
        contract_address=None,
        token_decimal=18,
        token_id=None,
        value_raw=parse_int(item.get("value")),
        unique_id=synth_unique_id(tx_hash, "external", "0"),
        provider="ankr",
        window_start=window_start,
        window_end=window_end,
        wallet_address=address,
    )


def _map_token(
    item: dict[str, Any],
    *,
    category: str,
    wallet_id: int,
    chain_pk: int,
    address: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any] | None:
    tx_hash = str(item.get("transactionHash") or item.get("hash") or "").strip()
    if not tx_hash:
        return None
    extra = str(item.get("logIndex") or item.get("tokenId") or "0")
    return row_dict(
        wallet_id=wallet_id,
        chain_id=chain_pk,
        tx_hash=tx_hash,
        block_number=parse_int(item.get("blockNumber") or item.get("blockHeight")),
        block_timestamp=_ts(item),
        from_address=item.get("fromAddress") or item.get("from"),
        to_address=item.get("toAddress") or item.get("to"),
        category=category,
        asset=item.get("tokenSymbol") or item.get("symbol"),
        contract_address=item.get("contractAddress") or item.get("tokenContract"),
        token_decimal=parse_int(item.get("tokenDecimals") or item.get("decimals")),
        token_id=None,
        value_raw=parse_int(item.get("value") or item.get("valueRaw")),
        unique_id=synth_unique_id(tx_hash, category, extra),
        provider="ankr",
        window_start=window_start,
        window_end=window_end,
        wallet_address=address,
    )


def _map_nft(
    item: dict[str, Any],
    *,
    wallet_id: int,
    chain_pk: int,
    address: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any] | None:
    kind = str(item.get("type") or item.get("contractType") or "ERC721").upper()
    category = "erc1155" if "1155" in kind else "erc721"
    tx_hash = str(item.get("transactionHash") or item.get("hash") or "").strip()
    if not tx_hash:
        return None
    token_id = item.get("tokenId") or item.get("id")
    extra = str(item.get("logIndex") or token_id or "0")
    return row_dict(
        wallet_id=wallet_id,
        chain_id=chain_pk,
        tx_hash=tx_hash,
        block_number=parse_int(item.get("blockNumber") or item.get("blockHeight")),
        block_timestamp=_ts(item),
        from_address=item.get("fromAddress") or item.get("from"),
        to_address=item.get("toAddress") or item.get("to"),
        category=category,
        asset=item.get("collectionName") or item.get("name"),
        contract_address=item.get("contractAddress"),
        token_decimal=0,
        token_id=str(token_id) if token_id not in (None, "") else None,
        value_raw=parse_int(item.get("amount") or item.get("value") or 1),
        unique_id=synth_unique_id(tx_hash, category, extra),
        provider="ankr",
        window_start=window_start,
        window_end=window_end,
        wallet_address=address,
    )
