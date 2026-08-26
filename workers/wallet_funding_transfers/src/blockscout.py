"""Blockscout PRO API — Base / Gnosis (Etherscan-compatible account module)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import httpx

from etherscan import _map
from quota import raise_if_quota

logger = logging.getLogger("wallet_funding_transfers")

BASE = "https://api.blockscout.com/v2/api"
ACTIONS = (("txlist", "external"), ("tokentx", "erc20"))
PAGE_SIZE = 500
MIN_INTERVAL_S = 0.25


class BlockscoutClient:
    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        self._client = client
        self._api_key = api_key
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

    async def _get(self, params: dict[str, Any]) -> Any:
        await self._throttle()
        params = {**params, "apikey": self._api_key}
        url = f"{BASE}?{urlencode(params)}"
        resp = await self._client.get(url)
        body = resp.text
        raise_if_quota("blockscout", resp.status_code, body)
        if resp.status_code == 402:
            raise_if_quota("blockscout", body)
        resp.raise_for_status()
        data = resp.json()
        status = str(data.get("status", ""))
        message = str(data.get("message", ""))
        result = data.get("result")
        raise_if_quota("blockscout", message, result)
        if status == "0" and "no transaction" in message.lower():
            return []
        if status == "0" and isinstance(result, str) and "not found" in str(result).lower():
            return []
        if status == "0":
            raise RuntimeError(f"blockscout error: {message}: {result}")
        if not isinstance(result, list):
            return []
        return result

    async def fetch_transfers(
        self,
        *,
        wallet_id: int,
        chain_pk: int,
        evm_chain_id: int,
        address: str,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for action, category in ACTIONS:
            raw = await self._get(
                {
                    "chain_id": evm_chain_id,
                    "module": "account",
                    "action": action,
                    "address": address,
                    "startblock": 0,
                    "endblock": 99999999,
                    "page": 1,
                    "offset": PAGE_SIZE,
                    "sort": "asc",
                }
            )
            for item in raw:
                mapped = _map(
                    item,
                    category=category,
                    wallet_id=wallet_id,
                    chain_pk=chain_pk,
                    evm_chain_id=evm_chain_id,
                    address=address,
                    window_start=window_start,
                    window_end=window_end,
                    provider="blockscout",
                )
                if mapped is not None:
                    rows.append(mapped)
        return rows
