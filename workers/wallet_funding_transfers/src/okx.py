"""OKX X Layer Data API (not Market API)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import httpx

from canonical import parse_int, row_dict, synth_unique_id
from quota import raise_if_quota

logger = logging.getLogger("wallet_funding_transfers")

BASE = "https://web3.okx.com"
NATIVE_PATH = "/api/v5/xlayer/address/transaction-list"
TOKEN_PATH = "/api/v5/xlayer/address/token-transaction-list"
CHAIN = "xlayer"
PAGE_LIMIT = 50
TOKEN_PROTOCOLS = (("token_20", "erc20"),)
MAX_PAGES = 20


class OkxDataClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        api_key: str,
        secret: str,
        passphrase: str,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._secret = secret
        self._passphrase = passphrase

    def _headers(self, timestamp: str, sign: str) -> dict[str, str]:
        return {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }

    def _sign(self, timestamp: str, method: str, request_path: str) -> str:
        prehash = f"{timestamp}{method}{request_path}"
        digest = hmac.new(
            self._secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    async def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        qs = urlencode(params)
        request_path = f"{path}?{qs}"
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        sign = self._sign(timestamp, "GET", request_path)
        resp = await self._client.get(
            BASE + request_path,
            headers=self._headers(timestamp, sign),
        )
        body = resp.text
        raise_if_quota("okx", resp.status_code, body)
        if resp.status_code in {429, 500}:
            raise RuntimeError(f"okx rate/limit: {resp.status_code} {body[:200]}")
        if resp.status_code >= 400:
            raise_if_quota("okx", body)
            raise RuntimeError(f"okx http {resp.status_code}: {body[:300]}")
        payload = resp.json()
        code = str(payload.get("code", ""))
        if code not in {"0", "00", ""}:
            raise_if_quota("okx", payload.get("msg"), payload)
            raise RuntimeError(f"okx data api: {payload.get('msg') or payload}")
        return _extract_list(payload.get("data"))

    async def fetch_transfers(
        self,
        *,
        wallet_id: int,
        chain_pk: int,
        address: str,
        from_block: int,
        to_block: int,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows.extend(
            await self._paged(
                NATIVE_PATH,
                {"chainShortName": CHAIN, "address": address},
                category="external",
                wallet_id=wallet_id,
                chain_pk=chain_pk,
                address=address,
                from_block=from_block,
                to_block=to_block,
                window_start=window_start,
                window_end=window_end,
            )
        )
        for protocol, category in TOKEN_PROTOCOLS:
            rows.extend(
                await self._paged(
                    TOKEN_PATH,
                    {
                        "chainShortName": CHAIN,
                        "address": address,
                        "protocolType": protocol,
                    },
                    category=category,
                    wallet_id=wallet_id,
                    chain_pk=chain_pk,
                    address=address,
                    from_block=from_block,
                    to_block=to_block,
                    window_start=window_start,
                    window_end=window_end,
                )
            )
        return rows

    async def _paged(
        self,
        path: str,
        base_params: dict[str, Any],
        *,
        category: str,
        wallet_id: int,
        chain_pk: int,
        address: str,
        from_block: int,
        to_block: int,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page = 1
        while True:
            raw = await self._get(
                path,
                {**base_params, "limit": PAGE_LIMIT, "page": page},
            )
            if not raw:
                break
            stop = False
            kept = 0
            for item in raw:
                height = parse_int(item.get("height") or item.get("blockHeight") or item.get("blockNumber"))
                if height is not None and height < from_block:
                    stop = True
                    continue
                if height is not None and height > to_block:
                    continue
                mapped = _map_okx(
                    item,
                    category=category,
                    wallet_id=wallet_id,
                    chain_pk=chain_pk,
                    address=address,
                    window_start=window_start,
                    window_end=window_end,
                )
                if mapped is not None:
                    out.append(mapped)
                    kept += 1
            if stop or len(raw) < PAGE_LIMIT or page >= MAX_PAGES:
                break
            page += 1
        return out


def _extract_list(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, dict):
        for key in ("transactionList", "list", "tokenTransactionList", "blockList"):
            inner = data.get(key)
            if isinstance(inner, list):
                return [x for x in inner if isinstance(x, dict)]
        return []
    if isinstance(data, list):
        if not data:
            return []
        first = data[0]
        if isinstance(first, dict):
            for key in ("transactionList", "list", "tokenTransactionList", "blockList"):
                inner = first.get(key)
                if isinstance(inner, list):
                    return [x for x in inner if isinstance(x, dict)]
            if "txId" in first or "txid" in first or "txHash" in first or "hash" in first:
                return [x for x in data if isinstance(x, dict)]
        return []
    return []


def _map_okx(
    item: dict[str, Any],
    *,
    category: str,
    wallet_id: int,
    chain_pk: int,
    address: str,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any] | None:
    tx_hash = str(
        item.get("txId")
        or item.get("txid")
        or item.get("txHash")
        or item.get("hash")
        or ""
    ).strip()
    if not tx_hash:
        return None
    ts = parse_int(item.get("transactionTime") or item.get("timestamp"))
    if ts and ts > 10_000_000_000:
        ts = ts // 1000
    block_ts = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
    extra = str(item.get("logIndex") or item.get("nonce") or "0")
    token_id = item.get("tokenId")
    amount = item.get("amount") or item.get("value")
    value_raw: int | str | None = parse_int(amount)
    if value_raw is None and amount not in (None, ""):
        value_raw = str(amount).strip() or None
    return row_dict(
        wallet_id=wallet_id,
        chain_id=chain_pk,
        tx_hash=tx_hash,
        block_number=parse_int(item.get("height") or item.get("blockHeight") or item.get("blockNumber")),
        block_timestamp=block_ts,
        from_address=item.get("from") or item.get("fromAddress"),
        to_address=item.get("to") or item.get("toAddress"),
        category=category,
        asset=item.get("symbol")
        or item.get("transactionSymbol")
        or item.get("tokenSymbol")
        or ("OKB" if category == "external" else None),
        contract_address=item.get("tokenContractAddress") or item.get("contractAddress"),
        token_decimal=parse_int(item.get("decimals") or (18 if category == "external" else None)),
        token_id=str(token_id) if token_id not in (None, "") else None,
        value_raw=value_raw,
        unique_id=synth_unique_id(tx_hash, category, extra),
        provider="okx",
        window_start=window_start,
        window_end=window_end,
        wallet_address=address,
    )
