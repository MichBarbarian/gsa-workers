"""Canonical transfer row for wallets.wallet_funding_transfers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.startswith("0x") or s.startswith("0X"):
            return int(s, 16)
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except ValueError:
            return None


def lower_addr(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s or s == "0x":
        return None
    return s


def direction_for(wallet: str, from_address: str | None, to_address: str | None) -> str:
    w = wallet.lower()
    f = (from_address or "").lower()
    t = (to_address or "").lower()
    if f == w and t == w:
        return "self"
    if t == w:
        return "incoming"
    return "outgoing"


def synth_unique_id(tx_hash: str, category: str, extra: str) -> str:
    return f"{tx_hash.lower()}:{category}:{extra}"


def iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def row_dict(
    *,
    wallet_id: int,
    chain_id: int,
    tx_hash: str,
    block_number: int | None,
    block_timestamp: datetime | None,
    from_address: str | None,
    to_address: str | None,
    category: str,
    asset: str | None,
    contract_address: str | None,
    token_decimal: int | None,
    token_id: str | None,
    value_raw: int | str | None,
    unique_id: str,
    provider: str,
    window_start: datetime,
    window_end: datetime,
    wallet_address: str,
) -> dict[str, Any]:
    tx_hash = tx_hash.lower()
    frm = lower_addr(from_address)
    to = lower_addr(to_address)
    return {
        "wallet_id": wallet_id,
        "chain_id": chain_id,
        "tx_hash": tx_hash,
        "block_number": block_number,
        "block_timestamp": iso(block_timestamp) if block_timestamp else None,
        "from_address": frm,
        "to_address": to,
        "direction": direction_for(wallet_address, frm, to),
        "category": category,
        "asset": asset,
        "contract_address": lower_addr(contract_address),
        "token_decimal": token_decimal,
        "token_id": token_id,
        "value_raw": str(value_raw) if value_raw is not None else None,
        "unique_id": unique_id.lower() if unique_id else unique_id,
        "provider": provider,
        "window_start": iso(window_start),
        "window_end": iso(window_end),
    }


# aliases for activity-style names
row_dict = row_dict
synth_unique_id = synth_unique_id


def keep_oldest_incoming(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    incoming = [r for r in rows if r.get("direction") == "incoming"]
    incoming.sort(key=lambda r: (r.get("block_timestamp") or "", r.get("block_number") or 0, r.get("unique_id") or ""))
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in incoming:
        uid = str(row.get("unique_id") or "")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        out.append(row)
        if len(out) >= limit:
            break
    return out
