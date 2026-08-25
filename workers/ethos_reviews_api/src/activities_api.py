"""Ethos API v2 activities client (public / reviews only)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger("ethos_reviews_api")

DEFAULT_ETHOS_API_BASE = "https://api.ethos.network/api/v2"
ETHOS_CLIENT_HEADER = "gsa-ethos-reviews@1.0"
PAGE_LIMIT = 1000
REVIEW_FILTERS = ["review", "review-archived"]


class ActivitiesClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str = DEFAULT_ETHOS_API_BASE,
        throttle_ms: int = 200,
    ) -> None:
        self._http = http
        self._base = base_url.rstrip("/")
        self._throttle_ms = max(int(throttle_ms), 0)

    def _headers(self) -> dict[str, str]:
        return {
            "X-Ethos-Client": ETHOS_CLIENT_HEADER,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _post_page(
        self,
        direction: str,
        *,
        userkey: str,
        offset: int,
    ) -> dict[str, Any]:
        if direction not in ("received", "given"):
            raise ValueError(f"unknown direction={direction}")
        url = f"{self._base}/activities/profile/{direction}"
        body = {
            "userkey": userkey,
            "filter": REVIEW_FILTERS,
            "orderBy": {"field": "timestamp", "direction": "desc"},
            "limit": PAGE_LIMIT,
            "offset": offset,
        }
        resp = await self._http.post(url, headers=self._headers(), json=body)
        if self._throttle_ms > 0:
            await asyncio.sleep(self._throttle_ms / 1000.0)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"unexpected Ethos activities type: {type(data)}")
        return data

    async def fetch_direction(
        self,
        direction: str,
        *,
        profile_id: int,
        since_unix: float | None,
    ) -> list[dict[str, Any]]:
        """Paginate one direction. If since_unix is set, stop at older-or-equal activities."""
        userkey = f"profileId:{profile_id}"
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            payload = await self._post_page(direction, userkey=userkey, offset=offset)
            values = payload.get("values") or []
            if not isinstance(values, list):
                raise RuntimeError(f"unexpected values type for {direction}: {type(values)}")
            if not values:
                break

            stop = False
            page_kept = 0
            for item in values:
                if not isinstance(item, dict):
                    continue
                ts = activity_unix(item)
                if since_unix is not None and ts is not None and ts <= since_unix:
                    stop = True
                    continue
                out.append(item)
                page_kept += 1

            logger.info(
                "Ethos %s profile_id=%s offset=%s page=%s kept=%s total=%s",
                direction,
                profile_id,
                offset,
                len(values),
                page_kept,
                len(out),
            )
            if stop or len(values) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT
        return out

    async def fetch_reviews(
        self,
        profile_id: int,
        *,
        since_unix: float | None,
    ) -> list[dict[str, Any]]:
        received = await self.fetch_direction(
            "received", profile_id=profile_id, since_unix=since_unix
        )
        given = await self.fetch_direction(
            "given", profile_id=profile_id, since_unix=since_unix
        )
        return received + given


def activity_unix(item: dict[str, Any]) -> float | None:
    """Best-effort unix seconds from an activity object."""
    raw = item.get("timestamp")
    if raw is None and isinstance(item.get("data"), dict):
        raw = item["data"].get("createdAt") or item["data"].get("timestamp")
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        if val > 1e12:
            return val / 1000.0
        return val
    text = str(raw).strip()
    if not text:
        return None
    try:
        val = float(text)
        if val > 1e12:
            return val / 1000.0
        if val > 1e9:
            return val
    except ValueError:
        pass
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.timestamp()
    except ValueError:
        return None
