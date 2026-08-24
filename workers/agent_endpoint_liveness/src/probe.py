"""HEAD then GET reachability probe. No body validation."""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx

from ssrf import ssrf_block_reason

USER_AGENT = "GSA-endpoint-liveness/1.0"
PROBE_TIMEOUT_SECONDS = 8.0
MAX_REDIRECTS = 5
METHOD_FALLBACK_STATUSES = {405, 501}


@dataclass(frozen=True)
class ProbeResult:
    is_reachable: bool
    latency_ms: int | None
    http_status: int | None
    error_message: str | None
    check_method: str


def _timeout() -> httpx.Timeout:
    return httpx.Timeout(PROBE_TIMEOUT_SECONDS)


async def _head(client: httpx.AsyncClient, url: str) -> ProbeResult:
    started = time.monotonic()
    try:
        response = await client.head(
            url,
            follow_redirects=True,
            timeout=_timeout(),
        )
        latency_ms = int((time.monotonic() - started) * 1000)
        return ProbeResult(
            is_reachable=True,
            latency_ms=latency_ms,
            http_status=response.status_code,
            error_message=None,
            check_method="HEAD",
        )
    except httpx.TimeoutException as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return ProbeResult(
            is_reachable=False,
            latency_ms=latency_ms,
            http_status=None,
            error_message=f"timeout:{exc.__class__.__name__}",
            check_method="HEAD",
        )
    except httpx.HTTPError as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return ProbeResult(
            is_reachable=False,
            latency_ms=latency_ms,
            http_status=None,
            error_message=f"{exc.__class__.__name__}:{exc}"[:500],
            check_method="HEAD",
        )


async def _get(client: httpx.AsyncClient, url: str) -> ProbeResult:
    started = time.monotonic()
    try:
        async with client.stream(
            "GET",
            url,
            follow_redirects=True,
            timeout=_timeout(),
        ) as response:
            async for _chunk in response.aiter_bytes():
                break
            latency_ms = int((time.monotonic() - started) * 1000)
            return ProbeResult(
                is_reachable=True,
                latency_ms=latency_ms,
                http_status=response.status_code,
                error_message=None,
                check_method="GET",
            )
    except httpx.TimeoutException as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return ProbeResult(
            is_reachable=False,
            latency_ms=latency_ms,
            http_status=None,
            error_message=f"timeout:{exc.__class__.__name__}",
            check_method="GET",
        )
    except httpx.HTTPError as exc:
        latency_ms = int((time.monotonic() - started) * 1000)
        return ProbeResult(
            is_reachable=False,
            latency_ms=latency_ms,
            http_status=None,
            error_message=f"{exc.__class__.__name__}:{exc}"[:500],
            check_method="GET",
        )


async def probe_url(client: httpx.AsyncClient, url: str) -> ProbeResult:
    blocked = ssrf_block_reason(url)
    if blocked:
        return ProbeResult(
            is_reachable=False,
            latency_ms=None,
            http_status=None,
            error_message=blocked,
            check_method="SKIP",
        )

    head = await _head(client, url)
    if head.is_reachable and head.http_status not in METHOD_FALLBACK_STATUSES:
        return head
    if not head.is_reachable and not (head.error_message or "").startswith("timeout:"):
        return head
    return await _get(client, url)
