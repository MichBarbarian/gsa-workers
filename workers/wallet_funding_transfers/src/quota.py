"""Detect provider quota exhaustion (daily/monthly) vs transient rate limits."""

from __future__ import annotations


class QuotaExhausted(RuntimeError):
    """Daily or monthly credits are gone. Stop this GHA group until the next cron."""

    def __init__(self, provider: str, detail: str = "") -> None:
        self.provider = provider
        super().__init__(f"{provider} quota exhausted{': ' + detail if detail else ''}")


_QUOTA_MARKERS = (
    "max daily",
    "daily api calls",
    "max calls per day",
    "quota exceeded",
    "quota limit",
    "out of credits",
    "credits exhausted",
    "not enough credits",
    "insufficient credits",
    "monthly limit",
    "plan limit",
    "limit of your plan",
    "upgrade your plan",
    "credit limit",
    "usage cap",
)


def looks_like_quota(text: str) -> bool:
    blob = (text or "").lower()
    return any(marker in blob for marker in _QUOTA_MARKERS)


def raise_if_quota(provider: str, *parts: object) -> None:
    blob = " ".join(str(p) for p in parts if p is not None)
    if looks_like_quota(blob):
        raise QuotaExhausted(provider, blob[:400])
