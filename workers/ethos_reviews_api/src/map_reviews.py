"""Map Ethos v2 activity objects → ethos.reviews upsert dicts."""

from __future__ import annotations

import json
from typing import Any

from activities_api import activity_unix

_SCORE_MAP = {
    "positive": "positive",
    "negative": "negative",
    "neutral": "neutral",
    "pos": "positive",
    "neg": "negative",
    "neu": "neutral",
}


def _s(val: Any) -> str | None:
    if val is None:
        return None
    text = str(val).strip()
    return text if text else None


def _lower(val: Any) -> str | None:
    text = _s(val)
    return text.lower() if text else None


def _i_opt(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _profile_id(obj: Any) -> int | None:
    if isinstance(obj, dict):
        return _i_opt(obj.get("profileId")) or _i_opt(obj.get("id"))
    return _i_opt(obj)


def _address_from_user(obj: Any) -> str | None:
    if isinstance(obj, str):
        return _lower(obj)
    if not isinstance(obj, dict):
        return None
    direct = _lower(obj.get("address")) or _lower(obj.get("primaryAddress"))
    if direct:
        return direct
    keys = obj.get("userkeys")
    if isinstance(keys, list):
        for key in keys:
            text = _s(key)
            if text and text.lower().startswith("address:"):
                return text.split(":", 1)[1].strip().lower() or None
    return None


def _data_blob(item: dict[str, Any]) -> dict[str, Any]:
    data = item.get("data")
    if isinstance(data, dict):
        inner = data.get("data")
        if isinstance(inner, dict) and (
            "score" in inner or "reviewId" in inner or "comment" in inner
        ):
            return inner
        return data
    return {}


def _normalize_score(raw: Any) -> str | None:
    text = _s(raw)
    if not text:
        return None
    return _SCORE_MAP.get(text.lower())


def _is_archived(item: dict[str, Any], data: dict[str, Any]) -> bool:
    typ = _s(item.get("type")) or _s(data.get("type")) or ""
    if "archiv" in typ.lower():
        return True
    for key in ("archived", "isArchived"):
        val = item.get(key)
        if val is None:
            val = data.get(key)
        if isinstance(val, bool):
            return val
        if _s(val) and str(val).lower() in ("true", "1", "yes"):
            return True
    return False


def _metadata_text(data: dict[str, Any]) -> str | None:
    meta = data.get("metadata")
    if meta is None:
        return None
    if isinstance(meta, str):
        return _s(meta)
    try:
        return json.dumps(meta, ensure_ascii=False, default=str)
    except TypeError:
        return _s(meta)


def map_activity(item: dict[str, Any]) -> dict[str, Any] | None:
    data = _data_blob(item)
    review_id = (
        _i_opt(data.get("reviewId"))
        or _i_opt(data.get("id"))
        or _i_opt(item.get("id"))
        or _i_opt(item.get("reviewId"))
    )
    if review_id is None or review_id <= 0:
        return None

    score = _normalize_score(
        data.get("score") or data.get("reviewScore") or item.get("score")
    )
    author = item.get("author") if isinstance(item.get("author"), dict) else data.get("author")
    subject = (
        item.get("subject") if isinstance(item.get("subject"), dict) else data.get("subject")
    )
    author_pid = (
        _profile_id(author)
        or _i_opt(data.get("authorProfileId"))
        or _profile_id(data.get("authorProfile"))
    )
    subject_pid = (
        _profile_id(subject)
        or _i_opt(data.get("subjectProfileId"))
        or _profile_id(data.get("subjectProfile"))
    )
    ts = activity_unix(item)
    if ts is None:
        ts = 0.0

    comment = _s(data.get("comment")) or _s(item.get("comment"))
    return {
        "graph_id": f"ethos-api:review:{review_id}",
        "review_id": review_id,
        "score": score,
        "author_address": _address_from_user(author) or _lower(data.get("author")),
        "subject_address": _address_from_user(subject) or _lower(data.get("subject")),
        "attestation_hash": _s(data.get("attestationHash")) or _s(data.get("attestation_hash")),
        "comment": comment,
        "metadata": _metadata_text(data),
        "created_at": int(ts),
        "archived": _is_archived(item, data),
        "author_profile_id": author_pid,
        "subject_profile_id": subject_pid,
    }


def map_activities(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        row = map_activity(item)
        if row is None:
            continue
        gid = row["graph_id"]
        if gid in seen:
            continue
        seen.add(gid)
        rows.append(row)
    return rows
