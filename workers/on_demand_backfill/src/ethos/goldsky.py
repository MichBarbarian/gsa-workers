"""Goldsky GraphQL client for Ethos signal history by profileId."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger("on_demand_backfill")

DEFAULT_GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/project_cmma0eekxnc4e01vt9klkbya9"
    "/subgraphs/ethos-network-base/prod/gn"
)

PAGE_SIZE = 1000

# Goldsky ethos-network-base 1.1.0 is Profile + Address only.
# Reviews (and other former signal entities) are not in the subgraph.
# Reviews: dedicated worker ethos_reviews_api (Ethos API v2).
ENTITY_QUERIES: dict[str, tuple[str, str, str]] = {}


def _build_query(root: str, where: str, fields: str, skip: int) -> str:
    return f"""
query EthosHistory {{
  {root}(where: {{ {where} }}, first: {PAGE_SIZE}, skip: {skip}, orderBy: id, orderDirection: asc) {{
    {fields}
  }}
}}
""".strip()


async def fetch_entity_pages(
    client: httpx.AsyncClient,
    *,
    url: str,
    entity: str,
    profile_id: int,
) -> list[dict[str, Any]]:
    spec = ENTITY_QUERIES.get(entity)
    if spec is None:
        raise ValueError(f"unknown entity: {entity}")
    root, where_tmpl, fields = spec
    where = where_tmpl.format(pid=str(profile_id))
    out: list[dict[str, Any]] = []
    skip = 0
    while True:
        query = _build_query(root, where, fields, skip)
        resp = await client.post(url, json={"query": query})
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise RuntimeError(f"Goldsky GraphQL errors for {entity}: {payload['errors']}")
        batch = (payload.get("data") or {}).get(root) or []
        if not isinstance(batch, list):
            raise RuntimeError(f"unexpected Goldsky data for {entity}: {type(batch)}")
        out.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        logger.info(
            "Goldsky paginate entity=%s profile_id=%s skip=%s total=%s",
            entity,
            profile_id,
            skip,
            len(out),
        )
    return out


async def fetch_all_signals(
    client: httpx.AsyncClient,
    *,
    url: str,
    profile_id: int,
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    if not ENTITY_QUERIES:
        logger.info(
            "Goldsky signal entities disabled (subgraph slim); skipping GraphQL profile_id=%s",
            profile_id,
        )
        return result
    for entity in ENTITY_QUERIES:
        rows = await fetch_entity_pages(
            client, url=url, entity=entity, profile_id=profile_id
        )
        result[entity] = rows
        logger.info(
            "Goldsky entity=%s profile_id=%s rows=%s",
            entity,
            profile_id,
            len(rows),
        )
    return result
