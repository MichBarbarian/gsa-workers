#!/usr/bin/env python3
"""Retry agent_manifest download errors and refresh off-chain uri_documents."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent
# Prefer local src (db) over sibling; sibling provides resolve/documents/handlers.
sys.path.insert(0, str(ROOT.parent / "agent_uri_resolve" / "src"))
sys.path.insert(0, str(ROOT / "src"))

from db import (  # noqa: E402
    CLAIM_RETRY_BASE_SECONDS,
    Database,
    feedback_uri_from_row,
    parse_feedback_id,
)
from nested import extract_did_uri, normalize_did_uri  # noqa: E402
from resolve import UriResolver  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("agent_uri_reprocess")


def load_dotenv_if_present() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def env_int(name: str, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    raw = os.environ.get(name)
    value = default if raw is None or raw.strip() == "" else int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return value


def env_str(name: str, default: str = "") -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def truncate_uri_for_error(uri: str | None, limit: int = 500) -> str:
    text = (uri or "").strip() or "unknown"
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def documents_equal(a: Any, b: Any) -> bool:
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(
        b, sort_keys=True, default=str
    )


async def resolve_uri_for_manifest(
    db: Database,
    resolver: UriResolver,
    row: dict[str, Any],
) -> str | None:
    provider = row.get("provider") or ""
    agent_id = int(row["agent_id"])

    if provider in ("erc-8004", "erc-8004-did"):
        agent_uri = db.lookup_agent_uri(agent_id)
        if not agent_uri:
            return None
        if provider == "erc-8004":
            return agent_uri
        parent = await resolver.resolve(agent_uri, force_refresh=False)
        if not parent.ok or not isinstance(parent.document, dict):
            return None
        did = extract_did_uri(parent.document)
        return normalize_did_uri(did) if did else None

    feedback_id = parse_feedback_id(provider)
    if feedback_id is None:
        return None
    feedback = db.lookup_feedback(feedback_id)
    if feedback is None:
        return None
    return feedback_uri_from_row(feedback)


async def process_error_manifest(
    db: Database,
    resolver: UriResolver,
    row: dict[str, Any],
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        manifest_id = int(row["id"])
        try:
            uri = await resolve_uri_for_manifest(db, resolver, row)
            if not uri:
                raise ValueError(f"Cannot resolve URI for provider={row.get('provider')}")
            if uri.startswith("internal_on_chain_id_"):
                raise ValueError("Skipping on-chain synthetic URI in error reprocess")

            result = await resolver.resolve(uri, force_refresh=True)
            ok = result.ok and result.document is not None and result.uri_document_id
            if ok:
                db.mark_reprocess_success(
                    manifest_id=manifest_id,
                    uri_document_id=int(result.uri_document_id),
                    url_type=result.used_gateway or "valid",
                )
                logger.info(
                    "Reprocess ok manifest_id=%s gateway=%s",
                    manifest_id,
                    result.used_gateway,
                )
            else:
                db.mark_reprocess_failure(
                    manifest_id=manifest_id,
                    error_message=(
                        f"{result.error or 'resolve_failed'} "
                        f"uri={truncate_uri_for_error(uri)}"
                    ),
                    url_type=result.used_gateway or "error",
                )
                logger.info(
                    "Reprocess fail manifest_id=%s err=%s",
                    manifest_id,
                    result.error,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Reprocess exception manifest_id=%s: %s", manifest_id, exc)
            try:
                db.mark_reprocess_failure(
                    manifest_id=manifest_id,
                    error_message=str(exc),
                    url_type="exception",
                )
            except Exception:
                logger.exception(
                    "Failed to persist reprocess failure manifest_id=%s", manifest_id
                )


async def process_refresh_doc(
    db: Database,
    resolver: UriResolver,
    row: dict[str, Any],
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        doc_id = int(row["id"])
        uri = row["uri"]
        old_document = row.get("document")
        try:
            result = await resolver.resolve(uri, force_refresh=True)
            if not result.ok or result.document is None:
                err = result.error or "resolve_failed"
                db.stamp_refresh_attempt_failed(doc_id, error_message=err)
                logger.warning(
                    "Refresh fetch failed doc_id=%s err=%s "
                    "(kept previous document; stamped fetched_at)",
                    doc_id,
                    err,
                )
                return

            changed = not documents_equal(old_document, result.document)
            if changed:
                new_id = db.upsert_document(
                    uri=uri,
                    document=result.document,
                    source_gateway=result.used_gateway or "refresh",
                    cid=None,
                )
                reset_n = db.reset_manifests_for_document(new_id)
                logger.info(
                    "Refresh changed doc_id=%s new_id=%s manifests_reset=%s gateway=%s",
                    doc_id,
                    new_id,
                    reset_n,
                    result.used_gateway,
                )
            else:
                db.renew_document_ttl(doc_id)
                logger.info("Refresh unchanged doc_id=%s (TTL renewed)", doc_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Refresh exception doc_id=%s: %s", doc_id, exc)
            try:
                db.stamp_refresh_attempt_failed(doc_id, error_message=str(exc))
            except Exception:
                logger.exception(
                    "Failed to stamp refresh attempt doc_id=%s", doc_id
                )


async def run_job() -> int:
    load_dotenv_if_present()
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        logger.error("SUPABASE_DB_URL is required")
        return 1

    pinata = env_str("PINATA_GATEWAY")
    scraping_ant = env_str("SCRAPING_ANT_KEY")
    claim_batch = env_int("CLAIM_BATCH_SIZE", 20, minimum=1, maximum=200)
    concurrency = env_int("CONCURRENCY", 4, minimum=1, maximum=20)
    max_runtime = env_int("MAX_RUNTIME_SECONDS", 19800, minimum=60)
    worker_id = env_str("WORKER_ID", "reprocess-a")

    db = Database(dsn)
    db.connect()
    started = time.monotonic()
    logger.info(
        "Started worker_id=%s claim_batch=%s concurrency=%s max_runtime=%ss",
        worker_id,
        claim_batch,
        concurrency,
        max_runtime,
    )

    total_errors = 0
    total_refresh = 0

    try:
        async with httpx.AsyncClient() as http:
            resolver = UriResolver(
                db, http, pinata_token=pinata, scraping_ant_key=scraping_ant
            )
            sem = asyncio.Semaphore(concurrency)

            while True:
                elapsed = time.monotonic() - started
                if elapsed >= max_runtime:
                    logger.info("Time budget reached (%.0fs)", elapsed)
                    break

                try:
                    errors = db.claim_error_manifests(claim_batch)
                except Exception as exc:
                    # Only soft-retry transient DB errors; programming/SQL bugs must fail fast.
                    if not isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
                        raise
                    logger.warning("Claim error manifests failed; retry: %s", exc)
                    await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                    continue

                if errors:
                    logger.info("Claimed error manifests batch size=%s", len(errors))
                    await asyncio.gather(
                        *[
                            process_error_manifest(db, resolver, row, sem)
                            for row in errors
                        ]
                    )
                    total_errors += len(errors)
                    continue

                try:
                    docs = db.claim_refresh_docs(claim_batch)
                except Exception as exc:
                    if not isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
                        raise
                    logger.warning("Claim refresh docs failed; retry: %s", exc)
                    await asyncio.sleep(CLAIM_RETRY_BASE_SECONDS)
                    continue

                if docs:
                    logger.info("Claimed refresh docs batch size=%s", len(docs))
                    await asyncio.gather(
                        *[process_refresh_doc(db, resolver, row, sem) for row in docs]
                    )
                    total_refresh += len(docs)
                    continue

                logger.info("No pending errors or refresh docs; exiting")
                break

    except Exception:
        logger.error("Critical job failure\n%s", traceback.format_exc())
        return 1
    finally:
        db.close()

    logger.info(
        "Done errors=%s refresh=%s elapsed=%.1fs",
        total_errors,
        total_refresh,
        time.monotonic() - started,
    )
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run_job()))


if __name__ == "__main__":
    main()
