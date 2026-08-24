# Agent URI reprocess

> [AGENTS.md](../../AGENTS.md) · sibling [`agent_uri_resolve`](../agent_uri_resolve/README.md)

GHA worker for:

1. **Download errors** on `agent_manifest` — retry with `reprocess_count` max **3** (first attempt immediate; later attempts need `updated_at` older than 3 days). Honors `does_need_manual_reprocess`.
2. **Off-chain refresh** on `uri_documents` — HTTP/IPFS rows with `fetched_at` older than **15 days**. Hex / data-URI / `internal_on_chain_id_*` are **not** refreshed (import requeues those via `is_uri_processed` / `is_feedback_processed` into `agent_uri_resolve`).

After a successful refresh, linked manifests get `is_processed = false` **only if** `document` changed.

Failed refresh **keeps** the previous `document` / `status='valid'`, but still stamps `fetched_at` / `expires_at` (15d) and `source_gateway=refresh_error:…` so the attempt is dated and the row leaves the current claim window.

Reuses resolve/handlers from `workers/agent_uri_resolve/src` via `sys.path`.

Requires schema migration `00000000000069_uri_reprocess_refresh_indexes.sql`.

## Schedule

`06:00` and `18:00` UTC + `workflow_dispatch` (resolve uses `00:00` / `12:00`).

## Env

| Variable | Required | Default |
|---|---|---|
| `SUPABASE_DB_URL` | Yes | — |
| `PINATA_GATEWAY` | No | — (Pinata dedicated Gateway Key; last IPFS fallback) |
| `SCRAPING_ANT_KEY` | No | — |
| `CLAIM_BATCH_SIZE` | No | `20` |
| `CONCURRENCY` | No | `4` |
| `MAX_RUNTIME_SECONDS` | No | `19800` |
| `WORKER_ID` | No | `reprocess-a` |

## Local

```powershell
cd workers/agent_uri_reprocess
uv sync
uv run playwright install chromium
uv run python job.py
```
