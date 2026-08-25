# Ethos Reviews API

> Project context: [AGENTS.md](../../AGENTS.md) · [Process catalog](../../docs/PROCESSES.md) (#16) · [Supabase map](../../docs/SUPABASE.md) · [Architecture](../../docs/ARCHITECTURE.md)

**Status: live after schema deploy** (cron `0 0,6,12,18 * * *` UTC + `workflow_dispatch`).

Fetches Ethos v2 **reviews** (given + received) for GSA-linked Claimed profiles and upserts `ethos.reviews`. Does **not** replace `ethos_scores` (credibility) and does **not** revive `ethos-enrich`.

**ADR:** vault `08 - Decisiones/2026-08-25 - Worker Ethos API reviews on-demand`  
**Schema:** `gsa-supabase-schema` → `supabase/docs/ethos-reviews-api.md` (`20260825020000_ethos_reviews_api_worker.sql`)  
**Vault ops:** `12 - Github Worker/Ethos Reviews API/`

Goldsky no longer indexes reviews. `on_demand_backfill` / `ethos_history` does not fetch them.

## Pipeline

```
claim_reviews_fetch (SKIP LOCKED)
  → POST /api/v2/activities/profile/received + /given
     filter review + review-archived, limit 1000
  → map → upsert ethos.reviews (graph_id = ethos-api:review:{id})
  → complete_reviews_fetch
```

- `reviews_fetched_at IS NULL` → full paginate.
- Else → newest-first, stop when `timestamp <= reviews_fetched_at`.
- Success → watermark now, `next_eligible_at = now() + 1 day`.
- Error → `reviews_last_status=error`, watermark unchanged; reclaim after 2h.

Empty queue → log `queue empty` → **exit 0**. Cap `MAX_RUNTIME_SECONDS=19800`.

## Eligibility

`ethos.profiles` with a Claimed `profile_addresses.wallet_id` and (`reviews_next_eligible_at IS NULL` or `<= now()`). Late-link trigger sets `reviews_next_eligible_at = now()`.

## Env

| Variable | Default | Role |
|---|---|---|
| `SUPABASE_DB_URL` | required | Pooler DSN |
| `WORKER_ID` | `reviews-a` | Claim stamp |
| `CONCURRENCY` | 3 | In-flight profiles |
| `CLAIM_BATCH_SIZE` | 10 | Claim size |
| `CLAIM_STALE_SECONDS` | 7200 | Reclaim |
| `THROTTLE_MS` | 200 | Pause after each Ethos POST |
| `ETHOS_API_BASE` | `https://api.ethos.network/api/v2` | API root |
| `UPSERT_CHUNK_SIZE` | 200 | INSERT chunks |
| `MAX_RUNTIME_SECONDS` | 19800 | Slot cap |

Header `X-Ethos-Client: gsa-ethos-reviews@1.0`. No extra secret.

## Local

```powershell
cd workers/ethos_reviews_api
copy .env.example .env
uv sync
uv run python job.py
```

## Monitor

```sql
SELECT
  count(*) FILTER (
    WHERE reviews_next_eligible_at IS NULL OR reviews_next_eligible_at <= now()
  ) AS due_clock,
  count(*) FILTER (WHERE reviews_fetched_at IS NOT NULL) AS fetched,
  count(*) FILTER (WHERE reviews_last_status = 'error') AS errors
FROM ethos.profiles p
WHERE EXISTS (
  SELECT 1 FROM ethos.profile_addresses pa
  WHERE pa.profile_id = p.profile_id
    AND pa.wallet_id IS NOT NULL
    AND lower(pa.status) = 'claimed'
);

SELECT count(*) FROM ethos.reviews;
```

Follow-up (not this worker): reactivate `aggregate_owner_signals` / `ethos_signals`.
