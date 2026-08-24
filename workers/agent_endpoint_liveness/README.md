# Agent endpoint liveness (15d)

> Project context: [AGENTS.md](../../AGENTS.md) · [Process catalog](../../docs/PROCESSES.md) (#15) · [Supabase map](../../docs/SUPABASE.md) · [Architecture](../../docs/ARCHITECTURE.md)

**Status: live after schema deploy** (cron `0 0,6,12,18 * * *` UTC + `workflow_dispatch`).

HTTP(s) reachability census of `erc_8004.agent_metadata_services`. Last result + 15d clock live in `erc_8004.agent_endpoint_health`. Per-agent rollup is the **view** `erc_8004.agent_endpoint_status` (`live` / `degraded` / `down` / `unknown`).

Does **not** write HUMI/WAMI, does **not** consume ERC-8004 Reputation `LIVENESS` feedback, and does **not** validate MCP/A2A bodies. HEAD/GET means a server answered at that URL.

**ADR:** vault `08 - Decisiones/2026-08-24 - Censo liveness endpoints agent_metadata_services 15d`  
**Schema:** `gsa-supabase-schema` → `supabase/docs/agent-endpoint-health.md`  
**Vault ops:** `12 - Github Worker/Agent Endpoint Liveness 15d/`

HEAD then GET, 8s timeout, no body. Locator typos (`htttps`, `https:\…`) are repaired in SQL `normalize_http_endpoint`; the services table is left raw.

## Pipeline

```
sync (HTTP locators from services)
  → claim due rows (SKIP LOCKED)
  → coalesce by endpoint_normalized
  → HEAD → GET on 405/501/timeout
  → complete_batch (+15d)
```

Empty queue after sync → log `queue empty` → **exit 0**. Run cap `MAX_RUNTIME_SECONDS=19800` (~5.5h).

## Schedule

| Trigger | When |
|---------|------|
| Cron | `0 0,6,12,18 * * *` UTC |
| Manual | `workflow_dispatch` |

Workflow: `.github/workflows/agent-endpoint-liveness.yml`. Concurrency group `agent-endpoint-liveness` (`cancel-in-progress: false`).

## Eligibility

Active rows in `agent_endpoint_health` with `next_eligible_at <= now()` and stale-or-null claim. Sync upserts current HTTP(s) services, deactivates removed URLs, **does not** reset the 15d clock.

## Env

| Variable | Default | Role |
|---|---|---|
| `SUPABASE_DB_URL` | required | Pooler DSN |
| `WORKER_ID` | `liveness-a` | Claim stamp |
| `CONCURRENCY` | 20 | Global in-flight probes |
| `PER_HOST_CONCURRENCY` | 2 | Per hostname |
| `CLAIM_BATCH_SIZE` | 40 | Claim size |
| `CLAIM_STALE_SECONDS` | 7200 | Reclaim |
| `MAX_RUNTIME_SECONDS` | 19800 | Slot cap |

Schema first: `gsa-supabase-schema` `20260824010000_agent_endpoint_health.sql`.

## Local

```powershell
cd workers/agent_endpoint_liveness
copy .env.example .env
uv sync
uv run python job.py
```

## Monitor

```sql
SELECT
  count(*) FILTER (WHERE is_active) AS active,
  count(*) FILTER (
    WHERE is_active AND next_eligible_at <= now()
      AND (claimed_at IS NULL OR claimed_at < now() - interval '2 hours')
  ) AS due,
  count(*) FILTER (WHERE is_active AND is_reachable IS TRUE) AS reachable
FROM erc_8004.agent_endpoint_health;

SELECT status, count(*) FROM erc_8004.agent_endpoint_status GROUP BY 1;
```
