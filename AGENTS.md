<!-- BEGIN:gsa-obsidian-knowledge -->
## Conocimiento org (bóveda Obsidian)

Fuente de verdad de producto/GSA: bóveda Obsidian (`GlobalScoreAgent` en Google Drive), vía **MCP Obsidian** en Cursor.

- Carpeta vault de este repo: **02 - Base de Datos** (operación / workers)
- Docs técnicos de este repo: **`docs/`**
- Schema / RPCs: repo hermano **`gsa-supabase-schema`**
- Decisiones cross-repo: vault **08 - Decisiones**
- Guía canónica: `10 - Agents/AGENTS.md` (stub raíz: `AGENTS.md`)

No inventes definiciones GSA/HUMI/WAMI/Walcert: consulta la bóveda. Español por defecto salvo que se pida otro idioma. Opiniones honestas.
<!-- END:gsa-obsidian-knowledge -->

# AGENTS.md — working on gsa-workers

Entry point for AI agents (and humans) changing GitHub Actions batch workers.

## What this repo is

**Python 3.12** batch jobs on **GitHub Actions**. Most claim rows from Supabase Postgres (`erc_8004.wallets` / `wallet_transactions`), query EVM chains over HTTP, save JSON, then call **inline SQL snapshot / upsert** RPCs. There are also **reference-data** workers (Dune queries, token prices), **URI ingest** workers (`uri_documents` + `agent_manifest`), **AI classification** (`web_dashboard.agents` via schema `llm`), and **on-demand backfill** (`on_demand_backfill`: Ethos history/scores + ERC-8183 + Virtual ACP + Olas Mech satellites).

- **Not** Edge Functions / supabase-js in the hot path
- **Not** Cloudflare Workers for these pipelines
- Schema / RPCs live in sibling repo **`gsa-supabase-schema`**

## Read in this order

1. [README.md](./README.md) — workers table, secrets, local run
2. [docs/PROCESSES.md](./docs/PROCESSES.md) — catalog of all live pipelines
3. [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) — claim → save → snapshot (wallets) + URI + on-demand backfill
4. [docs/SUPABASE.md](./docs/SUPABASE.md) — columns, RPCs, monitoring SQL
5. The worker README for the job you touch (`workers/<name>/README.md`)
6. That worker’s `src/db.py` and `job.py` (code of truth)

Ops / stuck wallets: [docs/OPS.md](./docs/OPS.md). Deprecations: [docs/DEPRECATION.md](./docs/DEPRECATION.md) (`ethos-enrich` absorbed by `on_demand_backfill`).  
LP discovery is live; 15-day refresh still pending: [docs/PENDING_LP_POSITIONS.md](./docs/PENDING_LP_POSITIONS.md).  
Token contracts + Alchemy Free design: [docs/TOKEN_CONTRACTS_DISCOVERY_ALCHEMY.md](./docs/TOKEN_CONTRACTS_DISCOVERY_ALCHEMY.md).  
15d activity ingest: [workers/wallet_activity_flows/README.md](./workers/wallet_activity_flows/README.md) → staging `wallets.wallet_activity_transfers`. Probe/enrich census removed ([DEPRECATION.md](./docs/DEPRECATION.md)).

## Hard rules

1. **Eligibility** (wallet claim workers) uses `is_valid_*` + `*_next_eligible_at <= NOW()`, not legacy “status + day window” alone.
2. **Wallet claim workers** pipeline is always claim → RPC → save → `wallet_apply_*_snapshot` → `Processed`. Do not reintroduce pg_cron for those snapshots. **Daily** snapshot writes **`erc_8004.wallet_daily_metrics`** only (`snapshot_date = CURRENT_DATE` in DB timezone, usually UTC); it does **not** update `wallet_transactions` until a rollup job exists. **Reference-data:** `dune_queries_import` (4 Dune queries → upserts, paginated + chunked); `token_prices_import` (DexScreener/CoinGecko → upsert → apply hits → `mark_price_misses`). **URI:** `agent_uri_resolve` / `agent_uri_reprocess` write `uri_documents` + `agent_manifest` directly (no snapshot RPC); do not revive Edge URI batch / `agent-process-uri` for ingest.
3. **Do not revive** deprecated cron jobs listed in [DEPRECATION.md](./docs/DEPRECATION.md).
4. DB helpers are **copy-pasted** per worker (`src/db.py`). If you change reconnect/retry or claim SQL, update **all claim-based workers** unless the change is worker-specific. (`agent_uri_reprocess` reuses `agent_uri_resolve` resolve/handlers via `sys.path`.)
5. Snapshot / upsert / index SQL changes belong in **`gsa-supabase-schema`** migrations (+ `supabase/scripts/`), then deploy to prod before relying on new worker behavior.
6. Prefer fixing workers so they **continue** on transient DB errors (retries + loop continue) rather than exiting 1 on the first SSL drop.

## Workers cheat sheet

| Folder | Workflow | Snapshot / upsert RPC | Destination |
|---|---|---|---|
| `wallet_nonce_balance_daily` | `wallet-nonce-balance-daily.yml` (matrix a/b) | `wallet_apply_daily_snapshot` | `wallet_daily_metrics` (flat); **not** `wallet_transactions` yet (rollup TBD) |
| `owner_wallet_nonce_balance_monthly` | `owner-wallet-nonce-balance-monthly.yml` | `wallet_apply_monthly_snapshot` | `wallet_owner_details` |
| `owner_wallet_origin` | `owner-wallet-origin.yml` | `wallet_apply_owner_history_snapshot` | `wallet_owner_details.first_transaction_at` |
| `dune_queries_import` | `dune-queries-import.yml` | `cex` / `mixer` / `bridge` / `ofac_sanction` upserts | `wallets.cex_addresses`, `mixer_addresses`, `bridge_addresses`, `ofac_sanction_addresses` |
| `token_prices_import` | `token-prices-import.yml` | `token_prices_upsert` + `apply_prices` + `mark_price_misses` | `wallets.token_prices` → positions |
| `wallet_token_contracts_discovery` | `wallet-token-contracts-discovery.yml` | `wallets.wallet_token_contracts_upsert` | `wallets.wallet_token_contracts` |
| `wallet_token_portfolio_discovery` | `wallet-token-portfolio-discovery.yml` | `wallets.wallet_token_positions_insert` | `wallets.wallet_token_positions` (fungible) |
| `wallet_lp_positions_discovery` | `wallet-lp-positions-discovery.yml` | `wallets.wallet_lp_positions_upsert` | `wallets.wallet_lp_positions` (NFT + classic LP) |
| `wallet_activity_flows` | `wallet-activity-flows.yml` (matrix etherscan / alchemy_k1 / bsc / xlayer) | `wallets.wallet_activity_transfers_insert` | `wallets.wallet_activity_transfers` (staging INSERT-only) |
| `agent_uri_resolve` | `agent-uri-resolve.yml` | direct SQL upsert | `uri_documents` + `agent_manifest` (ingest) |
| `agent_uri_reprocess` | `agent-uri-reprocess.yml` | direct SQL upsert | error retry + off-chain `uri_documents` refresh |
| `ai_agent_classifier` | `ai-agent-classifier.yml` | direct SQL | `web_dashboard.agents` AI category fields (`llm` config) |
| `on_demand_backfill` | `on-demand-backfill.yml` | Ethos claim/complete + scores; 8183 + Virtual ACP + Olas Mech `claim/complete_satellite_backfill` | `ethos.*` + `official_scores` + `bsc_erc_8183` / `virtual_acp` / `olas_mech` satellites |
| `erc8257_tools_import` | `erc8257-tools-import.yml` | `erc_8257.tools_upsert` + `sync_state` | `erc_8257.tools` (agenttoolindex full dump) |
| `agent_endpoint_liveness` | `agent-endpoint-liveness.yml` | `agent_endpoint_health_sync` / `_claim` / `_complete_batch` | `erc_8004.agent_endpoint_health` (15d HTTP census) |
| `ethos_reviews_api` | `ethos-reviews-api.yml` | `claim_reviews_fetch` / `complete_reviews_fetch` | `ethos.reviews` (Ethos API v2; GSA-linked Claimed) |

LP 15-day refresh worker: **not built** — see [docs/PENDING_LP_POSITIONS.md](./docs/PENDING_LP_POSITIONS.md).  
Agent manifest **consume** (profile / feedbacks / liveness / sentinel): **not built** — keep legacy consume off until readers JOIN `uri_documents`.  
Walcert consume of `wallet_activity_transfers` (normalize / `analyze_recent_flows` / DELETE staging): **not in this worker** — follow-up.

## How to validate a change

1. Local: `cd workers/<name>`, `uv sync`, `uv run python job.py` with `SUPABASE_DB_URL` (+ Alchemy / Dune / CoinGecko / `PINATA_GATEWAY` / `SCRAPING_ANT_KEY` / `GROQ` as needed). URI workers also need `uv run playwright install chromium`.
2. Or GitHub Actions → workflow → **Run workflow** (`workflow_dispatch`).
3. Logs: `Claimed batch`, reconnect/retry, snapshot failures (wallet claim), Dune tasks / chunk upserts, token-price enrich, discovery `Done wt_id=`, activity flows `Done wt_id=`, URI `Claimed agents` / `on-chain` / `Reprocess` / `Refresh`, classifier `Done agent_id=`, on-demand backfill `Step done name=` / `skipped_empty` / `Claimed history` / `Claimed satellite`, endpoint liveness `queue empty` / `Claimed batch`, or Ethos reviews `Done profile_id=`.
4. SQL: eligible counts in [docs/SUPABASE.md](./docs/SUPABASE.md) (wallets + URI + AI classifier sections); Ethos/8183 catch-up counts in worker README / vault Monitoreo.

## When to touch which repo

| Change | Repo |
|---|---|
| Claim SQL, retries, job loop, RPC clients, GHA env | **gsa-workers** |
| `wallet_apply_*_snapshot`, Dune reference upserts (`cex`/`mixer`/`bridge`/`ofac`), token_prices / discovery upserts, `uri_documents` / `agent_manifest` indexes & helpers, triggers, `next_eligible_at` / discovery flags, `llm.*` / agent AI category columns, `ethos.claim_reviews_fetch` / watermark | **gsa-supabase-schema** |
| Deploy order | Schema first (if needed) → push worker → `workflow_dispatch` |
