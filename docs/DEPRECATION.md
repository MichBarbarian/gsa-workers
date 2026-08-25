# Deprecation notes

## Ethos enrich workflow (absorbed by on-demand backfill)

| Legacy | Status |
|---|---|
| Workflow `ethos-enrich.yml` | **Removed** — replaced by `on-demand-backfill.yml` |
| Worker folder `workers/ethos_enrich/` | Thin shim / reference only; live code in `workers/on_demand_backfill/` |

Do **not** re-add a dedicated Ethos enrich cron. Steps `ethos_history` + `ethos_scores` run inside the orchestrator.

The dedicated worker `ethos_reviews_api` (`ethos-reviews-api.yml`) is **not** that enrich job: it only fills `ethos.reviews` via Ethos API v2 (ADR 2026-08-25) after Goldsky dropped review entities.

## pg_cron (do not re-enable)

These jobs were replaced by **inline** `wallet_apply_*_snapshot` calls in the GitHub Actions workers. Functions may remain as no-op stubs; cron should stay **disabled**.

| Cron job name | Replaced by |
|---|---|
| `wallet_update_transactions` | `wallet_nonce_balance_daily` → `wallet_apply_daily_snapshot` |
| `wallet_owner_update_transactions` | `owner_wallet_nonce_balance_monthly` → `wallet_apply_monthly_snapshot` |
| `wallet_owner_update_first_transactions` | `owner_wallet_origin` → `wallet_apply_owner_history_snapshot` |

`wallet_hourly_process` no longer toggles those OwnerTx / daily snapshot crons.

## Phase 2 — Cloudflare / Edge (after daily is stable)

After `wallet_nonce_balance_daily` is validated in production:

### Cloudflare

- Worker: `wallet-snapshot` in `gsa-cloudflare-workers`
- Route: `api.globalscoreagent.com/wallet-snapshot*`
- Action: disable route and retire worker when GitHub Actions covers all use cases

### Supabase Edge Functions

- `wallets-query-snapshot` — proxy to Cloudflare Worker (deprecate with daily)
- `wallet-transactional-current-batch` — **remains** for `wallet_transactional_details` (different table/flow)

Only remove components after confirming no external consumers depend on them.

## Owner wallet origin (future)

After `owner_wallet_origin` is validated in production, consider deprecating:

- Standalone `query_wallet_origin.py` CLI tool (replaced by this worker)
- Any manual origin-import scripts or one-off jobs writing `import_wallet_history_data`

## Agent URI ingest (Edge → GHA)

Current: **`agent_uri_resolve`** (00:00 / 12:00 UTC) materializes agents + feedbacks into `uri_documents` / `agent_manifest`. **`agent_uri_reprocess`** (06:00 / 18:00) retries download errors and refreshes off-chain HTTP/IPFS docs older than 15 days.

Do **not** re-enable Edge URI batch processors or legacy manifest reprocess cron for ingest. Canonical JSON lives in `uri_documents` (`uri_hash`); `agent_manifest.data` / `url` are dropped.

| Legacy component | Status |
|---|---|
| Edge `agent-uri-batch-processor` | Superseded by `agent_uri_resolve`; keep **disabled** |
| Edge `feedback-uri-batch-processor` | Superseded by `agent_uri_resolve`; keep **disabled** |
| Edge `agent-process-uri` (ingest path) | Superseded; keep **disabled** for ingest |
| pg_cron / SPs that reprocessed `agent_manifest.url` / `data` | Superseded by `agent_uri_reprocess`; leave off |
| Manifest **entity consume** pg_cron (profile / feedbacks / liveness / sentinel) | Still **off** until SQL readers JOIN `uri_documents`; not replaced by these two workers |

Ops may delete leftover URI Edge/cron artifacts in the schema repo; workers do not call those stubs.

### Scrape.do → ScrapingAnt (2026-08-15)

URI HTTP last-fallback moved from `SCRAPE_DO_TOKEN` / `scrape/scrape_do.py` to `SCRAPING_ANT_KEY` / `scrape/scraping_ant.py`. Do not reintroduce Scrape.do in workflows. Historical `source_gateway` values `scrape-do-*` remain in `uri_documents`; new successes use `scraping-ant-*`.

### Pinata dedicated gateway host (2026-08-15)

IPFS cascade: public gateways first (`ipfs.io` → `gateway.pinata.cloud` → Cloudflare → dweb), then **Pinata dedicated** last if `PINATA_GATEWAY` is set. Host is `violet-efficient-aardwolf-922.mypinata.cloud` (replaces retired `indigo-urban-flyingfish-439`). Secret value is the **Gateway Key** (`x-pinata-gateway-token`), not Pinata API Key/Secret/JWT. Historical `pinata-dedicated` rows keep the old host in past fetches only via `source_gateway` label (same name).

## Token prices (walcert → GHA → Dex/CoinGecko)

Current: **`token_prices_import`** enriches unpriced `wallet_token_positions` via DexScreener → CoinGecko into spot cache `wallets.token_prices` (PK `chain_id`+`contract`), then `wallet_token_positions_apply_prices`.

Dune daily dumps into `wallets.token_prices` are **retired** (table redesigned 2026-07-13). Do not re-enable Dune export for this table.

| Legacy component | Status |
|---|---|
| pg_cron `walcert_token_prices_import_data` (`0 1 * * *`) | Keep **disabled**; do not re-enable |
| pg_cron `walcert_token_prices_process` (`0 13 * * *`) | Keep **disabled**; do not re-enable |
| `walcert.token_prices_import_data` (pg_net → Edge) | Superseded; leave in place until Edge retired |
| `walcert.token_prices_process` | Superseded; leave in place |
| Edge `walcert-update-token-prices` | Superseded; retire after GHA is validated |
| `walcert.token_prices` / `walcert.token_prices_imported_data` | Legacy; GHA writes go to redesigned `wallets.token_prices` |
| Dune query `7526826` → old daily `wallets.token_prices` | Retired with spot-cache redesign |

Repointing consumers that still read `walcert.token_prices` is a separate follow-up.

## Token activity probe / enrich (replaced by wallet activity flows 15d)

Census getLogs probe + enrich-flag pipeline was removed (ADR 2026-08-13). Do **not** revive it.

| Legacy | Status |
|---|---|
| Workflow `wallet-token-activity-scan.yml` | **Removed** — replaced by `wallet-activity-flows.yml` |
| Worker folder `workers/token_activity/` (probe; enrich never existed) | **Removed** |
| Docs `docs/token_activity/` + `docs/PENDING_TOKEN_ACTIVITY_RPC.md` | **Removed** |
| Columns `token_activity_*` / `does_need_token_activity_enrich` / `chains.token_activity_runner_count` | Dropped in schema migration `20260813010000_wallet_activity_transfers.sql` |
| Rollup Fuente B (`does_need_token_activity_enrich` on D vs D−1) | Removed from `wallet_rollup_daily_metrics` |

Live replacement: `workers/wallet_activity_flows/` → INSERT-only staging `wallets.wallet_activity_transfers`. Do not re-add probe getLogs, enrich flags, or Fuente B enqueue.
