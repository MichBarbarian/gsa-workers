# Operations

Runbook for stuck wallets, failed Actions runs, and when to change schema vs workers.

## Stuck states

| Symptom | Likely cause | Action |
|---|---|---|
| Many `Pending`, `next_eligible_at` in the future | Claimed then job died before save | Wait until `CLAIM_STALE_SECONDS` (2h) or force re-queue (below) |
| `Completed` with non-empty JSON, not `Processed` | Snapshot never ran (old cron off / worker crash mid-batch) | Backfill `wallet_apply_*_snapshot` in batches of 50 — see [SUPABASE.md](./SUPABASE.md) |
| `Error` | RPC all-chains failed or snapshot marked error | Fix root cause; re-queue with `next_eligible_at = '-infinity'` |
| Claim timeouts in logs | Heavy DB load / missing index | Check `EXPLAIN` on claim; confirm partial index on `next_eligible_at` in schema repo |

### Force re-queue (example: daily Errors)

```sql
UPDATE erc_8004.wallets
SET import_nonce_and_balance_daily_next_eligible_at = '-infinity'
WHERE is_valid_import_current_nonce_and_balance_daily IS TRUE
  AND import_nonce_and_balance_daily_last_status = 'Error';
```

Adjust column names for monthly / origin ([SUPABASE.md](./SUPABASE.md)).

## Interpreting worker logs

### Claim workers

| Log line | Meaning |
|---|---|
| `Claimed batch size=...` | Claim OK; RPC about to run |
| `Reconnecting to Postgres after connection failure` | Transient SSL/DB drop; retry in progress |
| `Claim failed; will retry next loop` | Claim exhausted retries; loop continues |
| `Save/snapshot failed for batch; wallets stay Pending` | Batch not persisted; reclaim after stale window |
| `Snapshot failed for wallet id=...` | That wallet marked Error (or retried); others continue |
| `Time budget reached` | Soft stop (`MAX_RUNTIME_SECONDS`); exit 0 |
| `Critical job failure` | Unexpected error outside the DB continue path |

### Ethos reviews API

| Log line | Meaning |
|---|---|
| `Claimed batch size=` | Claim OK; Ethos activities about to run |
| `Done profile_id=` | Upsert + complete for that profile |
| `queue empty` | No GSA-linked Claimed profiles due; exit 0 |
| `Time budget reached` | Soft stop; remaining stay claimed until stale 2h |

Schema RPCs: `ethos.claim_reviews_fetch` / `complete_reviews_fetch`. Do not confuse with `ethos_scores` (credibility, TTL 15d) inside `on_demand_backfill`.

### Dune queries import

| Log line | Meaning |
|---|---|
| `=== Task N/4: … ===` | Starting one of cex / mixers / bridges / ofac |
| `Fetching Dune query … page N` | HTTP page fetch in progress |
| `Waiting … before next Dune page` | Rate-limit pacing between pages |
| `Waiting … before next task` | Rate-limit pacing between tasks |
| `Dune HTTP 429 rate limited; sleeping` | Hit Free/Plus rpm cap; retrying with backoff |
| `Fetched N rows for …; upserting in chunks` | Dune OK; chunked upsert starting |
| `Task … upsert chunk i/j` | One RPC chunk committed |
| `Task … OK` | That task succeeded |
| `Task … failed` | Task error; other tasks still run |
| `Finished … with failures` | Exit 1; at least one task failed |

## Dune queries import

Reference-data job (`dune_queries_import`). No claim / `Pending` / `next_eligible_at`. Four Dune queries per run (paginated fetch + chunked upsert). Schedule: day **18** 00:00 UTC (`0 0 18 * *`) after typical Dune billing reset ~17th (+ `workflow_dispatch`).

| Symptom | Likely cause | Action |
|---|---|---|
| Workflow fails immediately | Missing/invalid `DUNE_KEY` or `SUPABASE_DB_URL` | Check repo secrets; re-run |
| Task fails with HTTP **402** / datapoint limit | Billing-cycle quota exhausted (often `cex`) | Wait for reset (~17th) or raise limit; re-run with `dune_tasks=cex` when quota allows |
| Task fails with 0 rows | Empty/stale Dune result for that query | Check the query on Dune; re-run when data exists |
| Upsert / function does not exist | Migration not applied | Deploy `wallets_*_upsert` + tables in **gsa-supabase-schema** first |
| Upsert timeout | Large chunk / DB load | Lower `UPSERT_CHUNK_SIZE`; check `statement_timeout` (worker uses 600s) |
| One task failed, others OK | Partial run | Fix that query/RPC; re-run workflow (upserts are idempotent) |
| `max(updated_at)` old | Schedule not run or last run failed | `workflow_dispatch` on **Dune queries import** |

**Re-run:** GitHub → **Actions** → **Dune queries import** → **Run workflow**.

**Verify after a successful run** (CEX ~36k for query `7520736`):

```sql
SELECT 'cex' AS src, count(*) AS rows, max(updated_at) AS last_updated FROM wallets.cex_addresses
UNION ALL
SELECT 'mixers', count(*), max(updated_at) FROM wallets.mixer_addresses
UNION ALL
SELECT 'bridges', count(*), max(updated_at) FROM wallets.bridge_addresses
UNION ALL
SELECT 'ofac', count(*), max(updated_at) FROM wallets.ofac_sanction_addresses;
```

More monitoring SQL: [SUPABASE.md](./SUPABASE.md). Worker details: [dune_queries_import/README.md](../workers/dune_queries_import/README.md).

## Token prices import

Reference-data job (`token_prices_import`). No claim / `Pending` / `next_eligible_at`.

Enriches unpriced ERC-20 rows via cache → DexScreener → CoinGecko. Requires `COINGECKO_KEY`.

| Symptom | Likely cause | Action |
|---|---|---|
| Workflow fails immediately | Missing/invalid `COINGECKO_KEY` or `SUPABASE_DB_URL` | Check repo secrets; re-run |
| CoinGecko 429 | Demo rate/credits | Rely on Dex + TTL; upgrade plan |
| Upsert / apply missing | Migration not applied | Deploy spot-cache + `mark_price_misses` / upsert DISTINCT ON in **gsa-supabase-schema** |
| `CardinalityViolation` on upsert | Duplicate `(chain_id, contract)` in one batch | Fixed via candidate `DISTINCT ON` + upsert dedupe; redeploy if old code |
| Positions still `has_price_error` forever | Misses not marked | Need `mark_price_misses` after miss upsert; check `quality_reason` |
| Positions still unpriced USD | Miss / spam / low liquidity | Check `token_prices.source` and `token_quality`; known-unknown uses `unknown_token_dex_coingecko_defillama` |

**Re-run:** GitHub → **Actions** → **Token prices import** → **Run workflow**.

```sql
SELECT source, count(*), count(*) FILTER (WHERE price_usd IS NOT NULL) AS with_price
FROM wallets.token_prices
GROUP BY 1;
```

Worker details: [token_prices_import/README.md](../workers/token_prices_import/README.md).

## Agent URI resolve / reprocess

URI workers claim agents / feedbacks / `agent_manifest` / `uri_documents` (not wallet `Pending` status). Soft `MAX_RUNTIME_SECONDS=19800`. Optional secrets: `PINATA_GATEWAY` (dedicated gateway key → last IPFS fallback after public gateways; host `violet-efficient-aardwolf-922.mypinata.cloud`), `SCRAPING_ANT_KEY`.

### Interpret logs

| Log pattern | Meaning |
|---|---|
| `Claimed agents batch size=` | Resolve claimed agents |
| `Claimed on-chain feedbacks batch size=` | On-chain DB materialize |
| `Claimed feedbacks batch size=` | External URI/endpoint feedbacks |
| `Claimed error manifests batch size=` | Reprocess download-error queue |
| `Claimed refresh docs batch size=` | Reprocess off-chain &gt;15d queue |
| `Refresh unchanged doc_id=` | Document same; TTL renewed only |
| `stamped fetched_at` / `refresh_error:` | Refresh fetch failed; previous JSON kept; `fetched_at` advanced so the row leaves the current 15d window |
| `Time budget reached` | Soft stop; exit 0; next cron continues |
| Playwright / scrape / download failures | Recorded as download error on manifest; `agent_uri_reprocess` retries (max 3) |

### Symptoms

| Symptom | Likely cause | Action |
|---|---|---|
| `agents_pending` stuck high | Resolve not running / claim index miss | Check GHA `agent-uri-resolve` schedule; confirm `is_uri_processed = false` + indexes in schema |
| Manifests with `has_download_error` forever | Exhausted `reprocess_count` (≥3) or not due yet | Wait 3d between retries; or set `does_need_manual_reprocess`; inspect URI from agents/feedbacks |
| Off-chain docs never refresh | Wrong schedule or not HTTP/IPFS | Reprocess only at 06/18; hex/on-chain excluded by design |
| Same `doc_id` claimed all run; `refresh=` high but queue stuck | Failed fetch did not stamp `fetched_at` (fixed: stamp on fail) | Confirm worker ≥ stamp-on-fail commit; look for `refresh_error:` on `source_gateway` |
| Duplicate URI content across rows | Legacy pre-`uri_hash` data | Schema migration `00066` path; upsert is by `uri_hash` |

**Re-run:** Actions → **agent-uri-resolve** or **agent-uri-reprocess** → **Run workflow**.

Monitoring SQL: [SUPABASE.md](./SUPABASE.md) (Agent URI sections). READMEs: [`agent_uri_resolve`](../workers/agent_uri_resolve/README.md), [`agent_uri_reprocess`](../workers/agent_uri_reprocess/README.md).

## Wallet funding transfers

One-shot ingest of the first ~500 **incoming** native+ERC-20 transfers (`workers/wallet_funding_transfers`). Claims `erc_8004.wallet_transactions` (`is_valid_funding_transfers`). Success sets `funding_transfers_next_eligible_at = infinity`. Empty wallet still completes. Does **not** write `walcert.wallet_fund_origins`.

Matrix groups: `etherscan` · `blockscout` · `bsc` · `xlayer`. Cron every 6h UTC + `workflow_dispatch`.

| Log line | Meaning |
|---|---|
| `Claimed batch size=` | Claim OK; provider fetch about to run |
| `Done wt_id=` | Insert + complete (possibly 0 rows) |
| `{PROVIDER}_QUOTA_EXHAUSTED stop_claim=1` | Daily (Etherscan/Blockscout) or monthly (Ankr) credits gone; in-flight rows unlocked; **exit 0** |
| `queue empty` | No due rows for this group; exit 0 |

| Symptom | Likely cause | Action |
|---|---|---|
| Workflow fails on missing secret | `ETHERSCAN_FUNDING_KEY` / `BLOCKSCOUT_FUNDING_KEY` / `ANKR_FUNDING_KEY` | Use **funding** secrets, not 15d `ETHERSCAN_API_KEY` / `ANKR_API_KEY` |
| Group exits 0 after a few wallets | Quota circuit breaker | Wait next UTC day (Etherscan/Blockscout) or month (Ankr); do not retry in the same run |
| `due_active` never falls | Claim SQL / seed flags | Confirm migration `20260826010000_wallet_funding_transfers.sql`; check `is_valid_funding_transfers` |
| Staging empty after Done | Wallet has no incoming native/ERC-20 | Expected; one-shot still completes |

**Re-run:** Actions → **Wallet funding transfers** → **Run workflow** (empty `provider_group` = all four cells).

```sql
SELECT
  count(*) FILTER (WHERE is_valid_funding_transfers IS TRUE) AS seeded,
  count(*) FILTER (
    WHERE is_valid_funding_transfers IS TRUE
      AND COALESCE(wallet_category, '') NOT LIKE 'Dormant_%'
      AND funding_transfers_next_eligible_at <= NOW()
  ) AS due_active,
  count(*) FILTER (WHERE funding_transfers_completed_at IS NOT NULL) AS completed
FROM erc_8004.wallet_transactions;
```

Worker README: [`wallet_funding_transfers`](../workers/wallet_funding_transfers/README.md).

## Dual daily workers

`wallet_nonce_balance_daily` runs **two** GHA jobs (`worker-a`, `worker-b`) with separate concurrency groups. They share the same claim SQL (`FOR UPDATE SKIP LOCKED`), so batches do not overlap. Both need the same secrets.

Do not lower `CLAIM_STALE_SECONDS` too far: a slow RPC batch must finish before another runner reclaims the same wallets.

## Alchemy / RPC

- Public RPCs first (`networks.py`); Alchemy is fallback (`ALCHEMY_KEY` + `erc_8004.chains.subdomain_alchemy`).
- HTTP 500s from a public endpoint are normal; the client tries the next URL / Alchemy.
- If Alchemy is missing, expect more `Error` wallets on flaky public RPCs.

## Re-run a job

GitHub → **Actions** → workflow name → **Run workflow** (`workflow_dispatch`).

No need to change code for a plain re-run. After a schema change, deploy the migration in **gsa-supabase-schema** first.

## Schema vs worker

| Touch | Repo |
|---|---|
| Snapshot / upsert RPCs (`wallet_apply_*`, Dune reference tables, token_prices, discovery, mark_price_misses), URI indexes/helpers, triggers, new columns | `gsa-supabase-schema` |
| Claim/save/retry/job loop/GHA env/RPC / Dune / Dex / CoinGecko / URI resolve clients | `gsa-workers` |

Deploy order when both change: **schema → worker → workflow_dispatch**.

## Related

- [PROCESSES.md](./PROCESSES.md) — live pipeline catalog (#9b funding transfers, #13 on-demand, **#16 Ethos reviews API**)
- [PENDING_LP_POSITIONS.md](./PENDING_LP_POSITIONS.md) — LP 15-day refresh (discovery already live)
- [SUPABASE.md](./SUPABASE.md) — monitoring and backfill SQL
- [ARCHITECTURE.md](./ARCHITECTURE.md) — pipeline and budgets
- [DEPRECATION.md](./DEPRECATION.md) — do not re-enable old crons / Edge URI
- Workers: [`wallet_lp_positions_discovery`](../workers/wallet_lp_positions_discovery/README.md), [`wallet_funding_transfers`](../workers/wallet_funding_transfers/README.md), [`agent_uri_resolve`](../workers/agent_uri_resolve/README.md), [`agent_uri_reprocess`](../workers/agent_uri_reprocess/README.md), [`ethos_reviews_api`](../workers/ethos_reviews_api/README.md)
