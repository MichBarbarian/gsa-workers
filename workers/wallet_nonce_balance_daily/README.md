# Wallet nonce balance daily

> Project context: [AGENTS.md](../../AGENTS.md) · [Supabase map](../../docs/SUPABASE.md) · [Architecture](../../docs/ARCHITECTURE.md)

Batch job that queries native balance and nonce across 8 EVM chains for wallets in `erc_8004.wallets`, persists JSON results, and applies the daily snapshot inline (`wallet_daily_metrics` + `Processed` status). Does **not** update `wallet_transactions` until a rollup exists.

**Schedule (UTC):** `0 / 6 / 12 / 18` (ventana **00–24**). Matrix `worker-a` / `worker-b` + `workflow_dispatch`.

## Eligibility (`import_nonce_and_balance_daily_next_eligible_at`)

The worker claims wallets when:

```sql
is_valid_import_current_nonce_and_balance_daily IS TRUE
AND import_nonce_and_balance_daily_next_eligible_at <= NOW()
```

| Value | Meaning |
|---|---|
| `-infinity` | Never processed; eligible immediately |
| `<= NOW()` | New UTC calendar day due or stale Pending |
| `> NOW()` | Already processed today or in-flight |
| `NULL` | Out of worker scope (`is_valid` is false) |

### Maintenance

| Event | Who updates `next_eligible_at` |
|---|---|
| New valid wallet (`is_valid` becomes true) | DB trigger `trg_wallet_daily_next_eligible_at` → `-infinity` |
| Claim (worker) | `NOW() + CLAIM_STALE_SECONDS` (default 2h) |
| Save Completed/Error (worker) | Midnight UTC of the next calendar day |
| Snapshot success | Status → `Processed` (schedule unchanged from save) |

Legacy columns (`import_nonce_and_balance_daily_at`, `import_nonce_and_balance_daily_claimed_at`, JSON) remain for auditing.

## Pipeline

1. Claim batch via partial index on `next_eligible_at`
2. Query 8 chains in parallel (shared HTTP client; public RPCs first, Alchemy backup)
3. Batch save JSON with status `Completed` or `Error`
4. Call `erc_8004.wallet_apply_daily_snapshot(wallet_id)` for each `Completed` wallet
5. Snapshot writes flat rows to `erc_8004.wallet_daily_metrics` and sets `import_nonce_and_balance_daily_last_status = 'Processed'`

`wallet_transactions` is **not** updated by this snapshot anymore (rollup comes later). `chain_nonces` write/purge is also out of this path.

The pg_cron job `wallet_update_transactions` is deprecated; snapshot runs inline in this worker.

Claim, batch save, and snapshot reconnect and retry up to 3 times on Supabase connection drops (`OperationalError` / `InterfaceError`), so a transient SSL/DB close does not kill the whole run.

Retryable DB errors (timeout, connection, deadlock) are retried 3×; if they persist, the worker skips that batch and continues until the time budget.

## Manual re-queue

Force wallets back into the claim queue after errors:

```sql
UPDATE erc_8004.wallets
SET import_nonce_and_balance_daily_next_eligible_at = '-infinity'
WHERE is_valid_import_current_nonce_and_balance_daily IS TRUE
  AND import_nonce_and_balance_daily_last_status = 'Error';
```

## Monitoring

Eligible count (uses partial index):

```sql
SELECT COUNT(*) AS eligible_now
FROM erc_8004.wallets
WHERE is_valid_import_current_nonce_and_balance_daily IS TRUE
  AND import_nonce_and_balance_daily_next_eligible_at <= NOW();
```

Claim plan check (target: Index Only Scan, sub-100 ms):

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT w.id
FROM erc_8004.wallets w
WHERE w.is_valid_import_current_nonce_and_balance_daily IS TRUE
  AND w.import_nonce_and_balance_daily_next_eligible_at <= NOW()
ORDER BY w.import_nonce_and_balance_daily_next_eligible_at, w.id
LIMIT 200;
```

Backfill completeness (should be 0 before deploying worker):

```sql
SELECT COUNT(*) AS null_valid
FROM erc_8004.wallets
WHERE is_valid_import_current_nonce_and_balance_daily IS TRUE
  AND import_nonce_and_balance_daily_next_eligible_at IS NULL;
```

## Environment

| Variable | Default | Description |
|---|---|---|
| `SUPABASE_DB_URL` | required | Postgres connection string |
| `ALCHEMY_KEY` | optional | Alchemy fallback after public RPCs |
| `WORKER_ID` | `worker-a` | Claim identity (`worker-a` / `worker-b`) |
| `CONCURRENCY` | 20 | Parallel wallets (max 20) |
| `CLAIM_BATCH_SIZE` | 200 | Wallets per claim batch |
| `CLAIM_STALE_SECONDS` | 7200 | Re-claim delay after Pending claim |
| `MAX_RUNTIME_SECONDS` | 19800 | Internal time budget (~5.5h) |

## Deployment order

1. Apply schema migrations (`daily_next_eligible_at`, `wallet_apply_daily_snapshot`, deprecate cron)
2. Verify backfill: `null_valid = 0`
3. Deploy worker and run `workflow_dispatch` on both matrix jobs (`worker-a`, `worker-b`)
4. Confirm claims stay fast and wallets end in `Processed` with rows in `wallet_daily_metrics` for today's `snapshot_date` (UTC)
