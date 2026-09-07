# wallet_activity_flows

15-day activity ingest for non-`Dormant_*` `wallet_transactions`. **INSERT-only** into
`wallets.wallet_activity_transfers`. Does not compute Walcert metrics or delete staging.

**Workflow:** `.github/workflows/wallet-activity-flows.yml`  
**Schema:** `gsa-supabase-schema` `20260813010000_wallet_activity_transfers.sql` + `20260906060059_activity_flows_agent_ok_claim.sql`

## Providers

| Group (`PROVIDER_GROUP`) | Chains | Vendor |
|--------------------------|--------|--------|
| `etherscan` | ETH, Arb, Polygon, Celo | Etherscan V2 Free |
| `alchemy_k1` | Base, Gnosis | Alchemy Transfers (`ALCHEMY_ACTIVITY_KEY_1`). Gnosis timestamps via `eth_getBlockByNumber` + `block_cache` |
| `bsc` | BSC | Day **1** cut: Alchemy `ALCHEMY_ACTIVITY_KEY_2`. Day **15** cut: Ankr (`pageSize=1000`). **Through 2026-08-31 UTC** remaining day-15 BSC drains on Alchemy key_2 (Ankr credits exhausted). |
| `xlayer` | X Layer | OKX **Data API** (not Market API) |

## Claim

`is_valid_activity_flows` + `activity_flows_agent_ok` + `activity_flows_next_eligible_at <= now()` + `wallet_category NOT LIKE 'Dormant_%'`. Success → next UTC cut (day 15 or day 1 next month). Empty queue → exit 0.

## Secrets (GHA)

Create a **dedicated Alchemy Free app** for BSC (`ALCHEMY_ACTIVITY_KEY_2`). Do **not** reuse `ALCHEMY_FREE_KEY` (token discovery).

| Secret | Use |
|--------|-----|
| `SUPABASE_DB_URL` | Postgres |
| `ETHERSCAN_API_KEY` | ETH/Arb/Polygon/Celo |
| `ALCHEMY_ACTIVITY_KEY_1` | Base + Gnosis |
| `ALCHEMY_ACTIVITY_KEY_2` | BSC day-1 cut; also leftover day-15 BSC through 2026-08-31 UTC |
| `ANKR_API_KEY` | BSC day-15 cut (from 2026-09-01 UTC) |
| `OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE` | X Layer Data API |

## Local

```powershell
cd workers/wallet_activity_flows
copy .env.example .env
uv sync
uv run python job.py
uv run python scripts/smoke_providers.py
```

Deploy schema first, then this worker. Active UTC window **18:00→12:00** (closed 12–18). Cron: `0 0 1,15 * *` + `0 18,22,2,6,10 * * *`. Local outside window: `IGNORE_SCHEDULE_WINDOW=1`.
