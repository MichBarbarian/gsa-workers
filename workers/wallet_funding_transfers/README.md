# wallet_funding_transfers

One-shot ingest of the first ~500 **incoming** transfers (native + ERC-20) per `wallet_transactions` row.

**Workflow:** `.github/workflows/wallet-funding-transfers.yml`  
**Schema:** `gsa-supabase-schema` `20260826010000_wallet_funding_transfers.sql`

v1 is **INSERT-only** into `wallets.wallet_funding_transfers`. No analyze, CEX, or WAMI.

## Providers

| Group (`PROVIDER_GROUP`) | Chains | Secret |
|--------------------------|--------|--------|
| `etherscan` | ETH, Arb, Polygon, Celo | `ETHERSCAN_FUNDING_KEY` (not `ETHERSCAN_API_KEY`) |
| `blockscout` | Base, Gnosis | `BLOCKSCOUT_FUNDING_KEY` |
| `bsc` | BSC | `ANKR_FUNDING_KEY` |
| `xlayer` | X Layer | `OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE` |

## Claim

All mapped chains. `ORDER BY` non-`Dormant_*` first. Success → `funding_transfers_next_eligible_at = infinity`. Empty result is OK.

Quota exhausted (Etherscan/Blockscout daily, Ankr monthly): unlock remaining claims, **exit 0**, wait for next cron. Transient 429: short backoff.

## Local

```powershell
cd workers/wallet_funding_transfers
copy .env.example .env
uv sync
$env:PROVIDER_GROUP="etherscan"
uv run python job.py
```

Deploy schema first. Cron: `0 */6 * * *` UTC + `workflow_dispatch`.
