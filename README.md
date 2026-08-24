# GSA Workers

Unified Python batch workers for [Global Score Agent](https://www.globalscoreagent.com/), run via GitHub Actions against Supabase Postgres.

**For AI agents:** start at [AGENTS.md](./AGENTS.md). Process catalog: [docs/PROCESSES.md](./docs/PROCESSES.md) (wallet pipelines + URI #10–11 + **on-demand backfill** #13). Architecture / DB / ops: [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md), [docs/SUPABASE.md](./docs/SUPABASE.md), [docs/OPS.md](./docs/OPS.md). LP **15-day refresh** still pending: [docs/PENDING_LP_POSITIONS.md](./docs/PENDING_LP_POSITIONS.md).

## Workers

| Worker | Schedule (UTC) | Eligibility | Description |
|---|---|---|---|
| [`wallet_nonce_balance_daily`](./workers/wallet_nonce_balance_daily/README.md) | 0, 6, 12, 18h (matrix `worker-a`/`worker-b`) | `is_valid_..._daily` + `import_nonce_and_balance_daily_next_eligible_at` | Balance + nonce → daily JSON → `wallet_apply_daily_snapshot` → `wallet_daily_metrics` |
| [`owner_wallet_origin`](./workers/owner_wallet_origin/README.md) | 0, 6, 12, 18h | monthly `is_valid` + `import_wallet_history_next_eligible_at` | First on-chain activity → history JSON → `wallet_apply_owner_history_snapshot` |
| [`owner_wallet_nonce_balance_monthly`](./workers/owner_wallet_nonce_balance_monthly/README.md) | 0, 6, 12, 18h | `is_valid_..._monthly` + `import_nonce_and_balance_monthly_next_eligible_at` | Balance + nonce (30d) → monthly JSON → `wallet_apply_monthly_snapshot` |
| [`dune_queries_import`](./workers/dune_queries_import/README.md) | 18th 00:00 (monthly; post Dune billing reset) | n/a (reference data) | 4 Dune queries → cex/mixer/bridge/ofac upserts |
| [`token_prices_import`](./workers/token_prices_import/README.md) | 0, 6, 12, 18h | n/a (reference data) | Dex/CG → `token_prices` → apply / mark known-unknown misses |
| [`wallet_token_contracts_discovery`](./workers/wallet_token_contracts_discovery/README.md) | 0, 6, 12, 18h | `wallet_transactions.does_need_discovery_contracts` + `chains.subdomain_alchemy` | Alchemy ERC-20 balances → `wallet_token_contracts_upsert` |
| [`wallet_token_portfolio_discovery`](./workers/wallet_token_portfolio_discovery/README.md) | 0, 6, 12, 18h | portfolio discovery flag after contract discovery | Alchemy amounts + DeFiLlama → fungible `wallet_token_positions` |
| [`wallet_lp_positions_discovery`](./workers/wallet_lp_positions_discovery/README.md) | 0, 6, 12, 18h | LP flag after portfolio discovery | UniV3 NFT + `lp_pools` classic → `wallet_lp_positions` |
| [`wallet_activity_flows`](./workers/wallet_activity_flows/README.md) | 1/15 00:00 + every 4h | `is_valid_activity_flows` + not `Dormant_*` | 15d transfers → staging `wallets.wallet_activity_transfers` |
| [`agent_uri_resolve`](./workers/agent_uri_resolve/README.md) | 00:00, 12:00 | agents / `feedback_on_chain` / external feedbacks pending | Resolve/materialize → `uri_documents` + `agent_manifest` |
| [`agent_uri_reprocess`](./workers/agent_uri_reprocess/README.md) | 06:00, 18:00 | download errors (max 3) + off-chain docs &gt;15d | Retry errors; refresh HTTP/IPFS; `is_processed` only if document changed |
| [`ai_agent_classifier`](./workers/ai_agent_classifier/README.md) | 0, 6, 12, 18h | `web_dashboard.agents.does_need_ai_category_process` | LLM categories → `ai_category_*` (+ `llm.models_requests` rate limits) |
| [`on_demand_backfill`](./workers/on_demand_backfill/README.md) | 0, 6, 12, 18h | per-step queues (Ethos history/scores, ERC-8183 + Virtual ACP + Olas Mech satellites) | Orchestrator catch-up after late wallet link |
| [`erc8257_tools_import`](./workers/erc8257_tools_import/README.md) | 04:00 daily | n/a (reference data) | agenttoolindex dump → `erc_8257.tools` (+ sync_state watermark) |
| [`agent_endpoint_liveness`](./workers/agent_endpoint_liveness/README.md) | 0, 6, 12, 18h | HTTP(s) locators in `agent_metadata_services` due on 15d clock | HEAD/GET census → `erc_8004.agent_endpoint_health` |

Pending: [LP 15-day refresh](./docs/PENDING_LP_POSITIONS.md). Manifest **consume** (entity SPs) not built yet. `ethos_enrich` → absorbed by `on_demand_backfill` ([DEPRECATION](./docs/DEPRECATION.md)).

## Common pipeline (claim workers)

```
claim (Pending, next_eligible_at += CLAIM_STALE_SECONDS)
  → RPC (8 chains, public then Alchemy)
    → save (Completed|Error + schedule next run)
  → wallet_apply_*_snapshot → Processed
```

Reference-data: `dune_queries_import` (4 Dune queries → upserts); `token_prices_import` (Dex/CG enrich + miss mark); `erc8257_tools_import` (agenttoolindex → `erc_8257.tools`). Full catalog: [docs/PROCESSES.md](./docs/PROCESSES.md). Column/RPC inventory: [docs/SUPABASE.md](./docs/SUPABASE.md).

## Secrets

| Secret | Required | Role |
|---|---|---|
| `SUPABASE_DB_URL` | Yes | Postgres pooler DSN |
| `ALCHEMY_KEY` | Recommended | Alchemy fallback after public RPCs (claim workers) |
| `ALCHEMY_FREE_KEY` | For token contracts / portfolio / LP discovery | Alchemy Token API + eth_call |
| `DUNE_KEY` | For CEX import | Dune Analytics API key |
| `COINGECKO_KEY` | For token-prices enrich | CoinGecko Demo/Pro API key |
| `PINATA_GATEWAY` | Optional (URI workers) | Pinata dedicated gateway access token (last IPFS fallback after public gateways) |
| `SCRAPING_ANT_KEY` | Optional (URI workers) | ScrapingAnt API key (last HTTP fallback) |
| `GROQ` | For AI agent classifier | Groq API key (`llm.llm_provider.secret`) |
| `NVIDIA` | Optional (classifier NIM) | Hosted NVIDIA NIM key (`llm.llm_provider.secret`) |
| `MISTRAL` | Optional (classifier Mistral) | AI Studio / La Plateforme Experiment key (`llm.llm_provider.secret`) |
| `CLOUDFLARE` | Optional (classifier Workers AI) | Cloudflare API token with Workers AI + AI Gateway (`llm.llm_provider.secret`) |
| `ETHERSCAN_API_KEY` | For activity flows (ETH/Arb/Polygon/Celo) | Etherscan V2 Free |
| `ALCHEMY_ACTIVITY_KEY_1` | For activity flows (Base + Gnosis) | Dedicated Transfers key — not `ALCHEMY_FREE_KEY` |
| `ALCHEMY_ACTIVITY_KEY_2` | For activity flows BSC day-1 cut | Dedicated Alchemy **Free** app — not `ALCHEMY_FREE_KEY` |
| `ANKR_API_KEY` | For activity flows BSC day-15 cut | Ankr Advanced API |
| `OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE` | For activity flows X Layer | OKX **Data API** HMAC (not Market API) |

## CI defaults (workflows)

| Worker | CONCURRENCY | CLAIM_BATCH_SIZE | CLAIM_STALE_SECONDS | MAX_RUNTIME_SECONDS |
|---|---|---|---|---|
| daily | 20 | 200 | 7200 | 19800 |
| origin | 4 | 50 | 7200 | 19800 |
| monthly | 20 | 200 | 7200 | 19800 |
| cex import | n/a | n/a | n/a | GHA timeout 30m |
| token prices | n/a | n/a | n/a | GHA timeout 360m |
| token contracts discovery | 10 | 50 | 7200 | 19800 |
| token portfolio discovery | 5 | 25 | 7200 | 19800 |
| LP positions discovery | 5 | 25 | 7200 | 19800 |
| activity flows 15d | 1 | 20 | 7200 | 19800 |
| agent URI resolve | 4 | 20 | n/a | 19800 |
| agent URI reprocess | 4 | 20 | n/a | 19800 |
| AI agent classifier | 1 | 20 | n/a | 19800 |
| on-demand backfill | Ethos 3 / satellites 5 | Ethos 10 / satellites 100 | 7200 | 19800 |
| endpoint liveness 15d | 20 (per-host 2) | 40 | 7200 | 19800 |

Daily also sets `WORKER_ID` to `worker-a` or `worker-b`. Origin/monthly set `SKIP_ELIGIBLE_COUNT=1`.

Manual run: **Actions** → pick workflow → **Run workflow**.

## Local development

```powershell
cd workers/<worker_name>
copy .env.example .env
# Set SUPABASE_DB_URL and ALCHEMY_KEY or DUNE_KEY as needed

uv sync
uv run python job.py
```

## Repository layout

```
gsa-workers/
├── AGENTS.md
├── README.md
├── docs/
│   ├── PROCESSES.md
│   ├── TOKEN_CONTRACTS_DISCOVERY_ALCHEMY.md
│   ├── PENDING_LP_POSITIONS.md
│   ├── ARCHITECTURE.md
│   ├── SUPABASE.md
│   ├── OPS.md
│   └── DEPRECATION.md
├── workers/
│   ├── wallet_nonce_balance_daily/
│   │   ├── job.py
│   │   ├── README.md
│   │   ├── pyproject.toml
│   │   └── src/          # db, query, rpc, alchemy, networks, address
│   ├── owner_wallet_origin/
│   │   ├── job.py
│   │   ├── scripts/
│   │   └── src/          # db, origin, ...
│   ├── owner_wallet_nonce_balance_monthly/
│   │   ├── job.py
│   │   └── src/
│   ├── dune_queries_import/
│   │   ├── job.py
│   │   └── src/          # db, dune
│   ├── erc8257_tools_import/
│   │   ├── job.py
│   │   └── src/          # db, agenttoolindex
│   ├── agent_endpoint_liveness/
│   │   ├── job.py
│   │   └── src/          # db, probe, ssrf
│   ├── token_prices_import/
│   │   ├── job.py
│   │   └── src/          # db, dexscreener, coingecko
│   ├── wallet_token_contracts_discovery/
│   │   ├── job.py
│   │   └── src/          # db, alchemy_tokens
│   ├── wallet_token_portfolio_discovery/
│   │   ├── job.py
│   │   └── src/          # db, portfolio_calc, networks
│   ├── wallet_lp_positions_discovery/
│   │   ├── job.py
│   │   └── src/          # db, nft_lp, classic_lp, pricing, univ3_math
│   ├── ai_agent_classifier/
│   │   ├── job.py
│   │   └── src/          # db, llm_client, prompt
│   ├── on_demand_backfill/
│   │   ├── job.py        # orchestrator (Ethos + ERC-8183 + stubs)
│   │   └── src/          # steps/, ethos/, erc8183/, db_common
│   ├── agent_uri_resolve/
│   │   ├── job.py
│   │   └── src/          # db, resolve, handlers, scrape (Playwright)
│   └── agent_uri_reprocess/
│       ├── job.py        # imports resolve stack from sibling via sys.path
│       └── src/          # db (errors + refresh claims)
└── .github/workflows/
    ├── wallet-nonce-balance-daily.yml
    ├── owner-wallet-origin.yml
    ├── owner-wallet-nonce-balance-monthly.yml
    ├── dune-queries-import.yml
    ├── erc8257-tools-import.yml
    ├── token-prices-import.yml
    ├── wallet-token-contracts-discovery.yml
    ├── wallet-token-portfolio-discovery.yml
    ├── wallet-lp-positions-discovery.yml
    ├── ai-agent-classifier.yml
    ├── on-demand-backfill.yml
    ├── agent-uri-resolve.yml
    └── agent-uri-reprocess.yml
```

Schema / snapshot SQL: sibling repo **`gsa-supabase-schema`**.

## Deprecation

See [docs/DEPRECATION.md](./docs/DEPRECATION.md) (Cloudflare/Edge Phase 2 + deprecated pg_cron snapshot jobs).
