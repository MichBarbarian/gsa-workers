# On-demand backfill

Orquestador GHA multi-step: catch-up on-demand para dominios con flag `needs_*`.

## Steps (secuenciales)

| Step | Acción | Empty → |
|------|--------|---------|
| `ethos_history` | `claim_history_fetch` → Goldsky slim (no signal entities) → complete | skip |
| `ethos_scores` | `list_score_candidates` → Ethos API → `upsert_official_scores` | skip |
| `erc8183_satellites` | `claim_satellite_backfill` → Goldsky ERC-8183 → upsert satélites → complete (incluso 0 eventos) | skip |
| `virtual_acp_satellites` | `virtual_acp.claim_satellite_backfill` → Goldsky Virtual ACP → upsert → complete (incluso 0 eventos) | skip |
| `olas_mech_satellites` | `olas_mech.claim_satellite_backfill` → Autonolas (Base/Gnosis) → upsert requests/deliveries → complete (incluso 0 eventos) | skip |

Error en un step: log + **continúa** al siguiente. Presupuesto global `MAX_RUNTIME_SECONDS`.

## Env

Ver `.env.example`. Requerido: `SUPABASE_DB_URL`.

## Local

```bash
cd workers/on_demand_backfill
uv sync
uv run python job.py
```

## Workflow

`.github/workflows/on-demand-backfill.yml` — cron 0/6/12/18 + `workflow_dispatch`.

Reemplaza `ethos-enrich` (deprecated).

Schema claims:
- Ethos history/scores: `20260806010000_ethos_enrich_worker.sql`
- Ethos reviews API (dedicated worker, not this orchestrator): `20260825020000_ethos_reviews_api_worker.sql`
- ERC-8183: `20260807010000_bsc_erc_8183_satellite_backfill_claim.sql`
- Virtual ACP: `20260807140000_virtual_acp_satellite_backfill_claim.sql`
- Olas Mech: `20260807154000_olas_mech_satellite_backfill_claim.sql`

## Docs

- Repo: [docs/PROCESSES.md](../../docs/PROCESSES.md) proceso #13
- Vault: `12 - Github Worker/On Demand Backfill/`
- ADR: `08 - Decisiones/2026-08-06 - Worker on-demand backfill unificado`
- Virtual ACP: `08 - Decisiones/2026-08-07 - Virtual ACP worker backfill satelites on-demand`
- Olas Mech: `08 - Decisiones/2026-08-07 - Olas Mech worker backfill satelites on-demand`
