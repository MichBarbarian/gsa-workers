# AI Agent Classifier

> Project context: [AGENTS.md](../../AGENTS.md) · [Supabase map](../../docs/SUPABASE.md) · [Architecture](../../docs/ARCHITECTURE.md) · [Processes](../../docs/PROCESSES.md)

Claim worker that classifies `web_dashboard.agents` into service categories using OpenAI-compatible LLMs configured in schema `llm` (`process_code=agent-classifier`).

## Eligibility

```sql
does_need_ai_category_process IS TRUE
AND COALESCE(has_ai_category_process_error, false) IS NOT TRUE
```

Another process sets the flag to `TRUE`. This worker sets it `FALSE` on success or error. Sticky errors (`has_ai_category_process_error`) stay off the claim queue until a lazy requeue batch reopens them.

## Pipeline

1. Load active categories from `web_dashboard.agent_ai_categories`
2. Load `llm.process.system_prompt` for `process_code = 'agent-classifier'` (editable in DB)
3. Load active providers/models for that process (no full-error requeue at start)
4. Start **one asyncio worker per provider** (Groq / Cerebras / Gemini / OpenRouter, …); optional filter via `PROVIDERS`
5. Each worker claims agents with `FOR UPDATE SKIP LOCKED` and only uses models from its provider
6. When the clean claim queue is empty: requeue up to `REQUEUE_ERROR_BATCH_SIZE` (default **1000**) sticky errors onto the pending queue, then claim again; if requeue returns 0, exit that provider
7. Fingerprint prompt inputs (`ai_category_input_hash`); if another agent already classified the same inputs, **copy** categories and skip the LLM
8. Else pick a model from that provider with remaining daily capacity (`request_total` / `token_total` vs day caps)
9. Call `{base_url}/chat/completions` with provider params (`temperature`, `max_completion_tokens`, `response_format`); if `llm.models.does_need_thinking_off_parameter` then also `reasoning_effort=none` (plus `clear_thinking=false` only for Cerebras). Cloudflare Workers AI also sends `cf-aig-gateway-id: default`.
10. Upsert `llm.models_requests` (`request_total += 1`, `token_total += usage.total_tokens`) — not incremented on copy
11. On success: write `ai_category_*`, `ai_category_input_hash`, `llm_model_id`, clear error cols, flag `FALSE`
12. On failure: `has_ai_category_process_error=TRUE`, `ai_category_process_error_message`, flag `FALSE`

Allowlist includes quality categories `Invalid Metadata` / `Insufficient Metadata` and `Trading Bots` (Ave/Debot-style clones) vs distinct `Trading`. Prompt rules live in `llm.process.system_prompt`.

Exit `0` when: queue empty, all provider workers hit daily request/token limits (no copies left), or `MAX_RUNTIME_SECONDS` reached.

Rate limits: sliding-window hardcaps use `request_per_minute` and `tokens_per_minute` **per model** (except **NVIDIA**: one shared account bucket via `ProviderRateLimiter`). Daily caps use `request_per_day` and `tokents_per_day` (column name as in DB; `NULL` TPD = request-only). HTTP 429 TPM is retried; 429 TPD or persistent RPM 429 skips that model for the rest of the run (agent stays pending, no `mark_error`). Failed 429s do not increment `models_requests`.

Transient transport failures (`ConnectTimeout`, `ReadTimeout`, `ConnectError`, `ReadError`, `PoolTimeout`, `RemoteProtocolError`) are retried up to `LLM_MAX_ATTEMPTS` (4) with linear backoff instead of failing on the first drop. Connect timeout is short (`LLM_CONNECT_TIMEOUT_SECONDS`, 10s) so an unreachable endpoint fails fast and retries cheaply. Read/total timeout is **120s** (NIM MiniMax/Kimi can exceed 60s).

API keys come from GitHub Secrets / env vars named by `llm.llm_provider.secret` (`GROQ`, `CEREBRAS`, `GEMINI`, `OPEN_ROUTER`, `TOKEN_ROUTER`, `NVIDIA`, `MISTRAL`, `CLOUDFLARE`). Endpoint from `llm.llm_provider.base_url`.

**NVIDIA NIM (same key):** overflow slugs `minimaxai/minimax-m3` and `moonshotai/kimi-k3` (Nemotron nano disabled 2026-09-05 after HTTP 410 Gone). Account RPM ~40 is **shared across all slugs** — worker paces at `NVIDIA_ACCOUNT_RPM` (default 30) with `NVIDIA_CONCURRENCY=1`.

## Environment

| Variable | Default | Description |
|---|---|---|
| `SUPABASE_DB_URL` | required | Postgres connection string |
| `GROQ` | required (for Groq) | Groq API key (`llm.llm_provider.secret`) |
| `CEREBRAS` | required (for Cerebras) | Cerebras API key (`llm.llm_provider.secret`) |
| `GEMINI` | required (for Gemini) | Gemini API key (`llm.llm_provider.secret`) |
| `OPEN_ROUTER` | required (for OpenRouter) | OpenRouter API key (`llm.llm_provider.secret`) |
| `TOKEN_ROUTER` | required (for TokenRouter) | TokenRouter API key (provider currently off) |
| `NVIDIA` | required (for NVIDIA NIM) | Hosted NIM key from build.nvidia.com (`llm.llm_provider.secret`) |
| `MISTRAL` | required (for Mistral) | La Plateforme / AI Studio key (`llm.llm_provider.secret`) |
| `CLOUDFLARE` | required (for Cloudflare) | Workers AI API token (`llm.llm_provider.secret`); Account ID lives in `base_url` |
| `CLAIM_BATCH_SIZE` | 20 | Agents claimed per loop **per provider worker** |
| `REQUEUE_ERROR_BATCH_SIZE` | 1000 | Sticky errors reopened per lazy requeue when clean queue is empty (max 5000) |
| `CONCURRENCY` | 1 (local) / **2 in GHA** | Parallel LLM calls **per provider** (max 5; keep low for rpm) |
| `NVIDIA_ACCOUNT_RPM` | 30 | Shared account RPM for provider NVIDIA (all NIM slugs) |
| `NVIDIA_CONCURRENCY` | 1 | Parallel LLM calls **only** for NVIDIA lane |
| `PROVIDERS` | all active | Optional comma filter of `llm.llm_provider.name` (e.g. `Groq,GEMINI`) |
| `MAX_RUNTIME_SECONDS` | 19800 | Soft stop (~5.5h), shared across provider workers |

## Monitoring

```sql
SELECT
  count(*) FILTER (WHERE does_need_ai_category_process IS TRUE) AS pending,
  count(*) FILTER (WHERE has_ai_category_process_error IS TRUE) AS errors,
  count(*) FILTER (WHERE ai_category_primary IS NOT NULL) AS classified
FROM web_dashboard.agents;

SELECT m.name, mr.date, mr.request_total, mr.token_total,
       m.request_per_day, m.tokents_per_day
FROM llm.models_requests mr
JOIN llm.models m ON m.id = mr.model_id
WHERE mr.date = CURRENT_DATE
ORDER BY m.id;
```

## Local run

```bash
cd workers/ai_agent_classifier
uv sync
uv run python job.py
```

After deploying `ai_category_input_hash`, backfill classified donors once:

```bash
uv run python backfill_input_hash.py
```

## Workflow

`.github/workflows/ai-agent-classifier.yml` — cron `0 0,6,12,18 * * *` UTC + `workflow_dispatch`.
