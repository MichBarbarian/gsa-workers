"""OpenAI-compatible chat completions client (Groq / Cerebras / Gemini / Cloudflare)."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger("ai_agent_classifier")


def _normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def _parse_response_format(raw: str | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("provider response_format must be a JSON object")
    return parsed


async def chat_completion(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    api_key: str,
    model_slug: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float | None,
    max_completion_tokens: int | None,
    response_format: str | None,
    thinking_off: bool = False,
    timeout_seconds: float = 120.0,
) -> tuple[str, int]:
    """Return (content, total_tokens). total_tokens is 0 if usage missing."""
    url = f"{_normalize_base_url(base_url)}/chat/completions"
    body: dict[str, Any] = {
        "model": model_slug,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    if temperature is not None:
        body["temperature"] = float(temperature)
    if max_completion_tokens is not None:
        body["max_tokens"] = int(max_completion_tokens)
    fmt = _parse_response_format(response_format)
    if fmt is not None:
        body["response_format"] = fmt
    if thinking_off:
        # Disable reasoning/thinking tokens. Groq (Qwen 3.6) accepts reasoning_effort
        # only; Cerebras GLM also wants clear_thinking=false.
        body["reasoning_effort"] = "none"
        if "cerebras" in _normalize_base_url(base_url).lower():
            body["clear_thinking"] = False

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Workers AI OpenAI-compat requires a gateway id on hosted @cf/ models.
    if "api.cloudflare.com" in _normalize_base_url(base_url).lower():
        headers["cf-aig-gateway-id"] = "default"
    resp = await client.post(url, headers=headers, json=body, timeout=timeout_seconds)
    if resp.status_code >= 400:
        snippet = (resp.text or "")[:500]
        raise RuntimeError(f"LLM HTTP {resp.status_code}: {snippet}")

    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected LLM response shape: {data!r}") from exc
    if content is None or not str(content).strip():
        raise RuntimeError("LLM returned empty content")

    total_tokens = 0
    usage = data.get("usage")
    if isinstance(usage, dict) and usage.get("total_tokens") is not None:
        try:
            total_tokens = int(usage["total_tokens"])
        except (TypeError, ValueError):
            total_tokens = 0

    return str(content), max(total_tokens, 0)
