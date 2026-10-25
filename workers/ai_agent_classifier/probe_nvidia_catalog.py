#!/usr/bin/env python3
"""Re-probe NVIDIA NIM catalog for agent-classifier stable slugs.

  NVIDIA=... uv run python probe_nvidia_catalog.py

Prints JSON: catalog intersection + smoke chat results. No DB writes.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import httpx

BASE = "https://integrate.api.nvidia.com/v1"
CANDIDATES = [
    "minimaxai/minimax-m3.1-flash",
    "moonshotai/kimi-k3",
    "moonshotai/kimi-k2.6",
    "openai/gpt-oss-20b",
    "google/gemma-4-31b-it",
    "deepseek-ai/deepseek-v4.1-flash",
    "nvidia/nemotron-3.5-lightning-30b-a3b",
    "nvidia/nemotron-3-ultra-550b-a55b",
    "z-ai/glm-5.3",
    "z-ai/glm-5.3-flash",
]

SMOKE_BODY = {
    "model": "",
    "messages": [
        {
            "role": "system",
            "content": "Reply with a single JSON object only.",
        },
        {
            "role": "user",
            "content": (
                'Classify this agent. Return JSON: '
                '{"primary_category":"Other / Niche","confidence":0.5,'
                '"reasoning":"smoke","agent_purpose":"smoke test"}'
            ),
        },
    ],
    "temperature": 0.2,
    "max_tokens": 200,
    "response_format": {"type": "json_object"},
}


def main() -> int:
    key = os.environ.get("NVIDIA")
    if not key:
        print("Missing NVIDIA env", file=sys.stderr)
        return 1

    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    out: dict[str, Any] = {"base": BASE, "candidates": CANDIDATES}

    with httpx.Client(timeout=httpx.Timeout(120.0, connect=15.0)) as client:
        r = client.get(f"{BASE}/models", headers=headers)
        out["list_status"] = r.status_code
        catalog_ids: set[str] = set()
        if r.status_code == 200:
            data = r.json()
            for item in data.get("data") or []:
                mid = item.get("id")
                if isinstance(mid, str):
                    catalog_ids.add(mid)
            out["catalog_count"] = len(catalog_ids)
            out["in_catalog"] = sorted(c for c in CANDIDATES if c in catalog_ids)
            out["missing_from_catalog"] = sorted(
                c for c in CANDIDATES if c not in catalog_ids
            )
        else:
            out["list_error"] = (r.text or "")[:400]
            out["in_catalog"] = []
            out["missing_from_catalog"] = list(CANDIDATES)

        smoke: list[dict[str, Any]] = []
        for slug in CANDIDATES:
            if catalog_ids and slug not in catalog_ids:
                smoke.append(
                    {
                        "slug": slug,
                        "skipped": True,
                        "reason": "not_in_catalog",
                    }
                )
                continue
            body = dict(SMOKE_BODY)
            body["model"] = slug
            # Prefer disabling thinking when supported (ignored if unsupported).
            body["thinking"] = {"type": "disabled"}
            body["reasoning_effort"] = "none"
            t0 = time.perf_counter()
            try:
                resp = client.post(
                    f"{BASE}/chat/completions",
                    headers=headers,
                    json=body,
                )
                elapsed = round(time.perf_counter() - t0, 2)
                snippet = (resp.text or "")[:300]
                ok_json = False
                content = None
                if resp.status_code < 400:
                    try:
                        payload = resp.json()
                        content = (
                            payload.get("choices", [{}])[0]
                            .get("message", {})
                            .get("content")
                        )
                        if content:
                            json.loads(content)
                            ok_json = True
                    except Exception:
                        ok_json = False
                smoke.append(
                    {
                        "slug": slug,
                        "status": resp.status_code,
                        "elapsed_s": elapsed,
                        "ok_json": ok_json,
                        "snippet": snippet if resp.status_code >= 400 else None,
                        "content_preview": (
                            None if content is None else str(content)[:160]
                        ),
                    }
                )
            except Exception as exc:
                smoke.append(
                    {
                        "slug": slug,
                        "status": "error",
                        "elapsed_s": round(time.perf_counter() - t0, 2),
                        "ok_json": False,
                        "error": f"{exc.__class__.__name__}: {exc}",
                    }
                )
        out["smoke"] = smoke

        keep = [
            s["slug"]
            for s in smoke
            if s.get("status") == 200 and s.get("ok_json")
        ]
        # Prefer small/flash/20b order from plan
        preferred = [
            "openai/gpt-oss-20b",
            "minimaxai/minimax-m3.1-flash",
            "deepseek-ai/deepseek-v4.1-flash",
            "nvidia/nemotron-3.5-lightning-30b-a3b",
            "google/gemma-4-31b-it",
        ]
        recommended: list[str] = []
        for p in preferred:
            if p in keep and p not in recommended:
                recommended.append(p)
            if len(recommended) >= 3:
                break
        for k in keep:
            if k not in recommended and len(recommended) < 3:
                if "ultra" in k or "super" in k or "kimi-k3" in k:
                    continue
                recommended.append(k)
        out["recommended_keep"] = recommended
        out["smoke_ok"] = keep

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
