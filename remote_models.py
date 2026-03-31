"""
Shared remote model routing for BIZ.

Goal: keep core workflows off local inference and prefer env-configured
remote providers with simple fallback logic.
"""
from __future__ import annotations

import os
import time
from typing import Any

import requests
from openai import OpenAI

OPENROUTER_MODELS = [
    "stepfun/step-3.5-flash:free",
    "minimax/minimax-m2.5:free",
    "qwen/qwen3-next-80b-a3b-instruct:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "openai/gpt-oss-120b:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "google/gemma-3-27b-it:free",
]

GROQ_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
]

GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-2.5-flash",
]

_CLIENTS: dict[tuple[str, str], OpenAI] = {}
_MODEL_BACKOFF_UNTIL: dict[str, float] = {}
_MODEL_FAILURE_COUNTS: dict[str, int] = {}


def _openai_client(base_url: str, api_key: str) -> OpenAI:
    key = (base_url, api_key)
    client = _CLIENTS.get(key)
    if client is None:
        client = OpenAI(base_url=base_url, api_key=api_key, timeout=30.0)
        _CLIENTS[key] = client
    return client


def _attempts_for_chat() -> list[tuple[str, str, OpenAI]]:
    attempts: list[tuple[str, str, OpenAI]] = []
    openrouter_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    groq_key = os.environ.get("GROQ_API_KEY", "").strip()

    if openrouter_key:
        client = _openai_client("https://openrouter.ai/api/v1", openrouter_key)
        attempts.extend(("openrouter", model, client) for model in OPENROUTER_MODELS)
    if groq_key:
        client = _openai_client("https://api.groq.com/openai/v1", groq_key)
        attempts.extend(("groq", model, client) for model in GROQ_MODELS)
    return attempts


def _note_failure(model_key: str) -> None:
    now = time.time()
    failures = _MODEL_FAILURE_COUNTS.get(model_key, 0) + 1
    _MODEL_FAILURE_COUNTS[model_key] = failures
    _MODEL_BACKOFF_UNTIL[model_key] = now + min(120, 15 * failures)


def _backoff_active(model_key: str) -> bool:
    return _MODEL_BACKOFF_UNTIL.get(model_key, 0) > time.time()


def create_chat_completion(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.3,
    max_tokens: int = 2000,
) -> Any:
    """
    Try env-backed OpenAI-compatible providers in order.
    Returns the native provider SDK response object.
    """
    kwargs: dict[str, Any] = {
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"

    last_err: Exception | None = None
    for provider, model, client in _attempts_for_chat():
        model_key = f"{provider}:{model}"
        if _backoff_active(model_key):
            continue
        try:
            return client.chat.completions.create(model=model, **kwargs)
        except Exception as err:  # provider SDK exception types vary over time
            last_err = err
            _note_failure(model_key)
            time.sleep(0.4)
            continue

    raise RuntimeError(
        "All remote chat providers failed. "
        f"Checked OpenRouter/Groq env-backed models. Last error: {last_err}"
    )


def complete_text(
    *,
    system_prompt: str | None = None,
    user_prompt: str,
    temperature: float = 0.2,
    max_tokens: int = 600,
) -> str:
    """
    Text-only completion with Gemini REST fallback for non-tool tasks.
    """
    messages: list[dict[str, Any]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": user_prompt})

    try:
        response = create_chat_completion(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        content = response.choices[0].message.content
        if isinstance(content, str):
            return content.strip()
    except Exception:
        pass

    gemini_key = (
        os.environ.get("GOOGLE_AI_STUDIO_API_KEY", "").strip()
        or os.environ.get("GOOGLE_AI_VICTOR_API_KEY", "").strip()
    )
    if not gemini_key:
        raise RuntimeError("No remote text provider available")

    last_err: Exception | None = None
    for model in GEMINI_MODELS:
        try:
            payload: dict[str, Any] = {
                "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
                "generationConfig": {
                    "temperature": temperature,
                    "maxOutputTokens": max_tokens,
                },
            }
            if system_prompt:
                payload["systemInstruction"] = {"parts": [{"text": system_prompt}]}
            response = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                params={"key": gemini_key},
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            parts = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [])
            )
            text = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
            if text:
                return text
        except Exception as err:
            last_err = err
            continue

    raise RuntimeError(f"All remote text providers failed. Last error: {last_err}")


def provider_status_lines() -> list[str]:
    return [
        f"openrouter={'yes' if os.environ.get('OPENROUTER_API_KEY') else 'no'}",
        f"groq={'yes' if os.environ.get('GROQ_API_KEY') else 'no'}",
        f"gemini={'yes' if (os.environ.get('GOOGLE_AI_STUDIO_API_KEY') or os.environ.get('GOOGLE_AI_VICTOR_API_KEY')) else 'no'}",
    ]
