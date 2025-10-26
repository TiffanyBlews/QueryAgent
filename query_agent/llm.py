"""
Minimal client for calling an OpenAI-compatible chat completion endpoint.

Enhanced retry mechanism:
- LLM_MAX_RETRIES: Number of retry attempts (default: 7)
- Timeout retries use progressive backoff: 10s, 20s, 30s, 45s, 60s, 60s, 60s
- Only timeout errors are retried; other errors fail immediately

Error handling (configured in agent.py):
- FALLBACK_TO_TEMPLATE: "1" to fallback to template on LLM failure, "0" to fail (default: 0)
"""

from __future__ import annotations

import json
import os
import time
import logging
from typing import Any, Dict, List, Mapping, Optional

import requests

logger = logging.getLogger(__name__)


class LLMError(RuntimeError):
    """Raised when the LLM call fails."""


class OpenAIChatClient:
    """
    Lightweight wrapper around the OpenAI Chat Completions API.
    """

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        default_temperature: float = 0.3,
        request_timeout: Optional[float] = None,
    ) -> None:
        self.base_url = base_url or os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.model = model or os.environ.get("MODEL")
        self.default_temperature = default_temperature
        timeout_env = os.environ.get("OPENAI_TIMEOUT")
        self.request_timeout = (
            request_timeout
            if request_timeout is not None
            else (float(timeout_env) if timeout_env else 400.0)
        )

        if not self.api_key:
            raise LLMError("OPENAI_API_KEY is not set.")
        if not self.model:
            raise LLMError("MODEL is not specified.")

        self._endpoint = self.base_url.rstrip("/") + "/chat/completions"

    def create_chat_completion(
        self,
        messages: List[Mapping[str, str]],
        *,
        temperature: Optional[float] = None,
        response_format: Optional[Dict[str, Any]] = None,
        seed: Optional[int] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [dict(message) for message in messages],
            "temperature": temperature if temperature is not None else self.default_temperature,
        }
        if response_format:
            payload["response_format"] = response_format
        if seed is not None:
            payload["seed"] = seed
        payload.update(extra)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Retry mechanism for timeout errors
        max_retries = int(os.environ.get("LLM_MAX_RETRIES", "7"))  # 增加到7次重试
        last_error: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                response = requests.post(self._endpoint, headers=headers, json=payload, timeout=self.request_timeout)
                response.raise_for_status()

                data = response.json()
                if not data.get("choices"):
                    raise LLMError("No choices returned from LLM.")
                return data

            except requests.exceptions.Timeout as exc:
                last_error = exc
                if attempt < max_retries - 1:  # Don't sleep after the last attempt
                    # 指数退避，但有最大限制：10s, 20s, 30s, 45s, 60s, 60s
                    sleep_time = min(10 * (attempt + 1), 60)
                    logger.warning(
                        "LLM request timed out on attempt %d/%d, retrying in %ds: %s",
                        attempt + 1, max_retries, sleep_time, exc
                    )
                    time.sleep(sleep_time)
                    continue
                else:
                    raise LLMError(f"LLM request timed out after {max_retries} attempts: {exc}") from exc
            except requests.RequestException as exc:  # noqa: BLE001
                raise LLMError(f"LLM request failed: {exc}") from exc

        # This should never be reached, but just in case
        raise LLMError(f"LLM request failed after {max_retries} attempts: {last_error}")

    def run_json_completion(
        self,
        messages: List[Mapping[str, str]],
        *,
        temperature: Optional[float] = None,
        seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Request a JSON-formatted response and parse it.
        """
        data = self.create_chat_completion(
            messages,
            temperature=temperature,
            response_format={"type": "json_object"},
            seed=seed,
        )
        content = data["choices"][0]["message"]["content"]
        try:
            return json.loads(content)
        except Exception as exc:  # noqa: BLE001
            raise LLMError(f"Failed to parse JSON response: {exc}\nRaw content: {content}") from exc
