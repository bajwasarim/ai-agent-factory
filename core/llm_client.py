"""LLM client wrapper with provider abstraction."""

import os
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI, APIError, RateLimitError, APITimeoutError

from core.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


class BaseLLMClient(ABC):
    """Abstract base for LLM clients to enable provider swapping."""

    @abstractmethod
    def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 512,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate a response from the LLM."""
        pass


class OpenAIClient(BaseLLMClient):
    """OpenAI-compatible API client implementation."""

    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_RETRY_DELAY = 1.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        default_model: str = "gpt-4o-mini",
        base_url: Optional[str] = None,
        timeout: int = 60,
        max_retries: int = DEFAULT_RETRY_ATTEMPTS,
    ) -> None:
        """
        Initialize the OpenAI-compatible client.

        Args:
            api_key: API key. Falls back to LLM_API_KEY or OPENAI_API_KEY env var.
            default_model: Default model to use for generation.
            base_url: Optional base URL for API (Copilot/Azure/local gateways).
            timeout: Request timeout in seconds.
            max_retries: Max retry attempts for rate limit errors.
        """
        self.api_key = api_key or os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.default_model = default_model
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: Optional[OpenAI] = None
        self._usage_log: List[Dict[str, Any]] = []

    @property
    def client(self) -> OpenAI:
        """Lazy initialization of OpenAI client."""
        if self._client is None:
            if not self.api_key:
                raise ValueError("LLM_API_KEY or OPENAI_API_KEY not set in environment")
            client_kwargs: Dict[str, Any] = {
                "api_key": self.api_key,
                "timeout": self.timeout,
            }
            if self.base_url:
                client_kwargs["base_url"] = self.base_url
            self._client = OpenAI(**client_kwargs)
        return self._client

    def _retry_with_backoff(self, func: Callable[[], Any]) -> Any:
        """Execute function with exponential backoff on rate limit errors."""
        last_exception: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                return func()
            except RateLimitError as e:
                last_exception = e
                delay = self.DEFAULT_RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Rate limited. Retrying in {delay}s (attempt {attempt + 1}/{self.max_retries})")
                time.sleep(delay)
            except APITimeoutError as e:
                last_exception = e
                delay = self.DEFAULT_RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Timeout. Retrying in {delay}s (attempt {attempt + 1}/{self.max_retries})")
                time.sleep(delay)
        raise RuntimeError(f"LLM generation failed after {self.max_retries} attempts: {last_exception}")

    def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 512,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Generate a response from OpenAI-compatible API.

        Args:
            prompt: The input prompt.
            model: Model to use. Defaults to configured default_model.
            temperature: Sampling randomness (0.0-2.0).
            max_tokens: Maximum tokens in response (cost control).
            metadata: Optional tracing metadata (logged, not sent to provider).

        Returns:
            Generated text response.
        """
        model = model or self.default_model
        logger.debug(f"Generating with model={model}, prompt_length={len(prompt)}, metadata={metadata}")

        def _call() -> str:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful AI assistant."},
                    {"role": "user", "content": prompt},
                ],
                temperature=temperature,
                max_tokens=max_tokens,
            )
            content = response.choices[0].message.content or ""
            
            # Log usage for cost tracking
            usage_entry = {
                "model": model,
                "prompt_tokens": getattr(response.usage, "prompt_tokens", 0),
                "completion_tokens": getattr(response.usage, "completion_tokens", 0),
                "total_tokens": getattr(response.usage, "total_tokens", 0),
                "metadata": metadata,
            }
            self._usage_log.append(usage_entry)
            logger.debug(f"Usage: {usage_entry}")
            
            return content.strip()

        try:
            return self._retry_with_backoff(_call)
        except APIError as e:
            raise RuntimeError(f"LLM generation failed: {e}")

    def get_usage_log(self) -> List[Dict[str, Any]]:
        """Return logged token usage for cost analysis."""
        return self._usage_log.copy()

    def clear_usage_log(self) -> None:
        """Clear the usage log."""
        self._usage_log.clear()


class LLMClient:
    """
    Provider-agnostic LLM client facade.

    Default provider: OpenAI-compatible API (OpenAI / Copilot / Azure / local gateways).
    Swap provider by passing a different BaseLLMClient implementation.
    """

    def __init__(self, provider: Optional[BaseLLMClient] = None) -> None:
        """
        Initialize the LLM client.

        Args:
            provider: LLM provider implementation. Defaults to OpenAIClient.
        """
        self._provider = provider or OpenAIClient()

    @classmethod
    def from_env(cls) -> "LLMClient":
        """
        Factory constructor using environment variables.

        Required:
            - LLM_API_KEY (or OPENAI_API_KEY as fallback)
            - LLM_MODEL (or defaults to gpt-4o-mini)

        Optional:
            - LLM_BASE_URL: For Copilot / Azure / local gateways
            - LLM_TIMEOUT: Request timeout in seconds (default: 60)
            - LLM_MAX_RETRIES: Max retry attempts (default: 3)

        Returns:
            Configured LLMClient instance.
        """
        api_key = os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY")
        model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        base_url = os.getenv("LLM_BASE_URL")
        timeout = int(os.getenv("LLM_TIMEOUT", "60"))
        max_retries = int(os.getenv("LLM_MAX_RETRIES", "3"))

        if not api_key:
            raise ValueError("LLM_API_KEY or OPENAI_API_KEY not set in environment")

        provider = OpenAIClient(
            api_key=api_key,
            default_model=model,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
        )
        return cls(provider=provider)

    def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 512,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Generate a response using the configured provider.

        Args:
            prompt: The input prompt.
            model: Optional model override.
            temperature: Sampling randomness (0.0-2.0).
            max_tokens: Maximum tokens in response (cost control).
            metadata: Optional tracing metadata (e.g., {"project": "leadgen"}).

        Returns:
            Generated text response.
        """
        return self._provider.generate(
            prompt=prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            metadata=metadata,
        )

    def get_usage_log(self) -> List[Dict[str, Any]]:
        """Return usage log if provider supports it."""
        if hasattr(self._provider, "get_usage_log"):
            return self._provider.get_usage_log()
        return []

    def clear_usage_log(self) -> None:
        """Clear usage log if provider supports it."""
        if hasattr(self._provider, "clear_usage_log"):
            self._provider.clear_usage_log()
