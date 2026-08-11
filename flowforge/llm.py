"""The LLM node and its providers.

The node knows how to build a prompt, stream the answer out as ``node.delta``
events, and hand back the assembled text. It knows nothing about any particular
vendor — that lives behind :class:`LLMProvider`, so the offline ``echo``
provider and the real Anthropic one are interchangeable in tests and in prod.

Timeouts and retries are *not* handled here: they are the node spec's ``timeout``
and ``retries`` fields, applied by the engine to every node type alike.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Callable, Mapping, Protocol, runtime_checkable

from .errors import FlowForgeError
from .events import NODE_DELTA
from .nodes import Node, NodeContext, registry

DEFAULT_MODEL = "claude-opus-5"
# Thinking is on by default on Claude Opus 5, and max_tokens caps thinking plus
# response text together — so this is deliberately generous.
DEFAULT_MAX_TOKENS = 16000


class LLMError(FlowForgeError):
    """The LLM node is misconfigured, or its provider is unavailable."""


@runtime_checkable
class LLMProvider(Protocol):
    """Streams a completion, one text chunk at a time."""

    def stream(
        self,
        prompt: str,
        *,
        model: str,
        max_tokens: int,
        system: str | None = None,
    ) -> AsyncIterator[str]: ...


_PROVIDERS: dict[str, Callable[..., LLMProvider]] = {}


def register_provider(name: str, factory: Callable[..., LLMProvider]) -> None:
    if name in _PROVIDERS:
        raise ValueError(f"provider {name!r} already registered")
    _PROVIDERS[name] = factory


def build_provider(name: str, options: Mapping[str, Any] | None = None) -> LLMProvider:
    try:
        factory = _PROVIDERS[name]
    except KeyError:
        raise LLMError(
            f"unknown LLM provider {name!r}; registered: "
            f"{', '.join(sorted(_PROVIDERS)) or '<none>'}"
        ) from None
    return factory(**dict(options or {}))


def known_providers() -> list[str]:
    return sorted(_PROVIDERS)


class EchoProvider:
    """Deterministic offline provider: streams the prompt back in chunks.

    Exists so the streaming path, the retry path and the benchmarks can all run
    with no API key and no network.
    """

    def __init__(self, delay: float = 0.0, words_per_chunk: int = 3) -> None:
        self.delay = float(delay)
        self.words_per_chunk = max(1, int(words_per_chunk))

    async def stream(
        self,
        prompt: str,
        *,
        model: str,
        max_tokens: int,
        system: str | None = None,
    ) -> AsyncIterator[str]:
        words = prompt.split()
        for start in range(0, len(words), self.words_per_chunk):
            if self.delay:
                await asyncio.sleep(self.delay)
            chunk = " ".join(words[start : start + self.words_per_chunk])
            yield chunk if start == 0 else f" {chunk}"


class AnthropicProvider:
    """Streams from the Claude Messages API via the official SDK.

    The SDK's own retry loop is disabled (``max_retries=0``): a node's
    ``timeout`` / ``retries`` are the single place attempts are counted, so the
    two don't multiply into a wall-clock surprise.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any = None,
    ) -> None:
        self._client = client
        self._api_key = api_key
        self._base_url = base_url

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from anthropic import AsyncAnthropic
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise LLMError(
                "the 'anthropic' package is required for the anthropic provider: "
                "pip install anthropic"
            ) from exc
        options: dict[str, Any] = {"max_retries": 0}
        if self._api_key:
            options["api_key"] = self._api_key
        if self._base_url:
            options["base_url"] = self._base_url
        self._client = AsyncAnthropic(**options)
        return self._client

    async def stream(
        self,
        prompt: str,
        *,
        model: str,
        max_tokens: int,
        system: str | None = None,
    ) -> AsyncIterator[str]:
        client = self._ensure_client()
        request: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            request["system"] = system
        async with client.messages.stream(**request) as stream:
            async for text in stream.text_stream:
                yield text


register_provider("echo", EchoProvider)
register_provider("anthropic", AnthropicProvider)


@registry.register("llm")
class LLMNode(Node):
    """Calls a model and streams the answer out as it arrives.

    config::

        {
          "provider": "anthropic",          # or "echo" (default)
          "model": "claude-opus-5",
          "max_tokens": 16000,
          "system": "You are terse.",
          "prompt": "Summarise: {{fetch.text}}",
          "provider_options": {"api_key": "..."}
        }

    ``prompt`` and ``system`` are resolved against the variable pool before the
    node runs, so upstream outputs are already substituted in.
    """

    async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
        prompt = ctx.config.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise LLMError(f"node {ctx.node_id!r}: 'prompt' must be a non-empty string")

        provider_name = str(ctx.config.get("provider", "echo"))
        provider = build_provider(provider_name, ctx.config.get("provider_options"))
        model = str(ctx.config.get("model", DEFAULT_MODEL))
        max_tokens = int(ctx.config.get("max_tokens", DEFAULT_MAX_TOKENS))
        system = ctx.config.get("system")

        chunks: list[str] = []
        async for chunk in provider.stream(
            prompt,
            model=model,
            max_tokens=max_tokens,
            system=None if system is None else str(system),
        ):
            chunks.append(chunk)
            await ctx.emit(NODE_DELTA, text=chunk, index=len(chunks) - 1)

        return {
            "text": "".join(chunks),
            "chunks": len(chunks),
            "model": model,
            "provider": provider_name,
        }
