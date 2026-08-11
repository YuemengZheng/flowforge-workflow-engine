"""Adapters for external model APIs, on the existing ``LLMProvider`` protocol.

Eleven vendors, but nowhere near eleven implementations — that is the point of
the layer. Most of these APIs *are* OpenAI's: same ``/chat/completions`` request,
same ``data: {...}`` SSE frames, different host and auth header. So one wire
implementation is configured by a :class:`Profile`, and only the three that
genuinely differ get code:

* **OpenAI-compatible** (openai, azure, ollama, together, groq, fireworks,
  deepseek, mistral, and ``openai_compatible`` for anything else with that shape)
  — one class, eight profiles. Azure overrides the URL, since it puts the
  deployment in the path and the key in ``api-key``.
* **Vertex** — Gemini's ``streamGenerateContent``: different body, different
  frames, and a bearer token the caller supplies (minting one needs RSA signing,
  which is not in the standard library).
* **Bedrock** — neither. SigV4 request signing and a binary event-stream
  response, both implemented here.

Every adapter streams through the pooled HTTP client in ``pool.py``, so a wide
wave of LLM nodes does not open a connection each.

**Verification status is in the README and it matters:** these are tested against
an in-process server that speaks each vendor's wire format back at them. None has
ever been pointed at the live API, because that needs keys. What is verified is
the request shape and the stream parsing; what is not is any vendor's actual
behaviour.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import struct
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Iterator, Mapping
from urllib.parse import quote, urlsplit

from .llm import LLMError, register_provider
from .pool import HTTPConnectionPool

# Shared by every adapter unless one is passed in, so the process keeps one pool
# per origin rather than one per node.
_SHARED_POOL: HTTPConnectionPool | None = None


def shared_pool() -> HTTPConnectionPool:
    global _SHARED_POOL
    if _SHARED_POOL is None:
        _SHARED_POOL = HTTPConnectionPool()
    return _SHARED_POOL


async def close_shared_pool() -> None:
    """Release pooled sockets. For a clean shutdown and for tests."""
    global _SHARED_POOL
    if _SHARED_POOL is not None:
        await _SHARED_POOL.close()
        _SHARED_POOL = None


@dataclass(frozen=True)
class Profile:
    """Everything that differs between two APIs of the same shape."""

    name: str
    base_url: str = ""
    path: str = "/chat/completions"
    query: str = ""
    auth_header: str = "Authorization"
    auth_prefix: str = "Bearer "
    key_env: tuple[str, ...] = ()
    default_model: str = ""
    needs_key: bool = True
    needs_base_url: bool = False
    extra_headers: Mapping[str, str] = field(default_factory=dict)


#: The OpenAI-shaped APIs. Base URLs are the vendors' documented v1 endpoints;
#: default models are a cheap, current choice per vendor, overridable per node.
OPENAI_PROFILES: dict[str, Profile] = {
    "openai": Profile(
        "openai",
        base_url="https://api.openai.com/v1",
        key_env=("OPENAI_API_KEY",),
        default_model="gpt-4o-mini",
    ),
    "azure": Profile(
        "azure",
        path="/openai/deployments/{model}/chat/completions",
        query="api-version=2024-10-21",
        auth_header="api-key",
        auth_prefix="",
        key_env=("AZURE_OPENAI_API_KEY",),
        needs_base_url=True,
    ),
    "ollama": Profile(
        "ollama",
        base_url="http://127.0.0.1:11434/v1",
        key_env=(),
        default_model="llama3.1",
        needs_key=False,
    ),
    "together": Profile(
        "together",
        base_url="https://api.together.xyz/v1",
        key_env=("TOGETHER_API_KEY",),
        default_model="meta-llama/Llama-3.3-70B-Instruct-Turbo",
    ),
    "groq": Profile(
        "groq",
        base_url="https://api.groq.com/openai/v1",
        key_env=("GROQ_API_KEY",),
        default_model="llama-3.3-70b-versatile",
    ),
    "fireworks": Profile(
        "fireworks",
        base_url="https://api.fireworks.ai/inference/v1",
        key_env=("FIREWORKS_API_KEY",),
        default_model="accounts/fireworks/models/llama-v3p3-70b-instruct",
    ),
    "deepseek": Profile(
        "deepseek",
        base_url="https://api.deepseek.com/v1",
        key_env=("DEEPSEEK_API_KEY",),
        default_model="deepseek-chat",
    ),
    "mistral": Profile(
        "mistral",
        base_url="https://api.mistral.ai/v1",
        key_env=("MISTRAL_API_KEY",),
        default_model="mistral-large-latest",
    ),
    "openai_compatible": Profile(
        "openai_compatible",
        key_env=("LLM_API_KEY",),
        needs_base_url=True,
        needs_key=False,
    ),
}


class HTTPProvider:
    """Shared plumbing: key resolution, the POST, and the streamed body."""

    wire = ""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        pool: HTTPConnectionPool | None = None,
        timeout_s: float | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = (base_url or "").rstrip("/")
        self._pool = pool
        self.timeout_s = timeout_s
        self._extra_headers = dict(extra_headers or {})

    @property
    def pool(self) -> HTTPConnectionPool:
        return self._pool if self._pool is not None else shared_pool()

    # --------------------------------------------------------------- overrides

    def url(self, model: str) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def headers(self) -> dict[str, str]:  # pragma: no cover - abstract
        raise NotImplementedError

    def body(
        self, prompt: str, model: str, max_tokens: int, system: str | None
    ) -> dict[str, Any]:  # pragma: no cover - abstract
        raise NotImplementedError

    def deltas(self, chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
        """Turn response bytes into text pieces."""  # pragma: no cover
        raise NotImplementedError

    def resolve_model(self, model: str) -> str:
        return model

    # ------------------------------------------------------------------ stream

    async def stream(
        self,
        prompt: str,
        *,
        model: str,
        max_tokens: int,
        system: str | None = None,
    ) -> AsyncIterator[str]:
        resolved = self.resolve_model(model)
        chunks = self.pool.stream_body(
            self.url(resolved),
            self.body(prompt, resolved, max_tokens, system),
            self.headers(),
            timeout_s=self.timeout_s,
        )
        async for text in self.deltas(chunks):
            if text:
                yield text

    def key(self, env_names: tuple[str, ...], required: bool, label: str) -> str:
        if self._api_key:
            return self._api_key
        for name in env_names:
            value = os.environ.get(name)
            if value:
                return value
        if required:
            raise LLMError(
                f"{label}: no API key. Pass provider_options.api_key or set "
                f"{' or '.join(env_names) or '<no env var defined>'}"
            )
        return ""


class OpenAICompatibleProvider(HTTPProvider):
    """``POST /chat/completions`` with ``stream: true`` and OpenAI SSE frames."""

    wire = "openai"

    def __init__(self, profile: Profile = OPENAI_PROFILES["openai"], **kwargs: Any):
        super().__init__(**kwargs)
        self.profile = profile
        if profile.needs_base_url and not self._base_url:
            raise LLMError(
                f"provider {profile.name!r} needs provider_options.base_url "
                f"(it has no fixed endpoint)"
            )

    def resolve_model(self, model: str) -> str:
        # An explicit model always wins; the profile default is for convenience.
        return model or self.profile.default_model

    def url(self, model: str) -> str:
        base = self._base_url or self.profile.base_url
        path = self.profile.path.replace("{model}", quote(model, safe=""))
        url = f"{base}{path}"
        return f"{url}?{self.profile.query}" if self.profile.query else url

    def headers(self) -> dict[str, str]:
        key = self.key(
            self.profile.key_env, self.profile.needs_key, f"provider {self.profile.name!r}"
        )
        headers = {**dict(self.profile.extra_headers), **self._extra_headers}
        if key:
            headers[self.profile.auth_header] = f"{self.profile.auth_prefix}{key}"
        return headers

    def body(
        self, prompt: str, model: str, max_tokens: int, system: str | None
    ) -> dict[str, Any]:
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "stream": True,
        }

    async def deltas(self, chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
        async for payload in sse_data(chunks):
            if payload == "[DONE]":
                return
            event = _load(payload, self.profile.name)
            for choice in event.get("choices") or []:
                delta = choice.get("delta") or {}
                text = delta.get("content")
                if isinstance(text, str):
                    yield text


class VertexProvider(HTTPProvider):
    """Gemini on Vertex AI: ``:streamGenerateContent?alt=sse``.

    The access token is supplied, not minted: a service-account JWT has to be
    RSA-signed and that is not in the standard library. ``gcloud auth
    print-access-token`` or the metadata server is the caller's job, which is
    also how most deployments do it.
    """

    wire = "gemini"

    def __init__(
        self,
        project: str = "",
        location: str = "us-central1",
        access_token: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(api_key=access_token, **kwargs)
        self.project = project or os.environ.get("GOOGLE_CLOUD_PROJECT", "")
        self.location = location

    def resolve_model(self, model: str) -> str:
        return model or "gemini-2.0-flash"

    def url(self, model: str) -> str:
        if self._base_url:  # a test server, or a regional override
            base = self._base_url
        else:
            base = f"https://{self.location}-aiplatform.googleapis.com"
        if not self.project:
            raise LLMError(
                "provider 'vertex': no project. Pass provider_options.project or "
                "set GOOGLE_CLOUD_PROJECT"
            )
        return (
            f"{base}/v1/projects/{quote(self.project, safe='')}"
            f"/locations/{quote(self.location, safe='')}"
            f"/publishers/google/models/{quote(model, safe='')}"
            f":streamGenerateContent?alt=sse"
        )

    def headers(self) -> dict[str, str]:
        token = self.key(
            ("GOOGLE_ACCESS_TOKEN", "GCLOUD_ACCESS_TOKEN"), True, "provider 'vertex'"
        )
        return {"Authorization": f"Bearer {token}", **self._extra_headers}

    def body(
        self, prompt: str, model: str, max_tokens: int, system: str | None
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": max_tokens},
        }
        if system:
            # Gemini keeps the system prompt outside `contents`.
            request["systemInstruction"] = {"parts": [{"text": system}]}
        return request

    async def deltas(self, chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
        async for payload in sse_data(chunks):
            if payload == "[DONE]":
                return
            event = _load(payload, "vertex")
            for candidate in event.get("candidates") or []:
                for part in (candidate.get("content") or {}).get("parts") or []:
                    text = part.get("text")
                    if isinstance(text, str):
                        yield text


class BedrockProvider(HTTPProvider):
    """Anthropic models on Bedrock: SigV4 in, binary event frames out.

    Bedrock offers no OpenAI-compatible endpoint and no plain SSE, so both ends
    are implemented here: :func:`sigv4_headers` signs the request, and
    :func:`event_stream_payloads` unpacks the ``vnd.amazon.eventstream`` frames
    whose payloads carry base64'd Anthropic streaming events.
    """

    wire = "bedrock"

    def __init__(
        self,
        region: str = "",
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self.access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID", "")
        self.secret_key = secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        self.session_token = session_token or os.environ.get("AWS_SESSION_TOKEN")
        self._signed_body = b""

    def resolve_model(self, model: str) -> str:
        return model or "anthropic.claude-3-5-sonnet-20241022-v2:0"

    def url(self, model: str) -> str:
        base = self._base_url or f"https://bedrock-runtime.{self.region}.amazonaws.com"
        return f"{base}/model/{quote(model, safe='')}/invoke-with-response-stream"

    def body(
        self, prompt: str, model: str, max_tokens: int, system: str | None
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            request["system"] = system
        return request

    def headers(self) -> dict[str, str]:  # pragma: no cover - see stream()
        raise LLMError("bedrock headers depend on the body; stream() builds them")

    async def stream(
        self,
        prompt: str,
        *,
        model: str,
        max_tokens: int,
        system: str | None = None,
    ) -> AsyncIterator[str]:
        # Overridden because SigV4 hashes the body, so headers cannot be built
        # before it exists — the one place the base class's split does not fit.
        if not self.access_key or not self.secret_key:
            raise LLMError(
                "provider 'bedrock': no credentials. Pass provider_options "
                "access_key/secret_key or set AWS_ACCESS_KEY_ID and "
                "AWS_SECRET_ACCESS_KEY"
            )
        resolved = self.resolve_model(model)
        url = self.url(resolved)
        payload = self.body(prompt, resolved, max_tokens, system)
        raw = json.dumps(payload).encode("utf-8")
        headers = sigv4_headers(
            "POST",
            url,
            raw,
            region=self.region,
            service="bedrock",
            access_key=self.access_key,
            secret_key=self.secret_key,
            session_token=self.session_token,
        )
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/vnd.amazon.eventstream"
        headers.update(self._extra_headers)

        chunks = self.pool.stream_body(url, payload, headers, timeout_s=self.timeout_s)
        async for text in self.deltas(chunks):
            if text:
                yield text

    async def deltas(self, chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
        async for payload in event_stream_payloads(chunks):
            frame = _load(payload.decode("utf-8", errors="replace"), "bedrock")
            encoded = frame.get("bytes")
            if not isinstance(encoded, str):
                continue
            event = _load(base64.b64decode(encoded).decode("utf-8"), "bedrock")
            if event.get("type") == "content_block_delta":
                text = (event.get("delta") or {}).get("text")
                if isinstance(text, str):
                    yield text


# ------------------------------------------------------------------- framing


async def sse_data(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    """Yield the payload of every ``data:`` line in a Server-Sent Events body.

    Buffers across chunk boundaries: a frame is split wherever TCP felt like
    splitting it, which is the bug every hand-rolled SSE reader has first.
    """
    buffer = b""
    async for chunk in chunks:
        buffer += chunk
        while b"\n" in buffer:
            line, _, buffer = buffer.partition(b"\n")
            stripped = line.strip()
            if stripped.startswith(b"data:"):
                yield stripped[5:].strip().decode("utf-8", errors="replace")
    tail = buffer.strip()
    if tail.startswith(b"data:"):
        yield tail[5:].strip().decode("utf-8", errors="replace")


async def event_stream_payloads(chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """Unpack ``vnd.amazon.eventstream`` frames, yielding each payload.

    Layout per message: an 8-byte prelude (total length, headers length) with its
    own CRC32, then headers, then payload, then a CRC32 over everything before
    it. Both CRCs are checked — a silently mis-framed stream would otherwise
    surface as unexplained JSON errors much later.
    """
    buffer = b""
    async for chunk in chunks:
        buffer += chunk
        while True:
            frame, buffer, complete = _take_frame(buffer)
            if not complete:
                break
            yield frame


def _take_frame(buffer: bytes) -> tuple[bytes, bytes, bool]:
    if len(buffer) < 16:
        return b"", buffer, False
    total, headers_length = struct.unpack(">II", buffer[:8])
    if total < 16 or headers_length > total - 16:
        raise LLMError(f"bedrock: implausible event frame lengths {total}/{headers_length}")
    if len(buffer) < total:
        return b"", buffer, False
    (prelude_crc,) = struct.unpack(">I", buffer[8:12])
    if zlib.crc32(buffer[:8]) != prelude_crc:
        raise LLMError("bedrock: event frame prelude failed its CRC")
    (message_crc,) = struct.unpack(">I", buffer[total - 4 : total])
    if zlib.crc32(buffer[: total - 4]) != message_crc:
        raise LLMError("bedrock: event frame failed its CRC")
    payload = buffer[12 + headers_length : total - 4]
    return payload, buffer[total:], True


def encode_event_frame(payload: bytes, headers: Mapping[str, str] | None = None) -> bytes:
    """Build one event-stream message. Used by the tests to act as Bedrock."""
    encoded_headers = b"".join(
        bytes([len(name)])
        + name.encode("utf-8")
        + b"\x07"
        + struct.pack(">H", len(value))
        + value.encode("utf-8")
        for name, value in (headers or {}).items()
    )
    total = 16 + len(encoded_headers) + len(payload)
    prelude = struct.pack(">II", total, len(encoded_headers))
    message = prelude + struct.pack(">I", zlib.crc32(prelude)) + encoded_headers + payload
    return message + struct.pack(">I", zlib.crc32(message))


# --------------------------------------------------------------------- SigV4


def sigv4_headers(
    method: str,
    url: str,
    body: bytes,
    *,
    region: str,
    service: str,
    access_key: str,
    secret_key: str,
    session_token: str | None = None,
    now: datetime | None = None,
) -> dict[str, str]:
    """AWS Signature Version 4, in the standard library.

    Deliberately narrow: the request this signs always has a body, no unusual
    headers, and an already-encoded path, so the canonicalisation that trips
    people up (empty queries, duplicate headers, double-encoded paths) does not
    arise. Anything wider should use botocore rather than grow this.
    """
    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    amz_date = stamp.strftime("%Y%m%dT%H%M%SZ")
    datestamp = stamp.strftime("%Y%m%d")

    parts = urlsplit(url)
    host = parts.netloc
    canonical_uri = parts.path or "/"
    canonical_query = parts.query
    payload_hash = hashlib.sha256(body).hexdigest()

    signed_parts = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if session_token:
        signed_parts["x-amz-security-token"] = session_token
    signed_headers = ";".join(sorted(signed_parts))
    canonical_headers = "".join(
        f"{name}:{signed_parts[name]}\n" for name in sorted(signed_parts)
    )

    canonical_request = "\n".join(
        [
            method.upper(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    scope = f"{datestamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )

    signing_key = _signing_key(secret_key, datestamp, region, service)
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    headers = {
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
        "X-Amz-Date": amz_date,
        "X-Amz-Content-Sha256": payload_hash,
    }
    if session_token:
        headers["X-Amz-Security-Token"] = session_token
    return headers


def canonical_request_for(
    method: str, url: str, body: bytes, amz_date: str, session_token: str | None = None
) -> str:
    """The canonical request as :func:`sigv4_headers` builds it. For tests."""
    parts = urlsplit(url)
    payload_hash = hashlib.sha256(body).hexdigest()
    signed_parts = {
        "host": parts.netloc,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if session_token:
        signed_parts["x-amz-security-token"] = session_token
    return "\n".join(
        [
            method.upper(),
            parts.path or "/",
            parts.query,
            "".join(f"{n}:{signed_parts[n]}\n" for n in sorted(signed_parts)),
            ";".join(sorted(signed_parts)),
            payload_hash,
        ]
    )


def _signing_key(secret_key: str, datestamp: str, region: str, service: str) -> bytes:
    key = f"AWS4{secret_key}".encode("utf-8")
    for element in (datestamp, region, service, "aws4_request"):
        key = hmac.new(key, element.encode("utf-8"), hashlib.sha256).digest()
    return key


# ------------------------------------------------------------------ registry


def _load(text: str, provider: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LLMError(f"provider {provider!r}: unparseable stream frame: {exc}") from exc
    if not isinstance(value, dict):
        raise LLMError(f"provider {provider!r}: stream frame was not an object")
    return value


def _openai_factory(profile: Profile) -> Any:
    def factory(**options: Any) -> OpenAICompatibleProvider:
        return OpenAICompatibleProvider(profile=profile, **options)

    return factory


def register_all() -> list[str]:
    """Register every adapter. Idempotent, so importing twice is harmless."""
    from .llm import known_providers

    registered = []
    for name, profile in OPENAI_PROFILES.items():
        if name not in known_providers():
            register_provider(name, _openai_factory(profile))
            registered.append(name)
    for name, cls in (("vertex", VertexProvider), ("bedrock", BedrockProvider)):
        if name not in known_providers():
            register_provider(name, cls)
            registered.append(name)
    return registered


def profiles() -> Iterator[Profile]:
    return iter(OPENAI_PROFILES.values())


register_all()
