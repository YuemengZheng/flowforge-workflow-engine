"""Provider adapters, against an in-process server speaking each vendor's wire.

``FakeVendor`` is a real socket serving real HTTP/1.1 with chunked
``text/event-stream`` bodies, and it records what it was sent. So these tests
check the two things that are actually ours: the **request** each adapter builds
(URL, auth header, JSON body) and the **stream parsing** on the way back,
including frames split across TCP boundaries.

What they do not check, and cannot: that any vendor accepts these requests. No
adapter here has ever been pointed at a live API — see the README's status table.
"""

import asyncio
import hashlib
import hmac
import json
import unittest
from datetime import datetime, timezone

from flowforge import LLMError, build_provider
from flowforge.providers import (
    OPENAI_PROFILES,
    BedrockProvider,
    OpenAICompatibleProvider,
    VertexProvider,
    _signing_key,
    canonical_request_for,
    close_shared_pool,
    encode_event_frame,
    sigv4_headers,
)


class FakeVendor:
    """Serves one canned streaming response and remembers the request."""

    def __init__(self, frames: list[bytes], status: int = 200, content_type: str = "text/event-stream"):
        self.frames = frames
        self.status = status
        self.content_type = content_type
        self.requests: list[dict] = []
        self.server: asyncio.AbstractServer | None = None

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    @property
    def last(self) -> dict:
        return self.requests[-1]

    async def _handle(self, reader, writer):
        request_line = await reader.readline()
        method, target, _ = request_line.decode().split()
        headers = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode().partition(":")
            headers[name.strip().lower()] = value.strip()
        length = int(headers.get("content-length", 0) or 0)
        raw = await reader.readexactly(length) if length else b""
        self.requests.append(
            {
                "method": method,
                "target": target,
                "headers": headers,
                "body": json.loads(raw) if raw else {},
            }
        )

        reason = "OK" if self.status == 200 else "Bad Request"
        writer.write(
            f"HTTP/1.1 {self.status} {reason}\r\n"
            f"Content-Type: {self.content_type}\r\n"
            f"Transfer-Encoding: chunked\r\n"
            f"Connection: close\r\n\r\n".encode()
        )
        for frame in self.frames:
            writer.write(b"%x\r\n" % len(frame) + frame + b"\r\n")
            await writer.drain()
        writer.write(b"0\r\n\r\n")
        await writer.drain()
        writer.close()


def sse(*payloads: str) -> list[bytes]:
    return [f"data: {payload}\n\n".encode() for payload in payloads]


def openai_chunk(text: str) -> str:
    return json.dumps({"choices": [{"delta": {"content": text}}]})


def gemini_chunk(text: str) -> str:
    return json.dumps({"candidates": [{"content": {"parts": [{"text": text}]}}]})


def bedrock_frame(text: str) -> bytes:
    import base64

    inner = json.dumps({"type": "content_block_delta", "delta": {"text": text}})
    outer = json.dumps({"bytes": base64.b64encode(inner.encode()).decode()})
    return encode_event_frame(outer.encode(), {":event-type": "chunk"})


async def collect(provider, prompt="hello", model="m", max_tokens=64, system=None):
    return [
        chunk
        async for chunk in provider.stream(
            prompt, model=model, max_tokens=max_tokens, system=system
        )
    ]


class OpenAIWireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.vendor = FakeVendor(sse(openai_chunk("Hel"), openai_chunk("lo!"), "[DONE]"))
        self.port = await self.vendor.start()
        self.base = f"http://127.0.0.1:{self.port}/v1"

    async def asyncTearDown(self):
        await close_shared_pool()
        await self.vendor.stop()

    async def test_streams_and_assembles_deltas(self):
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="k", base_url=self.base
        )
        self.assertEqual(await collect(provider), ["Hel", "lo!"])

    async def test_request_shape(self):
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="secret", base_url=self.base
        )
        await collect(provider, prompt="why?", model="gpt-4o-mini", system="Be terse")

        sent = self.vendor.last
        self.assertEqual(sent["method"], "POST")
        self.assertEqual(sent["target"], "/v1/chat/completions")
        self.assertEqual(sent["headers"]["authorization"], "Bearer secret")
        self.assertTrue(sent["body"]["stream"])
        self.assertEqual(sent["body"]["model"], "gpt-4o-mini")
        self.assertEqual(
            sent["body"]["messages"],
            [
                {"role": "system", "content": "Be terse"},
                {"role": "user", "content": "why?"},
            ],
        )

    async def test_done_ends_the_stream_even_with_frames_after_it(self):
        self.vendor.frames = sse(openai_chunk("a"), "[DONE]", openai_chunk("ignored"))
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="k", base_url=self.base
        )
        self.assertEqual(await collect(provider), ["a"])

    async def test_a_frame_split_across_tcp_chunks_is_reassembled(self):
        whole = f"data: {openai_chunk('split me')}\n\n".encode()
        self.vendor.frames = [whole[:9], whole[9:20], whole[20:]]
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="k", base_url=self.base
        )
        self.assertEqual(await collect(provider), ["split me"])

    async def test_empty_deltas_are_not_emitted(self):
        self.vendor.frames = sse(
            json.dumps({"choices": [{"delta": {}}]}),
            openai_chunk(""),
            openai_chunk("real"),
        )
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="k", base_url=self.base
        )
        self.assertEqual(await collect(provider), ["real"])

    async def test_an_error_status_says_what_the_vendor_said(self):
        self.vendor.status = 400
        self.vendor.frames = [b'{"error":{"message":"bad model"}}']
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="k", base_url=self.base
        )
        with self.assertRaises(Exception) as raised:
            await collect(provider)
        self.assertIn("bad model", str(raised.exception))

    async def test_unparseable_frame_names_the_provider(self):
        self.vendor.frames = sse("{not json")
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["openai"], api_key="k", base_url=self.base
        )
        with self.assertRaises(LLMError) as raised:
            await collect(provider)
        self.assertIn("openai", str(raised.exception))


class ProfileTests(unittest.IsolatedAsyncioTestCase):
    """Every OpenAI-shaped vendor, driven through the same fake server."""

    async def asyncSetUp(self):
        self.vendor = FakeVendor(sse(openai_chunk("ok"), "[DONE]"))
        self.port = await self.vendor.start()
        self.base = f"http://127.0.0.1:{self.port}/v1"

    async def asyncTearDown(self):
        await close_shared_pool()
        await self.vendor.stop()

    async def test_each_profile_streams_through_its_own_wire(self):
        for name, profile in OPENAI_PROFILES.items():
            with self.subTest(provider=name):
                provider = OpenAICompatibleProvider(
                    profile=profile, api_key="k", base_url=self.base
                )
                self.assertEqual(await collect(provider), ["ok"])

    async def test_profiles_have_distinct_documented_endpoints(self):
        hosted = [p for p in OPENAI_PROFILES.values() if not p.needs_base_url]
        urls = [p.base_url for p in hosted]

        self.assertEqual(len(urls), len(set(urls)), "two vendors share a base URL")
        for profile in hosted:
            with self.subTest(provider=profile.name):
                self.assertTrue(profile.base_url.startswith("http"))
                self.assertTrue(profile.default_model)

    async def test_a_vendor_needing_a_base_url_says_so(self):
        for name in ("azure", "openai_compatible"):
            with self.subTest(provider=name):
                with self.assertRaises(LLMError) as raised:
                    OpenAICompatibleProvider(profile=OPENAI_PROFILES[name], api_key="k")
                self.assertIn("base_url", str(raised.exception))

    async def test_a_missing_key_is_reported_before_any_request(self):
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["groq"], base_url=self.base
        )
        with self.assertRaises(LLMError) as raised:
            await collect(provider)
        self.assertIn("GROQ_API_KEY", str(raised.exception))
        self.assertEqual(self.vendor.requests, [])

    async def test_ollama_needs_no_key(self):
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["ollama"], base_url=self.base
        )
        await collect(provider)
        self.assertNotIn("authorization", self.vendor.last["headers"])

    async def test_the_profile_default_model_is_used_when_none_is_given(self):
        provider = OpenAICompatibleProvider(
            profile=OPENAI_PROFILES["deepseek"], api_key="k", base_url=self.base
        )
        await collect(provider, model="")
        self.assertEqual(self.vendor.last["body"]["model"], "deepseek-chat")


class AzureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.vendor = FakeVendor(sse(openai_chunk("hi"), "[DONE]"))
        self.port = await self.vendor.start()

    async def asyncTearDown(self):
        await close_shared_pool()
        await self.vendor.stop()

    async def test_deployment_goes_in_the_path_and_the_key_in_its_own_header(self):
        provider = build_provider(
            "azure",
            {"api_key": "azure-secret", "base_url": f"http://127.0.0.1:{self.port}"},
        )
        await collect(provider, model="my-deployment")

        sent = self.vendor.last
        self.assertEqual(
            sent["target"],
            "/openai/deployments/my-deployment/chat/completions?api-version=2024-10-21",
        )
        self.assertEqual(sent["headers"]["api-key"], "azure-secret")
        self.assertNotIn("authorization", sent["headers"])


class VertexTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.vendor = FakeVendor(sse(gemini_chunk("Gem"), gemini_chunk("ini")))
        self.port = await self.vendor.start()
        self.base = f"http://127.0.0.1:{self.port}"

    async def asyncTearDown(self):
        await close_shared_pool()
        await self.vendor.stop()

    def provider(self, **kwargs):
        return VertexProvider(
            project="proj", access_token="token", base_url=self.base, **kwargs
        )

    async def test_streams_gemini_parts(self):
        self.assertEqual(await collect(self.provider()), ["Gem", "ini"])

    async def test_url_and_body_follow_the_vertex_shape(self):
        await collect(self.provider(), model="gemini-2.0-flash", system="Be brief")

        sent = self.vendor.last
        self.assertEqual(
            sent["target"],
            "/v1/projects/proj/locations/us-central1/publishers/google/models/"
            "gemini-2.0-flash:streamGenerateContent?alt=sse",
        )
        self.assertEqual(sent["headers"]["authorization"], "Bearer token")
        self.assertEqual(
            sent["body"]["contents"],
            [{"role": "user", "parts": [{"text": "hello"}]}],
        )
        # The system prompt sits outside `contents` in this API.
        self.assertEqual(sent["body"]["systemInstruction"]["parts"][0]["text"], "Be brief")
        self.assertEqual(sent["body"]["generationConfig"]["maxOutputTokens"], 64)

    async def test_a_missing_project_is_reported(self):
        provider = VertexProvider(project="", access_token="t", base_url=self.base)
        with self.assertRaises(LLMError) as raised:
            await collect(provider)
        self.assertIn("project", str(raised.exception))

    async def test_a_missing_token_is_reported(self):
        provider = VertexProvider(project="p", base_url=self.base)
        with self.assertRaises(LLMError) as raised:
            await collect(provider)
        self.assertIn("GOOGLE_ACCESS_TOKEN", str(raised.exception))


class BedrockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.vendor = FakeVendor(
            [bedrock_frame("Clau"), bedrock_frame("de")],
            content_type="application/vnd.amazon.eventstream",
        )
        self.port = await self.vendor.start()
        self.base = f"http://127.0.0.1:{self.port}"

    async def asyncTearDown(self):
        await close_shared_pool()
        await self.vendor.stop()

    def provider(self):
        return BedrockProvider(
            region="us-east-1",
            access_key="AKIDEXAMPLE",
            secret_key="secret",
            base_url=self.base,
        )

    async def test_decodes_event_frames_into_text(self):
        self.assertEqual(await collect(self.provider()), ["Clau", "de"])

    async def test_frames_split_across_tcp_chunks_are_reassembled(self):
        whole = bedrock_frame("whole frame")
        self.vendor.frames = [whole[:5], whole[5:17], whole[17:]]
        self.assertEqual(await collect(self.provider()), ["whole frame"])

    async def test_request_is_signed_and_shaped_for_bedrock(self):
        await collect(self.provider(), model="anthropic.claude-3-5-sonnet")

        sent = self.vendor.last
        self.assertEqual(
            sent["target"],
            "/model/anthropic.claude-3-5-sonnet/invoke-with-response-stream",
        )
        self.assertTrue(
            sent["headers"]["authorization"].startswith(
                "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"
            )
        )
        self.assertIn("x-amz-date", sent["headers"])
        self.assertIn("x-amz-content-sha256", sent["headers"])
        self.assertEqual(sent["body"]["anthropic_version"], "bedrock-2023-05-31")
        self.assertEqual(sent["body"]["max_tokens"], 64)

    async def test_missing_credentials_are_reported_before_any_request(self):
        provider = BedrockProvider(
            region="us-east-1", access_key="", secret_key="", base_url=self.base
        )
        with self.assertRaises(LLMError) as raised:
            await collect(provider)
        self.assertIn("AWS_ACCESS_KEY_ID", str(raised.exception))
        self.assertEqual(self.vendor.requests, [])

    async def test_a_corrupted_frame_is_caught_by_its_crc(self):
        frame = bytearray(bedrock_frame("tampered"))
        frame[-6] ^= 0xFF  # flip a payload bit, leaving the CRC stale
        self.vendor.frames = [bytes(frame)]
        with self.assertRaises(LLMError) as raised:
            await collect(self.provider())
        self.assertIn("CRC", str(raised.exception))


class SigV4Tests(unittest.TestCase):
    """Structure and determinism. Not a claim that AWS accepts these."""

    FIXED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    URL = "https://bedrock-runtime.us-east-1.amazonaws.com/model/m/invoke-with-response-stream"

    def sign(self, **kwargs):
        return sigv4_headers(
            "POST",
            self.URL,
            b'{"a":1}',
            region="us-east-1",
            service="bedrock",
            access_key="AKIDEXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG",
            now=self.FIXED,
            **kwargs,
        )

    def test_credential_scope_is_date_region_service(self):
        self.assertIn(
            "Credential=AKIDEXAMPLE/20260102/us-east-1/bedrock/aws4_request",
            self.sign()["Authorization"],
        )

    def test_signature_is_deterministic_for_fixed_inputs(self):
        self.assertEqual(self.sign()["Authorization"], self.sign()["Authorization"])

    def test_a_different_body_changes_the_signature(self):
        other = sigv4_headers(
            "POST",
            self.URL,
            b'{"a":2}',
            region="us-east-1",
            service="bedrock",
            access_key="AKIDEXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG",
            now=self.FIXED,
        )
        self.assertNotEqual(self.sign()["Authorization"], other["Authorization"])
        self.assertNotEqual(
            self.sign()["X-Amz-Content-Sha256"], other["X-Amz-Content-Sha256"]
        )

    def test_signed_headers_match_the_headers_actually_sent(self):
        headers = self.sign()
        signed = headers["Authorization"].split("SignedHeaders=")[1].split(",")[0]
        self.assertEqual(signed, "host;x-amz-content-sha256;x-amz-date")
        # Everything signed apart from `host` is sent explicitly; `host` is added
        # by the HTTP layer.
        self.assertIn("X-Amz-Date", headers)
        self.assertIn("X-Amz-Content-Sha256", headers)

    def test_a_session_token_is_signed_and_sent(self):
        headers = self.sign(session_token="temp-token")
        signed = headers["Authorization"].split("SignedHeaders=")[1].split(",")[0]
        self.assertIn("x-amz-security-token", signed)
        self.assertEqual(headers["X-Amz-Security-Token"], "temp-token")

    def test_canonical_request_has_the_documented_six_lines(self):
        canonical = canonical_request_for("POST", self.URL, b'{"a":1}', "20260102T030405Z")
        lines = canonical.split("\n")

        self.assertEqual(lines[0], "POST")
        self.assertEqual(lines[1], "/model/m/invoke-with-response-stream")
        self.assertEqual(lines[2], "")  # no query on this endpoint
        self.assertEqual(lines[-1], self.sign()["X-Amz-Content-Sha256"])
        self.assertEqual(lines[-2], "host;x-amz-content-sha256;x-amz-date")


class SigV4OfficialVectorTests(unittest.TestCase):
    """The signing chain against AWS's own published answer.

    Everything in :class:`SigV4Tests` asserts *structure* — that the scope reads
    the way it should, that the signature moves when the body does. None of it
    proves the number is the one AWS would compute. These constants are the
    ``get-vanilla`` case of AWS's signing test suite, copied verbatim from
    ``awslabs/aws-c-auth`` (``tests/aws-signing-test-suite/v4/get-vanilla``).

    The suite's canonical request cannot be replayed through
    :func:`canonical_request_for`: ``get-vanilla`` signs ``host;x-amz-date``,
    while this signer always signs ``x-amz-content-sha256`` too. So what is
    pinned here is the half that does not depend on which headers a caller
    signs — the signing-key ladder and the final HMAC — by feeding AWS's own
    string-to-sign through them and comparing with AWS's own signature.
    """

    SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
    DATESTAMP = "20150830"
    REGION = "us-east-1"
    SERVICE = "service"

    CANONICAL_REQUEST = (
        "GET\n"
        "/\n"
        "\n"
        "host:example.amazonaws.com\n"
        "x-amz-date:20150830T123600Z\n"
        "\n"
        "host;x-amz-date\n"
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    STRING_TO_SIGN = (
        "AWS4-HMAC-SHA256\n"
        "20150830T123600Z\n"
        "20150830/us-east-1/service/aws4_request\n"
        "bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"
    )
    SIGNATURE = "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"

    def test_signature_matches_the_aws_test_suite_vector(self):
        key = _signing_key(self.SECRET, self.DATESTAMP, self.REGION, self.SERVICE)
        signature = hmac.new(
            key, self.STRING_TO_SIGN.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        self.assertEqual(signature, self.SIGNATURE)

    def test_the_embedded_vector_is_internally_consistent(self):
        # Guards the constants above against a bad edit: the last line of the
        # string-to-sign is the hash of the canonical request, so a typo in
        # either shows up here rather than as a confusing failure above.
        self.assertEqual(
            self.STRING_TO_SIGN.split("\n")[-1],
            hashlib.sha256(self.CANONICAL_REQUEST.encode("utf-8")).hexdigest(),
        )


class NodeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """A workflow node reaching a vendor through the registry."""

    async def asyncSetUp(self):
        self.vendor = FakeVendor(sse(openai_chunk("from "), openai_chunk("groq"), "[DONE]"))
        self.port = await self.vendor.start()

    async def asyncTearDown(self):
        await close_shared_pool()
        await self.vendor.stop()

    async def test_an_llm_node_streams_from_a_registered_vendor(self):
        from flowforge import Graph, WorkflowEngine

        graph = Graph.from_dict(
            {
                "id": "ask",
                "nodes": [
                    {
                        "id": "ask",
                        "type": "llm",
                        "config": {
                            "provider": "groq",
                            "model": "llama-3.3-70b-versatile",
                            "max_tokens": 32,
                            "prompt": "why?",
                            "provider_options": {
                                "api_key": "k",
                                "base_url": f"http://127.0.0.1:{self.port}/v1",
                            },
                        },
                    }
                ],
            }
        )
        result = await WorkflowEngine(graph).run()

        self.assertEqual(result.outputs_of("ask")["text"], "from groq")
        self.assertEqual(result.outputs_of("ask")["chunks"], 2)
        self.assertEqual(result.outputs_of("ask")["provider"], "groq")

    async def test_every_registered_provider_is_reachable_by_name(self):
        from flowforge import known_providers

        expected = {
            "anthropic", "azure", "bedrock", "deepseek", "echo", "fireworks",
            "groq", "mistral", "ollama", "openai", "openai_compatible",
            "together", "vertex",
        }
        self.assertEqual(set(known_providers()), expected)


if __name__ == "__main__":
    unittest.main()
