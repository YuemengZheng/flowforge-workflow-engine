import unittest
from typing import Any

from flowforge import (
    AnthropicProvider,
    EchoProvider,
    Edge,
    Graph,
    NodeSpec,
    RunStatus,
    WorkflowEngine,
    known_providers,
)
from flowforge.llm import DEFAULT_MAX_TOKENS, DEFAULT_MODEL, LLMError, build_provider


class FakeStream:
    """Mimics the async context manager returned by client.messages.stream()."""

    def __init__(self, chunks, recorder, request):
        self._chunks = chunks
        self._recorder = recorder
        self._request = request

    async def __aenter__(self):
        self._recorder.append(self._request)
        return self

    async def __aexit__(self, *exc_info):
        return False

    @property
    def text_stream(self):
        async def gen():
            for chunk in self._chunks:
                yield chunk

        return gen()


class FakeMessages:
    def __init__(self, chunks, recorder):
        self._chunks = chunks
        self.requests = recorder

    def stream(self, **request: Any):
        return FakeStream(self._chunks, self.requests, request)


class FakeAnthropicClient:
    def __init__(self, chunks):
        self.requests: list[dict[str, Any]] = []
        self.messages = FakeMessages(chunks, self.requests)


class ProviderRegistryTests(unittest.TestCase):
    def test_the_providers_this_module_owns_are_registered(self):
        # `llm.py` registers these two; the vendor adapters in `providers.py`
        # register themselves and are pinned by tests/test_providers.py.
        self.assertIn("echo", known_providers())
        self.assertIn("anthropic", known_providers())

    def test_build_echo(self):
        self.assertIsInstance(build_provider("echo", {"words_per_chunk": 2}), EchoProvider)

    def test_unknown_provider_is_reported(self):
        with self.assertRaises(LLMError) as ctx:
            build_provider("gpt-42")
        self.assertIn("echo", str(ctx.exception))


class EchoProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_chunks_reassemble_into_the_prompt(self):
        provider = EchoProvider(words_per_chunk=2)
        chunks = [
            chunk
            async for chunk in provider.stream(
                "a b c d e", model="m", max_tokens=10
            )
        ]
        self.assertEqual(chunks, ["a b", " c d", " e"])
        self.assertEqual("".join(chunks), "a b c d e")


class LLMNodeTests(unittest.IsolatedAsyncioTestCase):
    async def test_prompt_is_resolved_from_upstream_before_the_call(self):
        graph = Graph(
            [
                NodeSpec("fetch", "stub", {"output": {"topic": "kahn scheduling"}}),
                NodeSpec("write", "llm", {"prompt": "explain {{fetch.topic}} briefly"}),
            ],
            [Edge("fetch", "write")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        outputs = result.outputs_of("write")
        self.assertEqual(outputs["text"], "explain kahn scheduling briefly")
        self.assertEqual(outputs["provider"], "echo")
        self.assertEqual(outputs["model"], DEFAULT_MODEL)

    async def test_missing_prompt_fails_the_node(self):
        graph = Graph([NodeSpec("write", "llm", {})], [])
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("prompt", result.nodes["write"].error)

    async def test_unknown_provider_fails_the_node(self):
        graph = Graph([NodeSpec("write", "llm", {"prompt": "hi", "provider": "nope"})], [])
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("unknown LLM provider", result.nodes["write"].error)


class AnthropicProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_shape_and_streamed_text(self):
        client = FakeAnthropicClient(["Kahn ", "keeps ", "a counter."])
        provider = AnthropicProvider(client=client)

        chunks = [
            chunk
            async for chunk in provider.stream(
                "explain kahn",
                model=DEFAULT_MODEL,
                max_tokens=DEFAULT_MAX_TOKENS,
                system="be terse",
            )
        ]

        self.assertEqual("".join(chunks), "Kahn keeps a counter.")
        request = client.requests[0]
        self.assertEqual(request["model"], "claude-opus-5")
        self.assertEqual(request["max_tokens"], DEFAULT_MAX_TOKENS)
        self.assertEqual(request["system"], "be terse")
        self.assertEqual(
            request["messages"], [{"role": "user", "content": "explain kahn"}]
        )

    async def test_system_is_omitted_when_unset(self):
        client = FakeAnthropicClient(["ok"])
        provider = AnthropicProvider(client=client)
        [chunk async for chunk in provider.stream("hi", model="m", max_tokens=10)]

        self.assertNotIn("system", client.requests[0])

    async def test_node_drives_the_provider_end_to_end(self):
        client = FakeAnthropicClient(["one ", "two"])
        graph = Graph(
            [
                NodeSpec(
                    "write",
                    "llm",
                    {
                        "prompt": "hello",
                        "provider": "fake",
                        "system": "terse",
                        "max_tokens": 512,
                    },
                )
            ],
            [],
        )
        from flowforge import register_provider

        register_provider("fake", lambda: AnthropicProvider(client=client))
        try:
            result = await WorkflowEngine(graph).run()
        finally:
            from flowforge.llm import _PROVIDERS

            _PROVIDERS.pop("fake", None)

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs_of("write")["text"], "one two")
        self.assertEqual(result.outputs_of("write")["chunks"], 2)
        self.assertEqual(client.requests[0]["max_tokens"], 512)


if __name__ == "__main__":
    unittest.main()
