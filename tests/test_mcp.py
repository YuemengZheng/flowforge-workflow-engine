"""MCP client, gateway and tool node — against a real server over real pipes.

``tests/fixtures/echo_mcp_server.py`` is an actual MCP server subprocess, so the
handshake, pagination, error replies and tool calls are exercised end to end
rather than against a mock that agrees with whatever the client sends.
"""

import asyncio
import json
import sys
import unittest
from pathlib import Path

from flowforge import Edge, Graph, NodeSpec, RunStatus, WorkflowEngine
from flowforge.mcp import (
    HTTPTransport,
    MCPClient,
    MCPError,
    MCPGateway,
    MCPToolError,
    StdioTransport,
    ToolSpec,
    build_transport,
    register_gateway,
    validate_arguments,
)

SERVER = str(Path(__file__).resolve().parent / "fixtures" / "echo_mcp_server.py")


def stdio(name="echo", **env):
    return MCPClient(name, StdioTransport(sys.executable, [SERVER], env=env or None))


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = stdio()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_handshake_reports_the_server(self):
        result = await self.client.initialize()
        self.assertEqual(result["serverInfo"]["name"], "echo-server")
        self.assertIn("capabilities", result)

    async def test_handshake_happens_once(self):
        await self.client.initialize()
        self.assertEqual(await self.client.initialize(), {})

    async def test_discovering_tools(self):
        tools = await self.client.list_tools()
        self.assertEqual(sorted(t.name for t in tools), ["add", "echo", "explode"])

        echo = next(t for t in tools if t.name == "echo")
        self.assertEqual(echo.qualified, "echo/echo")
        self.assertEqual(echo.input_schema["required"], ["text"])
        self.assertEqual(echo.description, "Echo the text back.")

    async def test_calling_a_tool(self):
        result = await self.client.call_tool("echo", {"text": "hello mcp"})
        self.assertEqual(result["text"], "hello mcp")

    async def test_structured_content_comes_through(self):
        result = await self.client.call_tool("add", {"a": 2, "b": 3})
        self.assertEqual(result["text"], "5")
        self.assertEqual(result["structured"], {"sum": 5})

    async def test_a_tool_reporting_failure_raises(self):
        with self.assertRaises(MCPToolError) as ctx:
            await self.client.call_tool("explode", {})
        self.assertIn("detonated on purpose", str(ctx.exception))

    async def test_unknown_method_surfaces_the_rpc_error(self):
        with self.assertRaises(MCPError) as ctx:
            await self.client._call("resources/list")
        self.assertIn("-32601", str(ctx.exception))

    async def test_calls_are_concurrency_safe(self):
        """Replies must be matched to requests, not read in arrival order."""
        results = await asyncio.gather(
            *(self.client.call_tool("echo", {"text": f"m{i}"}) for i in range(10))
        )
        self.assertEqual([r["text"] for r in results], [f"m{i}" for i in range(10)])


class PaginationAndNoiseTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_follows_next_cursor(self):
        client = stdio(MCP_PAGE_SIZE="1")
        try:
            tools = await client.list_tools()
            self.assertEqual(sorted(t.name for t in tools), ["add", "echo", "explode"])
        finally:
            await client.close()

    async def test_non_json_chatter_on_stdout_is_ignored(self):
        client = stdio(MCP_NOISE="1")
        try:
            result = await client.call_tool("echo", {"text": "still fine"})
            self.assertEqual(result["text"], "still fine")
        finally:
            await client.close()


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MCPGateway()
        self.gateway.add_server(
            "alpha", StdioTransport(sys.executable, [SERVER], env={"MCP_SERVER_NAME": "a"})
        )
        self.gateway.add_server(
            "beta", StdioTransport(sys.executable, [SERVER], env={"MCP_SERVER_NAME": "b"})
        )

    async def asyncTearDown(self):
        await self.gateway.close()

    async def test_discovery_builds_a_catalogue_from_every_server(self):
        tools = await self.gateway.discover()
        self.assertEqual(
            [t.qualified for t in tools],
            ["alpha/add", "alpha/echo", "alpha/explode",
             "beta/add", "beta/echo", "beta/explode"],
        )

    async def test_colliding_names_must_be_qualified(self):
        await self.gateway.discover()
        with self.assertRaises(MCPError) as ctx:
            self.gateway.resolve("echo")
        self.assertIn("more than one server", str(ctx.exception))

        # The qualified form is unambiguous and works.
        self.assertEqual(self.gateway.resolve("alpha/echo").server, "alpha")

    async def test_calling_through_the_gateway(self):
        result = await self.gateway.call("beta/echo", {"text": "routed"})
        self.assertEqual(result["text"], "routed")

    async def test_unknown_tool_lists_what_was_discovered(self):
        await self.gateway.discover()
        with self.assertRaises(MCPError) as ctx:
            self.gateway.resolve("nope")
        self.assertIn("alpha/add", str(ctx.exception))

    async def test_arguments_are_validated_before_the_call(self):
        with self.assertRaises(MCPError) as ctx:
            await self.gateway.call("alpha/add", {"a": 1})
        self.assertIn("missing required argument(s) b", str(ctx.exception))

    async def test_discovery_is_cached_until_refreshed(self):
        first = await self.gateway.discover()
        second = await self.gateway.discover()
        self.assertEqual([t.qualified for t in first], [t.qualified for t in second])

    async def test_a_single_name_resolves_when_only_one_server_has_it(self):
        solo = MCPGateway()
        solo.add_server("only", StdioTransport(sys.executable, [SERVER]))
        try:
            await solo.discover()
            self.assertEqual(solo.resolve("echo").server, "only")
        finally:
            await solo.close()


class UnreachableServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_dead_server_does_not_hide_the_working_ones(self):
        gateway = MCPGateway()
        gateway.add_server("good", StdioTransport(sys.executable, [SERVER]))
        gateway.add_server("dead", StdioTransport(sys.executable, ["-c", "raise SystemExit(1)"]))
        try:
            tools = await gateway.discover()
            self.assertTrue(all(t.server == "good" for t in tools))
            self.assertEqual(len(tools), 3)
        finally:
            await gateway.close()

    async def test_all_servers_dead_is_an_error(self):
        gateway = MCPGateway()
        gateway.add_server("dead", StdioTransport(sys.executable, ["-c", "raise SystemExit(1)"]))
        try:
            with self.assertRaises(MCPError) as ctx:
                await gateway.discover()
            self.assertIn("no MCP server could be reached", str(ctx.exception))
        finally:
            await gateway.close()


class TransportConfigTests(unittest.TestCase):
    def test_stdio_from_config(self):
        transport = build_transport({"type": "stdio", "command": "echo", "args": ["hi"]})
        self.assertIsInstance(transport, StdioTransport)

    def test_http_from_config(self):
        transport = build_transport({"type": "http", "url": "http://localhost:1/mcp"})
        self.assertIsInstance(transport, HTTPTransport)

    def test_missing_command_is_reported(self):
        with self.assertRaises(MCPError):
            build_transport({"type": "stdio"})

    def test_unknown_transport_is_reported(self):
        with self.assertRaises(MCPError):
            build_transport({"type": "carrier-pigeon"})


class ValidationTests(unittest.TestCase):
    def spec(self, **schema):
        return ToolSpec("t", "s", input_schema=schema)

    def test_enum_is_enforced(self):
        spec = self.spec(properties={"mode": {"enum": ["fast", "slow"]}})
        validate_arguments(spec, {"mode": "fast"})
        with self.assertRaises(MCPError):
            validate_arguments(spec, {"mode": "sideways"})

    def test_types_are_enforced(self):
        spec = self.spec(properties={"n": {"type": "number"}, "s": {"type": "string"}})
        validate_arguments(spec, {"n": 1.5, "s": "x"})
        with self.assertRaises(MCPError):
            validate_arguments(spec, {"n": "not a number"})

    def test_booleans_are_not_numbers(self):
        spec = self.spec(properties={"n": {"type": "integer"}})
        with self.assertRaises(MCPError) as ctx:
            validate_arguments(spec, {"n": True})
        self.assertIn("got boolean", str(ctx.exception))

    def test_unknown_keys_allowed_unless_forbidden(self):
        loose = self.spec(properties={"a": {"type": "string"}})
        validate_arguments(loose, {"a": "x", "extra": 1})

        strict = self.spec(properties={"a": {"type": "string"}}, additionalProperties=False)
        with self.assertRaises(MCPError):
            validate_arguments(strict, {"a": "x", "extra": 1})

    def test_a_tool_without_a_schema_accepts_anything(self):
        validate_arguments(ToolSpec("t", "s"), {"whatever": [1, 2]})


class ToolNodeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateway = MCPGateway()
        self.gateway.add_server("tools", StdioTransport(sys.executable, [SERVER]))
        register_gateway("test-gw", self.gateway)

    async def asyncTearDown(self):
        await self.gateway.close()

    async def test_a_workflow_calls_a_discovered_tool(self):
        graph = Graph(
            [
                NodeSpec("plan", "stub", {"output": {"question": "what is 2+3"}}),
                NodeSpec(
                    "compute",
                    "mcp_tool",
                    {
                        "gateway": "test-gw",
                        "tool": "add",
                        "arguments": {"a": 2, "b": 3},
                    },
                ),
                NodeSpec(
                    "report",
                    "stub",
                    {"output": {"answer": "{{plan.question}} = {{compute.text}}"}},
                ),
            ],
            [Edge("plan", "compute"), Edge("compute", "report")],
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs_of("compute")["tool"], "tools/add")
        self.assertEqual(result.outputs_of("report")["answer"], "what is 2+3 = 5")

    async def test_tool_arguments_are_resolved_from_upstream(self):
        graph = Graph(
            [
                NodeSpec("src", "stub", {"output": {"phrase": "from upstream"}}),
                NodeSpec(
                    "say",
                    "mcp_tool",
                    {
                        "gateway": "test-gw",
                        "tool": "echo",
                        "arguments": {"text": "{{src.phrase}}"},
                    },
                ),
            ],
            [Edge("src", "say")],
        )
        result = await WorkflowEngine(graph).run()
        self.assertEqual(result.outputs_of("say")["text"], "from upstream")

    async def test_an_unknown_gateway_fails_the_node(self):
        graph = Graph(
            [NodeSpec("x", "mcp_tool", {"gateway": "missing", "tool": "echo"})], []
        )
        result = await WorkflowEngine(graph).run()
        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIn("no MCP gateway registered", result.nodes["x"].error)

    async def test_a_failing_tool_can_use_the_error_strategy(self):
        graph = Graph.from_dict(
            {
                "nodes": [
                    {
                        "id": "boom",
                        "type": "mcp_tool",
                        "on_error": "default",
                        "error_output": {"text": "fallback"},
                        "config": {"gateway": "test-gw", "tool": "explode",
                                   "arguments": {}},
                    }
                ]
            }
        )
        result = await WorkflowEngine(graph).run()

        self.assertIs(result.status, RunStatus.COMPLETED)
        self.assertEqual(result.outputs_of("boom")["text"], "fallback")


if __name__ == "__main__":
    unittest.main()
