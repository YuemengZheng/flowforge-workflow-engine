"""MCP client and tool gateway.

The Model Context Protocol is JSON-RPC 2.0 with a fixed handshake: ``initialize``
then ``notifications/initialized``, after which ``tools/list`` reports what the
server can do and ``tools/call`` invokes it.

The point of a *gateway* rather than a client is discovery. Tools are not
declared in the workflow JSON — the gateway connects to its servers, asks each
what it offers, and builds the catalogue at runtime. A workflow node names a
tool; if two servers export the same name the qualified ``server/tool`` form
disambiguates. Adding a tool to a server makes it callable without touching any
workflow.

Transports: stdio (spawn a subprocess and talk over its pipes) and HTTP. Both
are stdlib; the HTTP one uses the pooled client in ``pool.py`` so a burst of
tool calls does not open a connection each.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

from .errors import FlowForgeError

PROTOCOL_VERSION = "2025-06-18"
CLIENT_INFO = {"name": "flowforge", "version": "0.5.0"}


class MCPError(FlowForgeError):
    """The MCP server returned an error, or the transport broke."""


class MCPToolError(MCPError):
    """A tool ran and reported failure (``isError`` on the result)."""


@dataclass(frozen=True)
class ToolSpec:
    """One tool as advertised by a server."""

    name: str
    server: str
    description: str = ""
    input_schema: Mapping[str, Any] = field(default_factory=dict)

    @property
    def qualified(self) -> str:
        return f"{self.server}/{self.name}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "server": self.server,
            "description": self.description,
            "input_schema": dict(self.input_schema),
        }


class Transport(Protocol):
    """Carries one JSON-RPC request and returns the decoded response."""

    async def request(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None: ...
    async def notify(self, payload: Mapping[str, Any]) -> None: ...
    async def close(self) -> None: ...


class StdioTransport:
    """Spawns the server and speaks newline-delimited JSON over its pipes."""

    def __init__(
        self,
        command: str,
        args: Sequence[str] = (),
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
    ) -> None:
        self.command = command
        self.args = list(args)
        self.env = dict(env) if env else None
        self.cwd = cwd
        self._process: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()

    async def _ensure(self) -> asyncio.subprocess.Process:
        if self._process is None or self._process.returncode is not None:
            self._process = await asyncio.create_subprocess_exec(
                self.command,
                *self.args,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env={**os.environ, **self.env} if self.env else None,
                cwd=self.cwd,
            )
        return self._process

    async def _send(self, payload: Mapping[str, Any]) -> asyncio.subprocess.Process:
        process = await self._ensure()
        assert process.stdin is not None
        process.stdin.write(json.dumps(payload).encode("utf-8") + b"\n")
        await process.stdin.drain()
        return process

    async def request(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        async with self._lock:
            process = await self._send(payload)
            assert process.stdout is not None
            while True:
                line = await process.stdout.readline()
                if not line:
                    raise MCPError(f"{self.command}: server closed its output")
                text = line.strip()
                if not text:
                    continue
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    continue  # servers are entitled to log on stdout
                # Skip anything that is not the answer to this request.
                if isinstance(message, Mapping) and message.get("id") == payload.get("id"):
                    return message

    async def notify(self, payload: Mapping[str, Any]) -> None:
        async with self._lock:
            await self._send(payload)

    async def close(self) -> None:
        if self._process is not None and self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except asyncio.TimeoutError:  # pragma: no cover - stubborn child
                self._process.kill()
        self._process = None


class HTTPTransport:
    """POSTs JSON-RPC to a streamable-HTTP MCP endpoint over a pooled connection."""

    def __init__(self, url: str, headers: Mapping[str, str] | None = None,
                 pool: Any = None) -> None:
        from .pool import HTTPConnectionPool

        self.url = url
        self.headers = dict(headers or {})
        self.pool = pool or HTTPConnectionPool()

    async def request(self, payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
        status, body = await self.pool.post_json(self.url, payload, self.headers)
        if status >= 400:
            raise MCPError(f"{self.url} returned HTTP {status}: {body[:200]}")
        if not body.strip():
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise MCPError(f"{self.url} returned non-JSON: {exc}") from None

    async def notify(self, payload: Mapping[str, Any]) -> None:
        await self.request(payload)

    async def close(self) -> None:
        await self.pool.close()


class MCPClient:
    """One MCP server: handshake once, then list and call its tools."""

    def __init__(self, name: str, transport: Transport) -> None:
        self.name = name
        self.transport = transport
        self._next_id = 0
        self._initialized = False
        self._handshake_lock = asyncio.Lock()

    def _rpc_id(self) -> int:
        self._next_id += 1
        return self._next_id

    async def _call(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": self._rpc_id(),
            "method": method,
            "params": dict(params or {}),
        }
        response = await self.transport.request(payload)
        if response is None:
            raise MCPError(f"{self.name}: no response to {method!r}")
        if "error" in response:
            error = response["error"] or {}
            raise MCPError(
                f"{self.name}: {method} failed "
                f"({error.get('code', '?')}: {error.get('message', 'unknown')})"
            )
        return response.get("result", {})

    async def initialize(self) -> dict[str, Any]:
        """Handshake. Idempotent — repeated calls are a no-op, not a second one."""
        async with self._handshake_lock:
            if self._initialized:
                return {}
            result = await self._call(
                "initialize",
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "clientInfo": CLIENT_INFO,
                },
            )
            await self.transport.notify(
                {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
            )
            self._initialized = True
            return dict(result)

    async def list_tools(self) -> list[ToolSpec]:
        """Ask the server what it can do. Follows ``nextCursor`` pagination."""
        await self.initialize()
        tools: list[ToolSpec] = []
        cursor: str | None = None
        while True:
            params = {"cursor": cursor} if cursor else {}
            result = await self._call("tools/list", params)
            for raw in result.get("tools", []) or []:
                tools.append(
                    ToolSpec(
                        name=str(raw.get("name", "")),
                        server=self.name,
                        description=str(raw.get("description", "")),
                        input_schema=raw.get("inputSchema") or {},
                    )
                )
            cursor = result.get("nextCursor")
            if not cursor:
                return tools

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        await self.initialize()
        result = await self._call(
            "tools/call", {"name": name, "arguments": dict(arguments)}
        )
        content = result.get("content") or []
        text = "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, Mapping) and block.get("type") == "text"
        )
        if result.get("isError"):
            raise MCPToolError(f"{self.name}/{name}: {text or 'tool reported an error'}")
        return {
            "text": text,
            "content": content,
            "structured": result.get("structuredContent"),
        }

    async def close(self) -> None:
        await self.transport.close()


def build_transport(config: Mapping[str, Any]) -> Transport:
    """``{"type": "stdio", "command": ...}`` or ``{"type": "http", "url": ...}``."""
    kind = str(config.get("type", "stdio")).lower()
    if kind == "stdio":
        command = config.get("command")
        if not command:
            raise MCPError("stdio server needs a 'command'")
        return StdioTransport(
            str(command),
            config.get("args", []),
            config.get("env"),
            config.get("cwd"),
        )
    if kind in ("http", "url"):
        url = config.get("url")
        if not url:
            raise MCPError("http server needs a 'url'")
        return HTTPTransport(str(url), config.get("headers"))
    raise MCPError(f"unknown MCP transport {kind!r}; expected 'stdio' or 'http'")


class MCPGateway:
    """Several MCP servers behind one catalogue, discovered at runtime.

    ``discover()`` is what makes this a gateway rather than a client: nothing
    about the tools is written down in advance. Names collide across servers
    often enough that the qualified ``server/tool`` form is always available and
    the bare name resolves only when it is unambiguous — a silent wrong-server
    call is worse than an error.
    """

    def __init__(self, clients: Mapping[str, MCPClient] | None = None) -> None:
        self.clients: dict[str, MCPClient] = dict(clients or {})
        self._catalogue: dict[str, ToolSpec] = {}
        self._ambiguous: set[str] = set()
        self._discovered = False

    @classmethod
    def from_config(cls, servers: Mapping[str, Mapping[str, Any]]) -> "MCPGateway":
        return cls(
            {
                name: MCPClient(name, build_transport(config))
                for name, config in servers.items()
            }
        )

    def add_server(self, name: str, transport: Transport) -> None:
        self.clients[name] = MCPClient(name, transport)
        self._discovered = False

    async def discover(self, refresh: bool = False) -> list[ToolSpec]:
        """Ask every server for its tools and rebuild the catalogue."""
        if self._discovered and not refresh:
            return self.tools
        catalogue: dict[str, ToolSpec] = {}
        ambiguous: set[str] = set()
        listings = await asyncio.gather(
            *(client.list_tools() for client in self.clients.values()),
            return_exceptions=True,
        )
        errors: list[str] = []
        for name, listing in zip(self.clients, listings):
            if isinstance(listing, BaseException):
                errors.append(f"{name}: {listing}")
                continue
            for tool in listing:
                catalogue[tool.qualified] = tool
                if tool.name in catalogue and catalogue[tool.name].server != tool.server:
                    ambiguous.add(tool.name)
                else:
                    catalogue[tool.name] = tool
        if errors and not catalogue:
            raise MCPError("no MCP server could be reached: " + "; ".join(errors))
        for name in ambiguous:
            catalogue.pop(name, None)
        self._catalogue = catalogue
        self._ambiguous = ambiguous
        self._discovered = True
        return self.tools

    @property
    def tools(self) -> list[ToolSpec]:
        """Distinct tools, sorted — the qualified aliases are not repeated."""
        seen: dict[str, ToolSpec] = {t.qualified: t for t in self._catalogue.values()}
        return sorted(seen.values(), key=lambda t: t.qualified)

    def resolve(self, name: str) -> ToolSpec:
        if name in self._ambiguous:
            owners = sorted(
                t.server for t in self._catalogue.values() if t.name == name
            )
            raise MCPError(
                f"tool {name!r} is exported by more than one server; "
                f"qualify it as {'/'.join((owners[0], name)) if owners else 'server/tool'}"
            )
        try:
            return self._catalogue[name]
        except KeyError:
            known = ", ".join(t.qualified for t in self.tools) or "<none discovered>"
            raise MCPError(f"unknown tool {name!r}; discovered: {known}") from None

    async def call(self, name: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        await self.discover()
        tool = self.resolve(name)
        validate_arguments(tool, arguments)
        return await self.clients[tool.server].call_tool(tool.name, arguments)

    async def close(self) -> None:
        await asyncio.gather(
            *(client.close() for client in self.clients.values()),
            return_exceptions=True,
        )


_GATEWAYS: dict[str, MCPGateway] = {}


def register_gateway(name: str, gateway: MCPGateway) -> None:
    """Make a gateway reachable from workflow JSON by name."""
    _GATEWAYS[name] = gateway


def get_gateway(name: str) -> MCPGateway:
    try:
        return _GATEWAYS[name]
    except KeyError:
        raise MCPError(
            f"no MCP gateway registered as {name!r}; "
            f"registered: {', '.join(sorted(_GATEWAYS)) or '<none>'}"
        ) from None


def _register_tool_node() -> None:
    """Registered lazily so importing mcp.py does not require the node layer."""
    from .events import NODE_DELTA
    from .nodes import Node, NodeContext, registry

    @registry.register("mcp_tool")
    class MCPToolNode(Node):
        """Calls a tool discovered on an MCP server.

        config::

            {"gateway": "default", "tool": "search/web_search",
             "arguments": {"query": "{{plan.question}}"}}

        The tool is looked up in the runtime catalogue, its arguments are
        validated against the schema the server advertised, and the result comes
        back as ``text`` plus the raw ``content`` blocks. Timeouts and retries
        are the node's ``timeout`` / ``retries``, same as every other node.
        """

        async def run(self, ctx: NodeContext) -> Mapping[str, Any]:
            gateway = get_gateway(str(ctx.config.get("gateway", "default")))
            tool = ctx.config.get("tool")
            if not isinstance(tool, str) or not tool:
                raise MCPError(f"node {ctx.node_id!r}: 'tool' must name a tool")
            arguments = ctx.config.get("arguments", {})
            if not isinstance(arguments, Mapping):
                raise MCPError(f"node {ctx.node_id!r}: 'arguments' must be an object")

            await ctx.emit(NODE_DELTA, tool=tool, phase="calling")
            result = await gateway.call(tool, arguments)
            spec = gateway.resolve(tool)
            return {
                "text": result["text"],
                "content": result["content"],
                "structured": result["structured"],
                "tool": spec.qualified,
            }


_register_tool_node()


# ------------------------------------------------------------------ validation

_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "number": (int, float),
    "integer": (int,),
    "boolean": (bool,),
    "array": (list, tuple),
    "object": (dict,),
}


def validate_arguments(tool: ToolSpec, arguments: Mapping[str, Any]) -> None:
    """Check arguments against the tool's advertised JSON Schema.

    Deliberately shallow — required keys, declared types, enums, and unknown
    keys when the schema forbids them. Catching a typo or a missing field before
    the call is most of the value; full JSON Schema is a library's job.
    """
    schema = tool.input_schema or {}
    properties = schema.get("properties") or {}
    required = schema.get("required") or []

    missing = [key for key in required if key not in arguments]
    if missing:
        raise MCPError(
            f"{tool.qualified}: missing required argument(s) {', '.join(sorted(missing))}"
        )

    if schema.get("additionalProperties") is False:
        unknown = [key for key in arguments if key not in properties]
        if unknown:
            raise MCPError(
                f"{tool.qualified}: unknown argument(s) {', '.join(sorted(unknown))}; "
                f"accepted: {', '.join(sorted(properties)) or '<none>'}"
            )

    for key, value in arguments.items():
        spec = properties.get(key)
        if not isinstance(spec, Mapping):
            continue
        declared = spec.get("type")
        if isinstance(declared, str) and declared in _JSON_TYPES:
            # bool is an int in Python; "number" must not silently accept True.
            if declared in ("number", "integer") and isinstance(value, bool):
                raise MCPError(
                    f"{tool.qualified}: argument {key!r} expects {declared}, got boolean"
                )
            if not isinstance(value, _JSON_TYPES[declared]):
                raise MCPError(
                    f"{tool.qualified}: argument {key!r} expects {declared}, got "
                    f"{type(value).__name__}"
                )
        allowed = spec.get("enum")
        if isinstance(allowed, list) and value not in allowed:
            raise MCPError(
                f"{tool.qualified}: argument {key!r} must be one of "
                f"{', '.join(map(repr, allowed))}, got {value!r}"
            )
