#!/usr/bin/env python3
"""A real MCP server over stdio, used to test the client against the protocol.

Implements the handshake, paginated ``tools/list`` and ``tools/call``. Small
enough to read, real enough that the client is exercised end to end rather than
against a mock that agrees with whatever the client happens to send.

Env knobs used by the tests:
    MCP_PAGE_SIZE   force pagination by capping tools per tools/list page
    MCP_NOISE       print a non-JSON line before replying, as chatty servers do
    MCP_SERVER_NAME name reported in serverInfo
"""

import json
import os
import sys

TOOLS = [
    {
        "name": "echo",
        "description": "Echo the text back.",
        "inputSchema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "additionalProperties": False,
        },
    },
    {
        "name": "add",
        "description": "Add two integers.",
        "inputSchema": {
            "type": "object",
            "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
            "required": ["a", "b"],
            "additionalProperties": False,
        },
    },
    {
        "name": "explode",
        "description": "Always reports a tool error.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


def call(name, arguments):
    if name == "echo":
        return {"content": [{"type": "text", "text": arguments.get("text", "")}]}
    if name == "add":
        total = arguments["a"] + arguments["b"]
        return {
            "content": [{"type": "text", "text": str(total)}],
            "structuredContent": {"sum": total},
        }
    if name == "explode":
        return {
            "content": [{"type": "text", "text": "detonated on purpose"}],
            "isError": True,
        }
    return {
        "content": [{"type": "text", "text": f"no such tool: {name}"}],
        "isError": True,
    }


def handle(message):
    method = message.get("method")
    params = message.get("params") or {}

    if method == "initialize":
        return {
            "protocolVersion": params.get("protocolVersion", "2025-06-18"),
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {
                "name": os.environ.get("MCP_SERVER_NAME", "echo-server"),
                "version": "1.0.0",
            },
        }

    if method == "tools/list":
        page_size = int(os.environ.get("MCP_PAGE_SIZE", "0") or 0)
        if not page_size:
            return {"tools": TOOLS}
        start = int(params.get("cursor") or 0)
        page = TOOLS[start : start + page_size]
        result = {"tools": page}
        if start + page_size < len(TOOLS):
            result["nextCursor"] = str(start + page_size)
        return result

    if method == "tools/call":
        return call(params.get("name"), params.get("arguments") or {})

    raise LookupError(method)


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        message = json.loads(line)
        if "id" not in message:  # a notification; nothing to answer
            continue
        if os.environ.get("MCP_NOISE"):
            print("server: handling a request", flush=True)
        try:
            response = {"jsonrpc": "2.0", "id": message["id"], "result": handle(message)}
        except LookupError as exc:
            response = {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32601, "message": f"method not found: {exc}"},
            }
        print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
