"""
MDBP MCP Server

Exposes MDBP as an MCP server so that Claude, Cursor, and other
MCP-compatible clients can use intent-based database access.

Tools exposed:
  - mdbp_query:          Execute an intent-based query
  - mdbp_describe_schema: Get available entities and fields

Transport modes:
  - stdio (default):      stdin/stdout for Claude Desktop, Cursor, etc.
  - sse:                  HTTP + Server-Sent Events at /sse
  - streamable-http:      HTTP + Streamable HTTP protocol at /mcp
  - websocket:            WebSocket at /ws

Run:
  mdbp-server --db-url sqlite:///my.db
  mdbp-server --db-url sqlite:///my.db --transport sse --port 8000
  mdbp-server --db-url sqlite:///my.db --transport streamable-http --port 8000
  mdbp-server --db-url sqlite:///my.db --transport websocket --port 8000
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from mdbp.core.intent import IntentType
from mdbp.core.masking import MaskingRule
from mdbp.core.policy import Policy
from mdbp.core.schema_registry import EntitySchema, FieldSchema
from mdbp.mdbp import MDBP


def load_config(config_path: str) -> dict:
    """Load MDBP schema + policy config from a JSON file."""
    path = Path(config_path)
    if not path.exists():
        from mdbp.core.errors import ConfigFileNotFoundError
        raise ConfigFileNotFoundError(path=config_path)
    with open(path) as f:
        return json.load(f)


def build_mdbp_from_config(db_url: str, config: dict) -> MDBP:
    """Create an MDBP instance from a config dict."""
    mdbp = MDBP(db_url=db_url)

    # Register entities
    for entity_conf in config.get("entities", []):
        fields = {
            fname: FieldSchema(**fdef)
            for fname, fdef in entity_conf["fields"].items()
        }
        schema = EntitySchema(
            entity=entity_conf["entity"],
            table=entity_conf["table"],
            primary_key=entity_conf.get("primary_key", "id"),
            fields=fields,
            description=entity_conf.get("description"),
        )
        mdbp.register_entity(schema)

    # Register policies
    for policy_conf in config.get("policies", []):
        if "allowed_intents" in policy_conf:
            policy_conf["allowed_intents"] = [
                IntentType(i) for i in policy_conf["allowed_intents"]
            ]
        if "masked_fields" in policy_conf:
            policy_conf["masked_fields"] = {
                field: MaskingRule(**rule) if isinstance(rule, dict) else rule
                for field, rule in policy_conf["masked_fields"].items()
            }
        mdbp.add_policy(Policy(**policy_conf))

    return mdbp


def create_server(mdbp: MDBP) -> Server:
    """Create an MCP server with MDBP tools."""
    server = Server("mdbp")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="mdbp_query",
                description=(
                    "Execute an intent-based database query via MDBP. "
                    "Instead of writing SQL, provide a structured intent with "
                    "entity name, operation type, and filters."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "intent": {
                            "type": "string",
                            "enum": ["list", "get", "count", "aggregate", "create", "batch_create", "upsert", "update", "delete", "union", "intersect", "except"],
                            "description": "The operation type",
                        },
                        "entity": {
                            "type": "string",
                            "description": "The logical entity name",
                        },
                        "filters": {
                            "type": "object",
                            "description": "Key-value filters. Supports suffixes: __gt, __gte, __lt, __lte, __ne, __like, __ilike, __not_like, __in, __not_in, __between, __null, __not_null",
                            "default": {},
                        },
                        "fields": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Fields to return (omit for all allowed fields)",
                        },
                        "sort": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                    "order": {"type": "string", "enum": ["asc", "desc"]},
                                },
                            },
                        },
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                        "id": {"description": "Primary key value for 'get' intent"},
                        "aggregation": {
                            "type": "object",
                            "properties": {
                                "op": {"type": "string", "enum": ["sum", "avg", "min", "max", "count"]},
                                "field": {"type": "string"},
                            },
                        },
                        "group_by": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "data": {
                            "type": "object",
                            "description": "Data payload for create/update intents",
                        },
                    },
                    "required": ["intent", "entity"],
                },
            ),
            Tool(
                name="mdbp_describe_schema",
                description=(
                    "Get the available entities, their fields, types, and descriptions. "
                    "Use this to understand what data you can query."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        if name == "mdbp_query":
            result = mdbp.query(arguments)
            return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

        elif name == "mdbp_describe_schema":
            schema = mdbp.describe_schema()
            return [TextContent(type="text", text=json.dumps(schema, indent=2))]

        raise ValueError(f"Unknown tool: {name}")

    return server


# ─── Transport: SSE ────────────────────────────────────────────


def sse_app(
    mdbp: MDBP,
    sse_path: str = "/sse",
    message_path: str = "/messages/",
):
    """Create a Starlette ASGI app that serves MDBP over SSE.

        app = sse_app(mdbp)
        uvicorn.run(app, host="0.0.0.0", port=8000)
    """
    from mcp.server.sse import SseServerTransport
    from starlette.applications import Starlette
    from starlette.responses import Response
    from starlette.routing import Mount, Route

    server = create_server(mdbp)
    sse = SseServerTransport(message_path)

    async def handle_sse(request):
        try:
            async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                await server.run(streams[0], streams[1], server.create_initialization_options())
        except Exception:
            pass
        return Response()

    return Starlette(
        routes=[
            Route(sse_path, endpoint=handle_sse),
            Mount(message_path, app=sse.handle_post_message),
        ],
    )


def run_sse(
    mdbp: MDBP,
    host: str = "127.0.0.1",
    port: int = 8000,
    **kwargs,
) -> None:
    """Start MDBP as an HTTP server with SSE transport.

        from mdbp import MDBP
        from mdbp.transport.server import run_sse

        mdbp = MDBP(db_url="sqlite:///my.db")
        run_sse(mdbp, host="0.0.0.0", port=8000)
    """
    import uvicorn

    app = sse_app(mdbp)
    uvicorn.run(app, host=host, port=port, **kwargs)


# ─── Transport: Streamable HTTP ───────────────────────────────


def streamable_http_app(
    mdbp: MDBP,
    path: str = "/mcp",
    stateless: bool = True,
):
    """Create a Starlette ASGI app that serves MDBP over Streamable HTTP.

        app = streamable_http_app(mdbp)
        uvicorn.run(app, host="0.0.0.0", port=8000)
    """
    import contextlib

    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Mount

    server = create_server(mdbp)
    session_manager = StreamableHTTPSessionManager(
        app=server, stateless=stateless,
    )

    @contextlib.asynccontextmanager
    async def lifespan(app):
        async with session_manager.run():
            yield

    return Starlette(
        routes=[
            Mount(path, app=session_manager.handle_request),
        ],
        lifespan=lifespan,
    )


def run_streamable_http(
    mdbp: MDBP,
    host: str = "127.0.0.1",
    port: int = 8000,
    **kwargs,
) -> None:
    """Start MDBP as an HTTP server with Streamable HTTP transport.

        from mdbp import MDBP
        from mdbp.transport.server import run_streamable_http

        mdbp = MDBP(db_url="sqlite:///my.db")
        run_streamable_http(mdbp, host="0.0.0.0", port=8000)
    """
    import uvicorn

    app = streamable_http_app(mdbp)
    uvicorn.run(app, host=host, port=port, **kwargs)


# ─── Transport: WebSocket ──────────────────────────────────────


def websocket_app(
    mdbp: MDBP,
    ws_path: str = "/ws",
):
    """Create a Starlette ASGI app that serves MDBP over WebSocket.

        app = websocket_app(mdbp)
        uvicorn.run(app, host="0.0.0.0", port=8000)
    """
    from mcp.server.websocket import websocket_server
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute

    server = create_server(mdbp)

    async def handle_ws(websocket):
        await websocket.accept(subprotocol="mcp")
        async with websocket_server(
            websocket.scope, websocket.receive, websocket.send,
        ) as streams:
            await server.run(streams[0], streams[1], server.create_initialization_options())

    return Starlette(
        routes=[
            WebSocketRoute(ws_path, endpoint=handle_ws),
        ],
    )


def run_websocket(
    mdbp: MDBP,
    host: str = "127.0.0.1",
    port: int = 8000,
    **kwargs,
) -> None:
    """Start MDBP as an HTTP server with WebSocket transport.

        from mdbp import MDBP
        from mdbp.transport.server import run_websocket

        mdbp = MDBP(db_url="sqlite:///my.db")
        run_websocket(mdbp, host="0.0.0.0", port=8000)
    """
    import uvicorn

    app = websocket_app(mdbp)
    uvicorn.run(app, host=host, port=port, **kwargs)


# ─── Transport: Stdio ─────────────────────────────────────────


def run_stdio(mdbp: MDBP) -> None:
    """Start MDBP with stdio transport (for Claude Desktop, Cursor, etc.).

        from mdbp import MDBP
        from mdbp.transport.server import run_stdio

        mdbp = MDBP(db_url="sqlite:///my.db")
        run_stdio(mdbp)
    """
    import asyncio

    server = create_server(mdbp)

    async def _run():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    asyncio.run(_run())


# ─── CLI ───────────────────────────────────────────────────────


_TRANSPORTS = ["stdio", "sse", "streamable-http", "websocket"]


def main() -> None:
    parser = argparse.ArgumentParser(description="MDBP MCP Server")
    parser.add_argument("--db-url", required=True, help="SQLAlchemy database URL")
    parser.add_argument("--config", required=False, help="Path to MDBP config JSON file")
    parser.add_argument(
        "--transport", choices=_TRANSPORTS, default="stdio",
        help="Transport mode (default: stdio)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host for HTTP servers (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port for HTTP servers (default: 8000)")
    args = parser.parse_args()

    config = load_config(args.config) if args.config else {"entities": [], "policies": []}
    mdbp = build_mdbp_from_config(args.db_url, config)

    if args.transport == "stdio":
        run_stdio(mdbp)
    elif args.transport == "sse":
        run_sse(mdbp, host=args.host, port=args.port)
    elif args.transport == "streamable-http":
        run_streamable_http(mdbp, host=args.host, port=args.port)
    elif args.transport == "websocket":
        run_websocket(mdbp, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
