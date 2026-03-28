"""
MDBP MCP Server

Exposes MDBP as an MCP server so that Claude, Cursor, and other
MCP-compatible clients can use intent-based database access.

Tools exposed:
  - mdbp_query:          Execute an intent-based query
  - mdbp_describe_schema: Get available entities and fields

Run:
  python -m mdbp.transport.server --db-url sqlite:///my.db --schema schema.json
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


def build_mdcp_from_config(db_url: str, config: dict) -> MDBP:
    """Create an MDBP instance from a config dict."""
    mdcp = MDBP(db_url=db_url)

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
        mdbp.add_policy(Policy(**policy_conf))

    return mdcp


def create_server(mdcp: MDBP) -> Server:
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
                            "enum": ["list", "get", "count", "aggregate", "create", "update", "delete"],
                            "description": "The operation type",
                        },
                        "entity": {
                            "type": "string",
                            "description": "The logical entity name",
                        },
                        "filters": {
                            "type": "object",
                            "description": "Key-value filters. Supports suffixes: __gt, __gte, __lt, __lte, __ne, __like, __in",
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


async def run(db_url: str, config_path: str | None = None) -> None:
    config = load_config(config_path) if config_path else {"entities": [], "policies": []}
    mdcp = build_mdcp_from_config(db_url, config)
    server = create_server(mdcp)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main() -> None:
    parser = argparse.ArgumentParser(description="MDBP MCP Server")
    parser.add_argument("--db-url", required=True, help="SQLAlchemy database URL")
    parser.add_argument("--config", required=False, help="Path to MDBP config JSON file")
    args = parser.parse_args()

    import asyncio
    asyncio.run(run(args.db_url, args.config))


if __name__ == "__main__":
    main()
