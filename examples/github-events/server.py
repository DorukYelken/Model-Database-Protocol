"""
MDBP GitHub Events — Live MCP Server

Connects to ClickHouse Playground's github_events table (10.6B rows of real GitHub data)
and exposes it as an MCP server. No API key, no signup, no setup.

Usage:
    pip install mdbp clickhouse-sqlalchemy clickhouse-driver
    python server.py

Then connect from Claude Desktop, Cursor, or any MCP client.

Example queries the LLM can make:
    - "Show me the most starred repos today"
    - "How many pull requests were opened this week?"
    - "Top 10 contributors to the Python language"
    - "Compare push events: JavaScript vs Rust this month"
"""

import sys
import os

# Debug: print where mdbp is loaded from
import mdbp as _mdbp_check
print(f"mdbp loaded from: {_mdbp_check.__file__}", file=sys.stderr)
print(f"mdbp version: {_mdbp_check.__version__}", file=sys.stderr)
print(f"cwd: {os.getcwd()}", file=sys.stderr)

from sqlalchemy import create_engine, text

from mdbp import MDBP
from mdbp.core.policy import Policy
from mdbp.core.schema_registry import EntitySchema, FieldSchema
from mdbp.core.audit import StreamAuditLogger
from mdbp.transport.server import run_sse, run_stdio

DB_URL = "clickhouse+native://explorer:@play.clickhouse.com:9440/default?secure=true"
TABLE_NAME = "github_events"


# ── Auto-discover fields via DESCRIBE TABLE ─────────────────

def discover_fields(db_url: str, table: str) -> dict[str, FieldSchema]:
    """Discover fields from ClickHouse using DESCRIBE TABLE (bypasses broken enum reflection)."""
    type_map = {
        "DateTime": "datetime", "Date": "datetime",
        "String": "text", "UUID": "text",
        "UInt8": "integer", "UInt16": "integer", "UInt32": "integer", "UInt64": "integer",
        "Int8": "integer", "Int16": "integer", "Int32": "integer", "Int64": "integer",
        "Float32": "numeric", "Float64": "numeric", "Decimal": "numeric",
    }

    engine = create_engine(db_url)
    with engine.connect() as conn:
        rows = conn.execute(text(f"DESCRIBE TABLE {table}"))
        fields = {}
        for row in rows:
            col_name = row[0]
            col_type = row[1]

            # Map ClickHouse type to MDBP dtype
            dtype = "text"
            for prefix, mapped in type_map.items():
                if col_type.startswith(prefix):
                    dtype = mapped
                    break
            # Enum, LowCardinality(String), Array(...) → text
            if "Enum" in col_type or "LowCardinality" in col_type:
                dtype = "text"

            is_array = col_type.startswith("Array")
            fields[col_name] = FieldSchema(
                column=col_name,
                dtype=dtype,
                filterable=not is_array,
                sortable=not is_array,
            )
    engine.dispose()
    return fields


# ── MDBP Setup ──────────────────────────────────────────────

fields = discover_fields(DB_URL, TABLE_NAME)

mdbp = MDBP(
    db_url=DB_URL,
    auto_discover=False,
    allowed_intents=["list", "get", "count", "aggregate"],
    audit=StreamAuditLogger(stream=sys.stderr),
)

mdbp.register_entity(EntitySchema(
    entity="github_event",
    table=TABLE_NAME,
    primary_key="file_time",
    description=(
        "Real-time GitHub events: pushes, PRs, issues, stars, forks, comments. "
        "10.6 billion rows from 2011 to now. "
        "IMPORTANT: Always add a created_at filter (e.g. created_at__gte: '2026-04-01') "
        "when using group_by to avoid memory limits. "
        "For aggregate sort, use 'result' as the sort field name."
    ),
    fields=fields,
))

# ── Policies ─────────────────────────────────────────────────

mdbp.add_policy(Policy(
    entity="github_event",
    role="*",
    max_rows=25,
    allowed_intents=["list", "count", "aggregate"],
))


# ── Run ──────────────────────────────────────────────────────

if __name__ == "__main__":
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"

    if transport == "sse":
        print("Starting GitHub Events MCP server on http://127.0.0.1:8000/sse")
        run_sse(mdbp, host="127.0.0.1", port=8000)
    else:
        run_stdio(mdbp)
