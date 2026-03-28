"""
E-Commerce MDBP Server

Intent-based database access for an e-commerce database.
MDBP auto-discovers all tables and columns — no manual schema needed.
Just add policies and start the server.

Run:
  python server.py
  python server.py --transport stdio
  python server.py --transport streamable-http --port 9000
"""

import argparse
import os

from mdbp import MDBP
from mdbp.core.policy import Policy
from mdbp.transport.server import run_sse, run_stdio, run_streamable_http, run_websocket

_DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_URL = os.environ.get("DATABASE_URL", f"sqlite:///{os.path.join(_DB_DIR, 'ecommerce.db')}")

# ─── MDBP Instance ────────────────────────────────────────────
# Auto-discovers all tables/columns from the database.
# Read-only: write operations globally blocked.

mdbp = MDBP(
    db_url=DB_URL,
    allowed_intents=["list", "get", "count", "aggregate"],
)

# ─── Policies ──────────────────────────────────────────────────

# Customer role: tenant isolation — can only see their own data.
# In production, customer_id would come from auth session.
mdbp.add_policy(Policy(
    entity="order", role="customer",
    allowed_intents=["list", "get", "count"],
    row_filter={"customer_id": 1},
    max_rows=50,
))
mdbp.add_policy(Policy(
    entity="customer", role="customer",
    allowed_intents=["get"],
    allowed_fields=["id", "name", "city", "tier"],
    row_filter={"id": 1},
    max_rows=1,
))
mdbp.add_policy(Policy(
    entity="product", role="customer",
    allowed_intents=["list", "get", "count"],
    max_rows=50,
))
mdbp.add_policy(Policy(
    entity="review", role="customer",
    allowed_intents=["list", "count"],
    max_rows=50,
))

# Support role: broad read access, PII restricted.
mdbp.add_policy(Policy(
    entity="customer", role="support",
    allowed_intents=["list", "get", "count", "aggregate"],
    denied_fields=["phone"],
    max_rows=200,
))
mdbp.add_policy(Policy(
    entity="order", role="support",
    allowed_intents=["list", "get", "count", "aggregate"],
    max_rows=200,
))

# Admin role: full read access, higher limits.
mdbp.add_policy(Policy(
    entity="customer", role="admin",
    allowed_intents=["list", "get", "count", "aggregate"],
    max_rows=1000,
))
mdbp.add_policy(Policy(
    entity="order", role="admin",
    allowed_intents=["list", "get", "count", "aggregate"],
    max_rows=1000,
))

# ─── Server ───────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce MDBP Server")
    parser.add_argument("--transport", choices=["stdio", "sse", "streamable-http", "websocket"], default="sse")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    runners = {
        "stdio": lambda: run_stdio(mdbp),
        "sse": lambda: run_sse(mdbp, host=args.host, port=args.port),
        "streamable-http": lambda: run_streamable_http(mdbp, host=args.host, port=args.port),
        "websocket": lambda: run_websocket(mdbp, host=args.host, port=args.port),
    }
    runners[args.transport]()
