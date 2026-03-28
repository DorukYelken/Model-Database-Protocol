# E-Commerce MDBP Server

A complete example showing how to build an MDBP server for an e-commerce database. Demonstrates entity registration with LLM-friendly descriptions, role-based access control, tenant isolation, and all transport modes.

## Quick Start

```bash
pip install mdbp
python setup_db.py      # Create database with sample data
python server.py        # Start SSE server on :8000
```

## Database Schema

```
products          customers          orders              reviews
----------        ----------         ----------          ----------
id                id                 id                  id
name              name               customer_id → FK    product_id → FK
description       email (PII)        product_id → FK     customer_id → FK
price             phone (PII)        quantity            rating (1-5)
category          city               total               comment
stock             tier               status              created_at
created_at        created_at         created_at
```

12 products, 8 customers, 15 orders, 10 reviews.

## Roles & Policies

| Role | Orders | Customers | Products | Reviews | Max Rows |
|------|--------|-----------|----------|---------|----------|
| **customer** | Own orders only (`row_filter`) | Own profile only, no email/phone | Browse all | Read only | 50 |
| **support** | All orders | All, but `phone` hidden (`denied_fields`) | All | All | 200 |
| **admin** | All orders | All fields including phone | All | All | 1000 |

Write operations are globally blocked (`allowed_intents=["list", "get", "count", "aggregate"]`).

## Example Queries

**List all electronics under $100:**
```json
{
    "intent": "list",
    "entity": "product",
    "filters": {"category": "electronics", "price__lt": 100},
    "fields": ["name", "price", "stock"]
}
```

**Customer sees only their orders (tenant isolation):**
```json
{
    "intent": "list",
    "entity": "order",
    "role": "customer",
    "fields": ["id", "total", "status"]
}
```
Returns only orders where `customer_id = 1` (Alice).

**Support tries to access phone (denied):**
```json
{
    "intent": "list",
    "entity": "customer",
    "role": "support",
    "fields": ["name", "email", "phone"]
}
```
Returns error: `MDBP_POLICY_FIELD_DENIED`.

**Admin aggregates total revenue:**
```json
{
    "intent": "aggregate",
    "entity": "order",
    "role": "admin",
    "aggregation": {"op": "sum", "field": "total"},
    "filters": {"status": "delivered"}
}
```

**Average product rating:**
```json
{
    "intent": "aggregate",
    "entity": "review",
    "aggregation": {"op": "avg", "field": "rating"}
}
```

**Count orders by status (GROUP BY):**
```json
{
    "intent": "aggregate",
    "entity": "order",
    "role": "admin",
    "aggregation": {"op": "count", "field": "id"},
    "group_by": ["status"]
}
```

## Transport Modes

```bash
python server.py                                    # SSE (default) at :8000/sse
python server.py --transport stdio                   # stdin/stdout for Claude Desktop
python server.py --transport streamable-http         # Streamable HTTP at :8000/mcp
python server.py --transport websocket               # WebSocket at :8000/ws
python server.py --transport sse --port 9000         # Custom port
```

## Claude Desktop Integration

Add to `claude_desktop_config.json`:

```json
{
    "mcpServers": {
        "ecommerce": {
            "command": "python",
            "args": ["-u", "path/to/server.py", "--transport", "stdio"],
            "env": {
                "PYTHONPATH": "path/to/project"
            }
        }
    }
}
```

## Adapting for Your Database

1. Replace `setup_db.py` with your own database (or point `DATABASE_URL` to an existing one)
2. Edit the `EntitySchema` registrations in `server.py` to match your tables
3. Define `Policy` objects for your roles
4. Run `python server.py`

The minimum viable server is just 5 lines:

```python
from mdbp import MDBP
from mdbp.transport.server import run_sse

mdbp = MDBP(db_url="sqlite:///my.db")  # auto-discovers all tables
run_sse(mdbp, port=8000)
```
