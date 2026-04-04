# MDBP - Model Database Protocol

**Intent-based data access protocol for AI systems.**

MDBP enables secure database access for LLMs. Instead of generating raw SQL, LLMs produce structured **intent** objects. MDBP validates these intents against a schema registry, enforces access policies, builds parameterized queries via SQLAlchemy, and returns LLM-friendly responses.

```
LLM Intent (JSON) -> Schema Validation -> Policy Check -> SQLAlchemy Query -> Response
```

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Intent Types](#intent-types)
- [Filtering](#filtering)
- [JOIN Operations](#join-operations)
- [Aggregation](#aggregation)
- [Computed Fields](#computed-fields)
- [CTE (Common Table Expressions)](#cte-common-table-expressions)
- [Write Operations](#write-operations)
- [Set Operations](#set-operations)
- [Schema Registry](#schema-registry)
- [Policy Engine](#policy-engine)
- [Data Masking](#data-masking)
- [Dry-Run Mode](#dry-run-mode)
- [MCP Server](#mcp-server)
- [Error Handling](#error-handling)
- [API Reference](#api-reference)
- [Security](#security)

---

## Installation

```bash
pip install mdbp
```

For development:
```bash
pip install mdbp[dev]
```

**Requirements:**
- Python >= 3.10
- SQLAlchemy >= 2.0
- Pydantic >= 2.0
- mcp >= 1.0

**Supported Databases:**
Any SQLAlchemy-supported backend: PostgreSQL, MySQL, SQLite, MSSQL, Oracle, etc.

---

## Quick Start

### Up and Running in 3 Lines

```python
from mdbp import MDBP

mdbp = MDBP(db_url="sqlite:///my.db")
result = mdbp.query({"intent": "list", "entity": "product", "limit": 10})
```

When `MDBP(db_url=...)` is called, all tables and columns are automatically discovered from the database. No manual registration required.

### Example Output

```json
{
    "success": true,
    "intent": "list",
    "entity": "product",
    "summary": "10 product(s) found",
    "data": [
        {"id": 1, "name": "Laptop", "price": 15000},
        {"id": 2, "name": "Mouse", "price": 250}
    ]
}
```

### Error Response

```json
{
    "success": false,
    "intent": "list",
    "entity": "spaceship",
    "error": {
        "code": "MDBP_SCHEMA_ENTITY_NOT_FOUND",
        "message": "Entity 'spaceship' not found in schema registry.",
        "details": {
            "entity": "spaceship",
            "available_entities": ["product", "order", "customer"]
        }
    }
}
```

When an LLM hallucinates a table name, MDBP catches it and returns the list of available entities. The LLM can self-correct using this feedback.

### Real-World Example (PostgreSQL)

```python
from mdbp import MDBP

mdbp = MDBP(
    db_url="postgresql+psycopg2://user:password@localhost:5432/mydb",
    allowed_intents=["list", "get", "count", "aggregate"],  # read-only mode
)

# Auto-discovers all tables and columns
schema = mdbp.describe_schema()
for entity, info in schema.items():
    print(f"{entity}: {len(info['fields'])} fields")

# List with sorting and limit
result = mdbp.query({
    "intent": "list",
    "entity": "stock_price",
    "fields": ["Date", "Close", "Volume"],
    "sort": [{"field": "Date", "order": "desc"}],
    "limit": 5,
})
for row in result["data"]:
    print(f"{row['Date']} | ${row['Close']:.2f} | Vol: {row['Volume']:,}")

# Aggregation
result = mdbp.query({
    "intent": "aggregate",
    "entity": "stock_price",
    "aggregation": {"op": "avg", "field": "Close"},
})
print(f"Average close: ${float(result['data'][0]['result']):.2f}")

# Count with filters
result = mdbp.query({
    "intent": "count",
    "entity": "stock_price",
    "filters": {"Close__gte": 100},
})
print(f"Days above $100: {result['data']['count']}")

# Hallucination protection
result = mdbp.query({"intent": "list", "entity": "nonexistent_table"})
print(result["error"]["code"])           # MDBP_SCHEMA_ENTITY_NOT_FOUND
print(result["error"]["details"])        # {"available_entities": [...]}

mdbp.dispose()
```

---

## Core Concepts

### What is an Intent?

An intent is a structured JSON object that describes a database operation. Every intent contains these core fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `intent` | string | Yes | Operation type: list, get, count, aggregate, create, update, delete |
| `entity` | string | Yes | Target table/entity name |
| `filters` | object | No | Filter conditions |
| `fields` | array | No | Fields to return (empty = all) |
| `sort` | array | No | Ordering |
| `limit` | integer | No | Result limit |
| `offset` | integer | No | Pagination offset |

### Pipeline

Every `mdbp.query()` call passes through these stages:

```
1. Parse       -> Convert dict to Intent model (Pydantic validation)
2. Whitelist   -> Check allowed_intents (global restriction)
3. Schema      -> Verify entity and fields exist in schema registry
4. Policy      -> Role-based access control, field restrictions
5. Plan        -> Convert Intent to SQLAlchemy statement
6. [Dry-run?]  -> Return compiled SQL without executing (if enabled)
7. Execute     -> Run parameterized query
8. Mask        -> Apply data masking to result fields (if configured)
9. Format      -> Convert result to LLM-friendly JSON
```

---

## Intent Types

### list - List Records

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "fields": ["name", "price"],
    "filters": {"price__gte": 100},
    "sort": [{"field": "price", "order": "desc"}],
    "limit": 10,
    "offset": 0,
    "distinct": True
})
```

### get - Get Single Record

```python
mdbp.query({
    "intent": "get",
    "entity": "product",
    "id": 42
})
```

Returns a single record by primary key. Returns `MDBP_NOT_FOUND` error if no record exists.

### count - Count Records

```python
mdbp.query({
    "intent": "count",
    "entity": "product",
    "filters": {"category": "electronics"}
})
```

Output:
```json
{"success": true, "data": {"count": 156}}
```

### aggregate - Aggregate

```python
mdbp.query({
    "intent": "aggregate",
    "entity": "order",
    "aggregation": {"op": "sum", "field": "amount"}
})
```

Supported operations: `sum`, `avg`, `min`, `max`, `count`

Multiple aggregations:

```python
mdbp.query({
    "intent": "aggregate",
    "entity": "order",
    "aggregations": [
        {"op": "count", "field": "id"},
        {"op": "sum", "field": "amount"},
        {"op": "avg", "field": "amount"}
    ],
    "group_by": ["status"]
})
```

### create - Create Record

```python
mdbp.query({
    "intent": "create",
    "entity": "product",
    "data": {"name": "Laptop", "price": 999.99},
    "returning": ["id", "name"]
})
```

### update - Update Record

```python
mdbp.query({
    "intent": "update",
    "entity": "product",
    "id": 5,
    "data": {"price": 899.99}
})
```

Bulk update with filters:

```python
mdbp.query({
    "intent": "update",
    "entity": "product",
    "filters": {"status": "draft"},
    "data": {"status": "published"}
})
```

### delete - Delete Record

```python
mdbp.query({
    "intent": "delete",
    "entity": "product",
    "id": 5
})
```

---

## Filtering

### Simple Filters (Operator Suffix)

Append a suffix to the field name in the `filters` dict to specify the operator:

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "filters": {
        "category": "electronics",       # equality (=)
        "price__gt": 100,                # greater than (>)
        "price__lte": 5000,              # less than or equal (<=)
        "name__like": "%laptop%",        # LIKE
        "status__ne": "deleted",          # not equal (!=)
        "color__in": ["red", "blue"],     # IN (...)
        "stock__not_null": True,          # IS NOT NULL
    }
})
```

**All Operators:**

| Suffix | SQL Equivalent | Example |
|--------|---------------|---------|
| *(none)* | `=` | `{"city": "Istanbul"}` |
| `__gt` | `>` | `{"price__gt": 100}` |
| `__gte` | `>=` | `{"price__gte": 100}` |
| `__lt` | `<` | `{"price__lt": 500}` |
| `__lte` | `<=` | `{"price__lte": 500}` |
| `__ne` | `!=` | `{"status__ne": "deleted"}` |
| `__like` | `LIKE` | `{"name__like": "%phone%"}` |
| `__ilike` | `ILIKE` | `{"name__ilike": "%Phone%"}` |
| `__not_like` | `NOT LIKE` | `{"name__not_like": "%test%"}` |
| `__in` | `IN (...)` | `{"id__in": [1, 2, 3]}` |
| `__not_in` | `NOT IN` | `{"id__not_in": [4, 5]}` |
| `__between` | `BETWEEN` | `{"price__between": [100, 500]}` |
| `__null` | `IS NULL` | `{"email__null": true}` |
| `__not_null` | `IS NOT NULL` | `{"email__not_null": true}` |

### Complex Filters (where)

Use the `where` field for nested AND/OR/NOT logic:

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "where": {
        "logic": "or",
        "conditions": [
            {"field": "category", "op": "eq", "value": "electronics"},
            {
                "logic": "and",
                "conditions": [
                    {"field": "price", "op": "lt", "value": 50},
                    {"field": "stock", "op": "gt", "value": 0}
                ]
            }
        ]
    }
})
```

SQL equivalent:
```sql
WHERE category = 'electronics' OR (price < 50 AND stock > 0)
```

**NOT example:**

```python
"where": {
    "logic": "not",
    "conditions": [
        {"field": "status", "op": "eq", "value": "deleted"}
    ]
}
```

**EXISTS example:**

```python
"where": {
    "logic": "and",
    "conditions": [
        {
            "op": "exists",
            "subquery": {
                "intent": "list",
                "entity": "order",
                "fields": ["id"],
                "filters": {"customer_id": 1}
            }
        }
    ]
}
```

### Subquery Filters

Use `$query` in filter values for subqueries:

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "filters": {
        "category_id__in": {
            "$query": {
                "intent": "list",
                "entity": "category",
                "fields": ["id"],
                "filters": {"name": "electronics"}
            }
        }
    }
})
```

SQL equivalent:
```sql
SELECT * FROM products
WHERE category_id IN (SELECT id FROM categories WHERE name = 'electronics')
```

---

## JOIN Operations

### Basic JOIN

```python
mdbp.query({
    "intent": "list",
    "entity": "order",
    "fields": ["product", "amount", "customer.name"],
    "join": [{
        "entity": "customer",
        "type": "inner",
        "on": {"customer_id": "id"}
    }]
})
```

- `on`: `{local_field: foreign_field}` format
- Dot notation in `fields`: `"customer.name"` resolves to the joined table's column
- `type`: `inner`, `left`, `right`, `full`

### Multiple JOINs

```python
mdbp.query({
    "intent": "list",
    "entity": "order_item",
    "fields": ["quantity", "order.status", "product.name"],
    "join": [
        {"entity": "order", "type": "inner", "on": {"order_id": "id"}},
        {"entity": "product", "type": "inner", "on": {"product_id": "id"}}
    ]
})
```

### Self-JOIN (Alias)

```python
mdbp.query({
    "intent": "list",
    "entity": "employee",
    "fields": ["name", "manager.name"],
    "join": [{
        "entity": "employee",
        "alias": "manager",
        "type": "left",
        "on": {"manager_id": "id"}
    }]
})
```

---

## Aggregation

### GROUP BY

```python
mdbp.query({
    "intent": "aggregate",
    "entity": "order",
    "aggregation": {"op": "count", "field": "id"},
    "group_by": ["status"]
})
```

### HAVING

```python
mdbp.query({
    "intent": "aggregate",
    "entity": "order",
    "aggregation": {"op": "sum", "field": "amount"},
    "group_by": ["customer_id"],
    "having": [{
        "op": "sum",
        "field": "amount",
        "condition": "gt",
        "value": 10000
    }]
})
```

SQL: `HAVING SUM(amount) > 10000`

### Advanced GROUP BY Modes

```python
# ROLLUP
mdbp.query({
    "intent": "aggregate",
    "entity": "sale",
    "aggregation": {"op": "sum", "field": "amount"},
    "group_by": ["year", "quarter"],
    "group_by_mode": "rollup"
})

# CUBE
mdbp.query({
    "intent": "aggregate",
    "entity": "sale",
    "aggregation": {"op": "sum", "field": "amount"},
    "group_by": ["region", "product"],
    "group_by_mode": "cube"
})

# GROUPING SETS
mdbp.query({
    "intent": "aggregate",
    "entity": "sale",
    "aggregation": {"op": "sum", "field": "amount"},
    "group_by": ["region", "product"],
    "group_by_mode": "grouping_sets",
    "grouping_sets": [["region"], ["product"], []]
})
```

---

## Computed Fields

### CASE WHEN

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "fields": ["name", "price"],
    "computed_fields": [{
        "name": "price_tier",
        "case": {
            "when": [
                {"condition": {"field": "price", "op": "gt", "value": 1000}, "then": "premium"},
                {"condition": {"field": "price", "op": "gt", "value": 100}, "then": "standard"}
            ],
            "else_value": "budget"
        }
    }]
})
```

### Window Functions

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "fields": ["name", "price", "category_id"],
    "computed_fields": [{
        "name": "price_rank",
        "window": {
            "function": "rank",
            "partition_by": ["category_id"],
            "order_by": [{"field": "price", "order": "desc"}]
        }
    }]
})
```

**Supported window functions:**
`rank`, `dense_rank`, `row_number`, `ntile`, `lag`, `lead`, `first_value`, `last_value`, `sum`, `avg`, `min`, `max`, `count`

### Scalar Functions

```python
mdbp.query({
    "intent": "list",
    "entity": "user",
    "fields": ["id"],
    "computed_fields": [
        {
            "name": "email_upper",
            "function": {"name": "upper", "args": ["email"]}
        },
        {
            "name": "display_name",
            "function": {
                "name": "coalesce",
                "args": ["nickname", {"literal": "Anonymous"}]
            }
        },
        {
            "name": "price_int",
            "function": {"name": "cast", "args": ["price"], "cast_to": "integer"}
        },
        {
            "name": "order_year",
            "function": {"name": "extract", "args": [{"literal": "year"}, "created_at"]}
        }
    ]
})
```

**Supported scalar functions:**
`coalesce`, `upper`, `lower`, `cast`, `concat`, `trim`, `length`, `abs`, `round`, `substring`, `extract`, `now`, `current_date`, `replace`

Note: `extract` requires the first argument as `{"literal": "part"}` where part is `year`, `month`, `day`, `hour`, `minute`, or `second`.

---

## CTE (Common Table Expressions)

```python
mdbp.query({
    "intent": "list",
    "entity": "product",
    "fields": ["name", "price"],
    "cte": [{
        "name": "expensive_categories",
        "query": {
            "intent": "aggregate",
            "entity": "product",
            "aggregation": {"op": "avg", "field": "price"},
            "group_by": ["category_id"],
            "having": [{"op": "avg", "field": "price", "condition": "gt", "value": 500}]
        }
    }],
    "filters": {
        "category_id__in": {"$cte": "expensive_categories", "field": "category_id"}
    }
})
```

---

## Write Operations

### batch_create - Bulk Insert

```python
mdbp.query({
    "intent": "batch_create",
    "entity": "product",
    "rows": [
        {"name": "Laptop", "price": 15000},
        {"name": "Mouse", "price": 250},
        {"name": "Keyboard", "price": 800}
    ]
})
```

### upsert - Insert or Update

```python
mdbp.query({
    "intent": "upsert",
    "entity": "product",
    "data": {"id": 1, "name": "Laptop Pro", "price": 18000},
    "conflict_target": ["id"],
    "conflict_update": ["name", "price"]
})
```

SQL: `INSERT ... ON CONFLICT (id) DO UPDATE SET name=..., price=...`

### UPDATE with JOIN

```python
mdbp.query({
    "intent": "update",
    "entity": "order",
    "data": {"status": "vip_order"},
    "from_entity": "customer",
    "from_join_on": {"customer_id": "id"},
    "from_filters": {"tier": "vip"}
})
```

### RETURNING

```python
mdbp.query({
    "intent": "create",
    "entity": "product",
    "data": {"name": "Tablet", "price": 3000},
    "returning": ["id", "name"]
})
```

---

## Set Operations

### UNION

```python
mdbp.query({
    "intent": "union",
    "entity": "customer",
    "union_all": False,
    "union_queries": [
        {"intent": "list", "entity": "customer", "fields": ["name"], "filters": {"city": "Istanbul"}},
        {"intent": "list", "entity": "customer", "fields": ["name"], "filters": {"city": "Ankara"}}
    ]
})
```

`intersect` and `except` intents are also supported in the same way.

---

## Schema Registry

### Auto-Discovery (Default)

```python
mdbp = MDBP(db_url="sqlite:///my.db")
# All tables and columns are automatically registered
```

Table name to entity name conversion:
- `products` -> `product`
- `categories` -> `category`
- `order_items` -> `order_item`

### Manual Registration

Override auto-discovery or provide custom names:

```python
from mdbp.core.schema_registry import EntitySchema, FieldSchema

mdbp.register_entity(EntitySchema(
    entity="order",
    table="orders",
    primary_key="id",
    fields={
        "id": FieldSchema(column="id", dtype="integer"),
        "customer_name": FieldSchema(
            column="cust_name",
            dtype="text",
            description="Full name of the customer"
        ),
        "total": FieldSchema(
            column="total_amount",
            dtype="numeric",
            description="Total order amount"
        ),
        "status": FieldSchema(
            column="order_status",
            dtype="text",
            filterable=True,
            sortable=True
        ),
    },
    description="Customer orders"
))
```

**FieldSchema Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `column` | str | - | Physical column name |
| `dtype` | str | "text" | Data type: text, integer, numeric, boolean, datetime |
| `description` | str | None | LLM-friendly field description |
| `filterable` | bool | True | Can be used in filters |
| `sortable` | bool | True | Can be used in sort |

### Viewing the Schema

```python
schema = mdbp.describe_schema()
```

Output:
```json
{
    "product": {
        "description": "Product catalog",
        "fields": {
            "id": {"type": "integer", "description": null, "filterable": true, "sortable": true},
            "name": {"type": "text", "description": null, "filterable": true, "sortable": true},
            "price": {"type": "numeric", "description": null, "filterable": true, "sortable": true}
        }
    }
}
```

This output can be included in an LLM system prompt.

---

## Policy Engine

The Policy Engine provides role-based access control.

### Defining Policies

```python
from mdbp.core.policy import Policy

# Analyst: read-only, sensitive fields hidden
mdbp.add_policy(Policy(
    entity="user",
    role="analyst",
    allowed_fields=["id", "name", "email", "created_at"],
    denied_fields=["password_hash", "ssn"],
    max_rows=100,
    allowed_intents=["list", "get", "count"]
))
```

**Policy Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `entity` | str | - | Target entity |
| `role` | str | "*" | Role name ("*" = all roles) |
| `allowed_fields` | list | None | Allowed fields (None = all) |
| `denied_fields` | list | [] | Denied fields (overrides allowed) |
| `max_rows` | int | 1000 | Maximum rows returned |
| `allowed_intents` | list | [list,get,count,aggregate] | Allowed operations |
| `row_filter` | dict | None | Automatically injected filter |
| `masked_fields` | dict | {} | Fields to mask in results (see [Data Masking](#data-masking)) |

### Tenant Isolation

```python
mdbp.add_policy(Policy(
    entity="order",
    role="customer",
    row_filter={"tenant_id": current_user.tenant_id}
))
```

When this policy is active, `WHERE tenant_id = :value` is automatically appended to all queries. The LLM cannot access other tenants' data.

### Global Intent Restriction

```python
# Read-only mode
mdbp = MDBP(
    db_url="sqlite:///my.db",
    allowed_intents=["list", "get", "count", "aggregate"]
)
```

This works independently from the policy engine. `create`, `update`, `delete` intents are globally blocked.

### Querying with a Role

```python
result = mdbp.query({
    "intent": "list",
    "entity": "user",
    "fields": ["name", "password_hash"],
    "role": "analyst"
})
# Error: MDBP_POLICY_FIELD_DENIED
```

---

## Data Masking

Data masking lets you return masked values for sensitive fields instead of blocking the query entirely. Unlike `denied_fields` (which rejects the query), `masked_fields` allows the query but masks the values in the response.

### Basic Usage

```python
from mdbp.core.policy import Policy

mdbp.add_policy(Policy(
    entity="customer",
    role="support",
    masked_fields={
        "email": "email",       # d***@example.com
        "phone": "last_n",      # ******4567
    },
    # name, city, etc. are returned unmasked
))
```

Only the fields listed in `masked_fields` are masked. All other fields are returned as-is.

### Built-in Strategies

| Strategy | Description | Example |
|----------|-------------|---------|
| `"partial"` | Show first and last character | `"doruk"` → `"d***k"` |
| `"redact"` | Replace entirely | `"doruk"` → `"***"` |
| `"email"` | Mask local part, keep domain | `"d@x.com"` → `"d***@x.com"` |
| `"last_n"` | Show only last N characters (default 4) | `"5551234567"` → `"******4567"` |
| `"first_n"` | Show only first N characters (default 4) | `"5551234567"` → `"5551******"` |
| `"hash"` | SHA-256 hash (first 8 chars) | `"doruk"` → `"a1b2c3d4"` |

### Strategy Options (MaskingRule)

Use `MaskingRule` for strategies that need configuration:

```python
from mdbp import MaskingRule

mdbp.add_policy(Policy(
    entity="customer",
    role="support",
    masked_fields={
        "phone": MaskingRule(strategy="last_n", options={"n": 4}),
        "credit_card": MaskingRule(strategy="last_n", options={"n": 4}),
        "email": MaskingRule(strategy="hash", options={"length": 12}),
    },
))
```

### Custom Masking Functions

Provide any callable for full control:

```python
mdbp.add_policy(Policy(
    entity="customer",
    role="support",
    masked_fields={
        "ssn": lambda v: "***-**-" + str(v)[-4:],
        "email": lambda v: v.split("@")[0][0] + "***@" + v.split("@")[1],
    },
))
```

### Masking + Denied Fields

`masked_fields` and `denied_fields` work together:

```python
mdbp.add_policy(Policy(
    entity="customer",
    role="support",
    denied_fields=["password_hash"],       # query is rejected if requested
    masked_fields={"email": "email"},      # query succeeds, value is masked
))
```

### Config File Support

```json
{
    "policies": [{
        "entity": "customer",
        "role": "support",
        "masked_fields": {
            "email": "email",
            "phone": {"strategy": "last_n", "options": {"n": 4}}
        }
    }]
}
```

### Notes

- `None`/null values are never masked — they stay `None`
- Numeric values are converted to string before masking
- Masking is applied by the library after query execution, not by the AI
- Works with all intent types that return data: `list`, `get`, `aggregate`, mutations with `returning`

---

## Dry-Run Mode

Any intent can include `"dry_run": true` to get the compiled SQL and parameters without executing the query. Schema validation and policy enforcement still apply.

```python
result = mdbp.query({
    "intent": "list",
    "entity": "product",
    "filters": {"price__gte": 100},
    "fields": ["name", "price"],
    "dry_run": True
})
```

Output:

```json
{
    "success": true,
    "intent": "list",
    "entity": "product",
    "dry_run": true,
    "sql": "SELECT products.name, products.price FROM products WHERE products.price >= :price_1",
    "params": {"price_1": 100}
}
```

Useful for:
- **Debugging**: See the exact SQL that MDBP generates
- **Testing**: Validate query structure without hitting the database
- **Approval workflows**: Review queries before execution

---

## MCP Server

MDBP can be exposed to Claude, Cursor, and other MCP-compatible clients via the Model Context Protocol.

### Starting via CLI

```bash
# stdio (default) — for Claude Desktop, Cursor, etc.
mdbp-server --db-url "postgresql://user:pass@localhost/mydb"

# SSE — HTTP + Server-Sent Events at /sse
mdbp-server --db-url "postgresql://..." --transport sse --port 8000

# Streamable HTTP — newer MCP HTTP protocol at /mcp
mdbp-server --db-url "postgresql://..." --transport streamable-http --port 8000

# WebSocket — WebSocket at /ws
mdbp-server --db-url "postgresql://..." --transport websocket --port 8000

# With config file
mdbp-server --db-url "sqlite:///my.db" --config config.json --transport sse
```

### Config File

```json
{
    "entities": [
        {
            "entity": "product",
            "table": "products",
            "primary_key": "id",
            "description": "Product catalog",
            "fields": {
                "id": {"column": "id", "dtype": "integer"},
                "name": {"column": "product_name", "dtype": "text", "description": "Product name"},
                "price": {"column": "unit_price", "dtype": "numeric"}
            }
        }
    ],
    "policies": [
        {
            "entity": "product",
            "role": "viewer",
            "allowed_intents": ["list", "get", "count"],
            "max_rows": 100
        }
    ]
}
```

### Claude Desktop Integration

`claude_desktop_config.json`:

```json
{
    "mcpServers": {
        "mdbp": {
            "command": "python",
            "args": ["-u", "path/to/server.py"],
            "env": {
                "PYTHONPATH": "path/to/mdbp/project"
            }
        }
    }
}
```

### Programmatic Usage

All transports are available as one-liner functions:

```python
from mdbp import MDBP
from mdbp.transport.server import run_sse, run_streamable_http, run_websocket, run_stdio

mdbp = MDBP(db_url="postgresql://user:pass@localhost/mydb")

run_sse(mdbp, host="0.0.0.0", port=8000)                # SSE at /sse
run_streamable_http(mdbp, host="0.0.0.0", port=8000)     # Streamable HTTP at /mcp
run_websocket(mdbp, host="0.0.0.0", port=8000)           # WebSocket at /ws
run_stdio(mdbp)                                           # stdin/stdout
```

**ASGI apps (for custom middleware or mounting):**

```python
from mdbp.transport.server import sse_app, streamable_http_app, websocket_app

app = sse_app(mdbp)                # Starlette ASGI app — /sse endpoint
app = streamable_http_app(mdbp)    # Starlette ASGI app — /mcp endpoint
app = websocket_app(mdbp)          # Starlette ASGI app — /ws endpoint
```

**Low-level (full control):**

```python
from mdbp.transport.server import create_server

server = create_server(mdbp)  # Returns mcp.server.Server — wire any transport yourself
```

### Exposed MCP Tools

| Tool | Description |
|------|-------------|
| `mdbp_query` | Execute an intent-based database query |
| `mdbp_describe_schema` | List available entities and fields |

---

## Error Handling

MDBP catches all errors and returns structured JSON. It never raises exceptions from `query()`.

### Error Structure

```json
{
    "success": false,
    "intent": "list",
    "entity": "product",
    "error": {
        "code": "MDBP_SCHEMA_FIELD_NOT_FOUND",
        "message": "Field 'colour' not found on entity 'product'.",
        "details": {
            "entity": "product",
            "field": "colour",
            "available_fields": ["id", "name", "price", "color", "category_id"]
        }
    }
}
```

### Error Codes

#### Schema Errors (`MDBP_SCHEMA_*`)

| Code | Meaning | Details |
|------|---------|---------|
| `MDBP_SCHEMA_ENTITY_NOT_FOUND` | Entity does not exist in registry | available_entities list |
| `MDBP_SCHEMA_FIELD_NOT_FOUND` | Field does not exist on entity | available_fields list |
| `MDBP_SCHEMA_ENTITY_REF_NOT_FOUND` | Referenced JOIN entity not found | entity_reference, field |

#### Policy Errors (`MDBP_POLICY_*`)

| Code | Meaning | Details |
|------|---------|---------|
| `MDBP_POLICY_INTENT_NOT_ALLOWED` | Intent type not allowed for role | intent_type, entity, role |
| `MDBP_POLICY_FIELD_DENIED` | Field is in denied_fields list | entity, denied_fields |
| `MDBP_POLICY_FIELD_NOT_ALLOWED` | Field is not in allowed_fields list | entity, allowed_fields |

#### Intent Errors (`MDBP_INTENT_*`)

| Code | Meaning | Details |
|------|---------|---------|
| `MDBP_INTENT_TYPE_NOT_ALLOWED` | Intent type globally blocked | intent_type, allowed_intents |
| `MDBP_INTENT_VALIDATION_ERROR` | Invalid intent structure (Pydantic) | errors list |

#### Query Errors (`MDBP_QUERY_*`)

| Code | Meaning | Details |
|------|---------|---------|
| `MDBP_QUERY_PLAN_ERROR` | Query planning failed | - |
| `MDBP_QUERY_MISSING_FIELD` | Required field missing | intent_type, required_field |
| `MDBP_QUERY_UNKNOWN_FILTER_OP` | Unknown filter operator | op, supported_ops |
| `MDBP_QUERY_UNION_REQUIRES_SUBQUERIES` | UNION needs 2+ sub-queries | - |

#### Connection Errors (`MDBP_CONN_*`)

| Code | Meaning | Details |
|------|---------|---------|
| `MDBP_CONN_FAILED` | Database connection failed | - |
| `MDBP_CONN_EXECUTION_ERROR` | Query execution failed | original_error |
| `MDBP_NOT_FOUND` | GET query returned no results | entity, id |

#### Config Errors

| Code | Meaning | Details |
|------|---------|---------|
| `MDBP_CONFIG_FILE_NOT_FOUND` | Config file does not exist | path |

### Handling Errors in Code

```python
from mdbp import MDBP

mdbp = MDBP(db_url="sqlite:///my.db")
result = mdbp.query({"intent": "list", "entity": "product"})

if not result["success"]:
    code = result["error"]["code"]
    if code == "MDBP_SCHEMA_ENTITY_NOT_FOUND":
        entities = result["error"]["details"]["available_entities"]
        print(f"Available entities: {entities}")
```

---

## API Reference

### MDBP Class

```python
class MDBP:
    def __init__(
        self,
        db_url: str,
        auto_discover: bool = True,
        allowed_intents: list[str] | None = None,
    ) -> None

    def register_entity(schema: EntitySchema) -> None
    def add_policy(policy: Policy) -> None
    def query(raw_intent: dict | Intent) -> dict
    def describe_schema() -> dict
    def dispose() -> None
```

| Method | Description |
|--------|-------------|
| `register_entity()` | Register a custom entity schema (overrides auto-discovery) |
| `add_policy()` | Add an access control policy |
| `query()` | Execute the full MDBP pipeline. Accepts dict or Intent. Returns structured response. |
| `describe_schema()` | Return LLM-friendly schema description |
| `dispose()` | Release all database connections |

### EntitySchema

```python
class EntitySchema(BaseModel):
    entity: str                           # Logical entity name
    table: str                            # Physical table name
    primary_key: str = "id"               # Primary key column
    fields: dict[str, FieldSchema]        # Field definitions
    relations: dict[str, RelationSchema] = {}
    description: str | None = None
```

### FieldSchema

```python
class FieldSchema(BaseModel):
    column: str              # Physical column name
    dtype: str = "text"      # text, integer, numeric, boolean, datetime
    description: str | None = None
    filterable: bool = True
    sortable: bool = True
```

### RelationSchema

```python
class RelationSchema(BaseModel):
    target_entity: str                  # Related entity name
    join_column: str                    # Column on this entity's table
    target_column: str                  # Column on target entity's table
    relation_type: str = "many_to_one"  # one_to_one, many_to_one, one_to_many
```

### Policy

```python
class Policy(BaseModel):
    entity: str
    role: str = "*"
    allowed_fields: list[str] | None = None
    denied_fields: list[str] = []
    max_rows: int = 1000
    allowed_intents: list[IntentType] = [LIST, GET, COUNT, AGGREGATE]
    row_filter: dict | None = None
    masked_fields: dict[str, str | MaskingRule | Callable] = {}
```

### IntentType Enum

```python
class IntentType(str, Enum):
    LIST = "list"
    GET = "get"
    COUNT = "count"
    AGGREGATE = "aggregate"
    CREATE = "create"
    BATCH_CREATE = "batch_create"
    UPSERT = "upsert"
    UPDATE = "update"
    DELETE = "delete"
    UNION = "union"
    INTERSECT = "intersect"
    EXCEPT = "except"
```

---

## Security

### Hallucination Protection

LLMs can generate non-existent table or column names. The schema registry catches these:

```
LLM: query "userz" table
MDBP: MDBP_SCHEMA_ENTITY_NOT_FOUND + list of available entities
LLM: self-corrects -> query "user" table
```

### SQL Injection Prevention

All queries are parameterized via SQLAlchemy. Raw SQL strings are never constructed.

### Access Control

- **denied_fields**: Sensitive fields (password_hash, ssn) can never be returned
- **allowed_fields**: Only whitelisted fields are accessible
- **masked_fields**: Sensitive fields are returned with masked values (email, phone, etc.)
- **allowed_intents**: Write operations can be blocked globally or per role
- **max_rows**: Limits large query results per role
- **row_filter**: Automatic tenant isolation via injected WHERE conditions

---

## Try It Out

The [examples/ecommerce-mdbp-server](https://github.com/DorukYelken/Model-Database-Protocol/tree/main/examples/ecommerce-mdbp-server) project is a complete working example you can run locally:

```bash
cd examples/ecommerce-mdbp-server
pip install mdbp
python setup_db.py    # Create e-commerce database with sample data
python server.py      # Start MDBP server on :8000
```

This example demonstrates:
- Auto-discovery (no manual schema registration)
- Role-based access control (customer, support, admin)
- Tenant isolation via `row_filter`
- PII protection via `denied_fields`
- All 4 transport modes (stdio, sse, streamable-http, websocket)

See the [example README](https://github.com/DorukYelken/Model-Database-Protocol/tree/main/examples/ecommerce-mdbp-server) for full details and sample queries.

---

<p align="center">
  <img src="https://raw.githubusercontent.com/DorukYelken/Model-Database-Protocol/main/logo.png" alt="MDBP Logo" width="64">
  <br>
  <sub>MDBP — Model Database Protocol</sub>
</p>
