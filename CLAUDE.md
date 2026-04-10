# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is MDBP?

MDBP (Model Database Protocol) is an intent-based data access protocol for AI systems. Instead of LLMs generating raw SQL, they produce structured intent objects. MDBP validates intents against a schema registry, enforces access policies, builds parameterized queries via SQLAlchemy, and returns LLM-friendly responses.

## Commands

```bash
# Install (editable, with dev deps)
pip install -e ".[dev]"

# Run all tests
pytest tests/test_e2e.py -v

# Run a single test
pytest tests/test_e2e.py -v -k "test_name"

# Run MCP server (stdio, default)
mdbp-server --db-url sqlite:///my.db --config config.json

# Run MCP server with other transports
mdbp-server --db-url sqlite:///my.db --transport sse --port 8000
mdbp-server --db-url sqlite:///my.db --transport streamable-http --port 8000
mdbp-server --db-url sqlite:///my.db --transport websocket --port 8000
```

Tests use a file-based SQLite database (`test_mdbp.db`) that is created and cleaned up automatically.

## Architecture

The core pipeline in `MDBP.query()` ([mdbp.py](mdbp/mdbp.py)):

```
Dict/Intent → Parse → AllowedIntents check → Schema validation → Policy enforcement → QueryPlanner → [Dry-run?] → SQLConnector → [Data Masking] → ResponseFormatter → [Audit Log] → dict
```

### Module responsibilities

- **[mdbp/mdbp.py](mdbp/mdbp.py)** — Orchestrator. Wires the pipeline, catches all `MDBPError` subtypes, returns structured responses. Supports dry-run mode.
- **[core/intent.py](mdbp/core/intent.py)** — Pydantic models: `Intent`, `IntentType`, `FilterGroup`, `FilterCondition`, `JoinSpec`, `HavingCondition`. This is the protocol's "wire format".
- **[core/schema_registry.py](mdbp/core/schema_registry.py)** — Maps logical entity/field names to physical table/column names. `auto_discover()` introspects DB metadata via SQLAlchemy so no manual registration is needed.
- **[core/policy.py](mdbp/core/policy.py)** — `PolicyEngine` evaluates `Policy` objects per entity+role. Enforces field-level access (denied/allowed fields), data masking, intent type restrictions, row limits, and automatic row filters (tenant isolation).
- **[core/audit.py](mdbp/core/audit.py)** — Audit logging. `AuditEntry` dataclass + pluggable backends: `CallbackAuditLogger`, `PythonAuditLogger`, `StreamAuditLogger`. Logs every query with timestamp, intent, role, success/error, row count, duration, masked fields. Enabled via `MDBP(audit=...)`.
- **[core/masking.py](mdbp/core/masking.py)** — Data masking engine. `MaskingRule` model + built-in strategies (`partial`, `redact`, `email`, `last_n`, `first_n`, `hash`) + custom callable support. Applied after query execution, before response formatting.
- **[core/query_planner.py](mdbp/core/query_planner.py)** — Converts `Intent` → SQLAlchemy statement. Handles all SQL operations: SELECT, JOIN, DISTINCT, GROUP BY, HAVING, UNION, INSERT, UPDATE, DELETE. Never builds raw SQL strings.
- **[core/errors.py](mdbp/core/errors.py)** — Error hierarchy rooted at `MDBPError`. Every error has a machine-readable `code` (e.g. `MDBP_SCHEMA_FIELD_NOT_FOUND`), human-readable `message`, and structured `details` dict.
- **[core/response.py](mdbp/core/response.py)** — Formats `QueryResult` into `MDBPResponse` with success/error structure.
- **[connectors/sql.py](mdbp/connectors/sql.py)** — `SQLConnector` wraps SQLAlchemy engine. Executes statements, returns `QueryResult`. Includes BigQuery fallback: when `MetaData.reflect()` returns empty on BigQuery dialect, discovers tables via `INFORMATION_SCHEMA.TABLES` and reflects each one individually.
- **[transport/server.py](mdbp/transport/server.py)** — Exposes MDBP as an MCP server (JSON-RPC over stdio/SSE/streamable-http/websocket). Provides `mdbp_query` and `mdbp_describe_schema` tools.

### Error code prefixes

- `MDBP_SCHEMA_*` — Entity/field not found (hallucination protection)
- `MDBP_POLICY_*` — Access denied, field denied, intent not allowed
- `MDBP_INTENT_*` — Invalid intent structure or globally blocked intent type
- `MDBP_QUERY_*` — Query planning failures (missing fields, unknown operators)
- `MDBP_CONN_*` — Database connection/execution errors

### Key design decisions

- **Auto-discovery by default**: `MDBP(db_url=...)` reflects all tables/columns from the DB. Manual `register_entity()` is optional (for overrides like field renaming or descriptions). BigQuery is supported via an `INFORMATION_SCHEMA.TABLES` fallback since its driver can't list tables through standard `MetaData.reflect()`.
- **`allowed_intents` parameter**: Global whitelist at MDBP level (e.g. `["list", "get", "count"]` for read-only). Separate from per-entity/role policies.
- **Two filter modes**: Simple dict with operator suffixes (`{"price__gte": 100}`) and complex nested `FilterGroup` with AND/OR/NOT logic via the `where` field.
- **JOIN fields use dot notation**: `"customer.name"` in `fields` resolves to the joined entity's column.
- **All queries are parameterized**: SQLAlchemy handles escaping. No raw SQL strings are ever constructed.
- **Config-driven server setup**: `mdbp-server --config` loads entity schemas and policies from a JSON file via `build_mdbp_from_config()`.
- **Dry-run mode**: Any intent can include `"dry_run": true` to get the compiled SQL and parameters without executing the query. Schema validation and policy enforcement still apply. Useful for debugging, testing, and approval workflows.
- **Audit logging**: `MDBP(audit=...)` enables structured query logging. Every `query()` call emits an `AuditEntry` with timestamp, intent type, entity, role, success/error code, row count, duration (ms), dry-run flag, and masked field names. Pluggable backends: `PythonAuditLogger` (Python logging), `StreamAuditLogger` (JSON lines to stdout/file), `CallbackAuditLogger` (custom callable). Default is no logging (backward compatible).
- **Data masking**: `Policy.masked_fields` masks field values in query results without blocking the query. Complementary to `denied_fields` (which blocks entirely). Supports built-in strategies (`partial`, `redact`, `email`, `last_n`, `first_n`, `hash`), `MaskingRule` with options, and custom callables. Masking is applied by the library after execution, not by the AI. `None` values are never masked. Default is no masking (backward compatible).
