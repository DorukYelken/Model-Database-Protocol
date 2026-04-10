"""
End-to-end test for MDBP.

Tests the full pipeline including:
  - Auto-discovery
  - All SQL operations (JOIN, DISTINCT, OR, NULL, BETWEEN, HAVING, UNION)
  - allowed_intents restriction
  - Policy enforcement
  - Hallucination protection
"""

import sys
import io

# Ensure UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    insert,
)

from mdbp import MDBP
from mdbp.core.policy import Policy


DB_URL = "sqlite:///test_mdbp.db"


def setup_test_db() -> str:
    engine = create_engine(DB_URL)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    metadata.drop_all(bind=engine)
    metadata = MetaData()

    categories = Table(
        "categories", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )

    products = Table(
        "products", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("price", Numeric),
        Column("category_id", Integer, ForeignKey("categories.id")),
        Column("stock", Integer),
        Column("description", String, nullable=True),
    )

    users = Table(
        "users", metadata,
        Column("id", Integer, primary_key=True),
        Column("username", String),
        Column("email", String),
        Column("password_hash", String),
        Column("role", String),
    )

    metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(insert(categories).values([
            {"id": 1, "name": "electronics"},
            {"id": 2, "name": "furniture"},
        ]))
        conn.execute(insert(products).values([
            {"id": 1, "name": "Laptop", "price": 999.99, "category_id": 1, "stock": 50, "description": "High-end laptop"},
            {"id": 2, "name": "Mouse", "price": 29.99, "category_id": 1, "stock": 200, "description": None},
            {"id": 3, "name": "Desk", "price": 249.99, "category_id": 2, "stock": 30, "description": "Standing desk"},
            {"id": 4, "name": "Chair", "price": 199.99, "category_id": 2, "stock": 45, "description": None},
            {"id": 5, "name": "Monitor", "price": 449.99, "category_id": 1, "stock": 75, "description": "4K monitor"},
        ]))
        conn.execute(insert(users).values([
            {"id": 1, "username": "alice", "email": "alice@test.com", "password_hash": "hash1", "role": "admin"},
            {"id": 2, "username": "bob", "email": "bob@test.com", "password_hash": "hash2", "role": "user"},
        ]))
        conn.commit()

    engine.dispose()
    return DB_URL


def test_basic_operations():
    """Test list, get, count, aggregate with auto-discovery."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # LIST
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"category_id": 1}})
    assert result["success"] is True
    assert len(result["data"]) == 3
    print("  list ✓")

    # GET
    result = mdbp.query({"intent": "get", "entity": "product", "id": 1})
    assert result["success"] is True
    assert result["data"]["name"] == "Laptop"
    print("  get ✓")

    # COUNT
    result = mdbp.query({"intent": "count", "entity": "product"})
    assert result["success"] is True
    print("  count ✓")

    # AGGREGATE
    result = mdbp.query({
        "intent": "aggregate",
        "entity": "product",
        "aggregation": {"op": "avg", "field": "price"},
    })
    assert result["success"] is True
    print("  aggregate ✓")


def test_filter_operators():
    """Test all filter suffixes: __gt, __gte, __lt, __lte, __ne, __like, __in, __between, __null, __not_null."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # __gte
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"price__gte": 200}})
    assert result["success"] and len(result["data"]) == 3
    print("  __gte ✓")

    # __lt
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"price__lt": 100}})
    assert result["success"] and len(result["data"]) == 1
    print("  __lt ✓")

    # __ne
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"name__ne": "Laptop"}})
    assert result["success"] and len(result["data"]) == 4
    print("  __ne ✓")

    # __like
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"name__like": "M%"}})
    assert result["success"] and len(result["data"]) == 2  # Mouse, Monitor
    print("  __like ✓")

    # __in
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"name__in": ["Laptop", "Mouse"]}})
    assert result["success"] and len(result["data"]) == 2
    print("  __in ✓")

    # __not_in
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"name__not_in": ["Laptop", "Mouse"]}})
    assert result["success"] and len(result["data"]) == 3
    print("  __not_in ✓")

    # __between
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"price__between": [100, 500]}})
    assert result["success"] and len(result["data"]) == 3  # Desk, Chair, Monitor
    print("  __between ✓")

    # __null
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"description__null": True}})
    assert result["success"] and len(result["data"]) == 2  # Mouse, Chair
    print("  __null ✓")

    # __not_null (via __null: False)
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"description__null": False}})
    assert result["success"] and len(result["data"]) == 3
    print("  __not_null ✓")


def test_or_conditions():
    """Test complex OR/AND/NOT nested conditions."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # OR: price > 400 OR stock > 100
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "where": {
            "logic": "or",
            "conditions": [
                {"field": "price", "op": "gt", "value": 400},
                {"field": "stock", "op": "gt", "value": 100},
            ],
        },
    })
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Laptop" in names    # price 999 > 400
    assert "Mouse" in names     # stock 200 > 100
    assert "Monitor" in names   # price 449 > 400
    print("  OR ✓")

    # Nested: category_id=1 AND (price > 400 OR stock > 100)
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "filters": {"category_id": 1},
        "where": {
            "logic": "or",
            "conditions": [
                {"field": "price", "op": "gt", "value": 400},
                {"field": "stock", "op": "gt", "value": 100},
            ],
        },
    })
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Desk" not in names   # category_id=2
    assert "Chair" not in names  # category_id=2
    print("  nested AND+OR ✓")

    # NOT: NOT (price > 400)
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "where": {
            "logic": "not",
            "conditions": [
                {"field": "price", "op": "gt", "value": 400},
            ],
        },
    })
    assert result["success"] is True
    for r in result["data"]:
        assert float(r["price"]) <= 400
    print("  NOT ✓")

    # Condition with null check
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "where": {
            "logic": "and",
            "conditions": [
                {"field": "description", "op": "null"},
                {"field": "price", "op": "lt", "value": 100},
            ],
        },
    })
    assert result["success"] is True
    assert len(result["data"]) == 1  # Mouse: null description + price 29.99
    print("  condition null ✓")

    # Condition with between
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "where": {
            "logic": "and",
            "conditions": [
                {"field": "price", "op": "between", "value": [200, 500]},
            ],
        },
    })
    assert result["success"] is True
    assert len(result["data"]) == 2  # Desk (249.99), Monitor (449.99)
    print("  condition between ✓")


def test_distinct():
    """Test DISTINCT."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["category_id"],
        "distinct": True,
    })
    assert result["success"] is True
    assert len(result["data"]) == 2  # 2 unique category_ids
    print("  DISTINCT ✓")

    # COUNT DISTINCT
    result = mdbp.query({
        "intent": "count",
        "entity": "product",
        "fields": ["category_id"],
        "distinct": True,
    })
    assert result["success"] is True
    print("  COUNT DISTINCT ✓")


def test_join():
    """Test JOIN operations."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # INNER JOIN products + categories
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "price", "category.name"],
        "join": [{"entity": "category", "type": "inner", "on": {"category_id": "id"}}],
    })
    assert result["success"] is True
    assert len(result["data"]) == 5
    print("  INNER JOIN ✓")

    # LEFT JOIN
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "category.name"],
        "join": [{"entity": "category", "type": "left", "on": {"category_id": "id"}}],
    })
    assert result["success"] is True
    print("  LEFT JOIN ✓")


def test_having():
    """Test GROUP BY + HAVING."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Group by category_id, HAVING count > 2
    result = mdbp.query({
        "intent": "aggregate",
        "entity": "product",
        "aggregation": {"op": "count", "field": "id"},
        "group_by": ["category_id"],
        "having": [{"op": "count", "field": "id", "condition": "gt", "value": 2}],
    })
    assert result["success"] is True
    assert len(result["data"]) == 1  # Only electronics (3 products)
    print("  HAVING ✓")


def test_union():
    """Test UNION."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "union",
        "entity": "product",
        "union_queries": [
            {"intent": "list", "entity": "product", "fields": ["name"], "filters": {"category_id": 1}},
            {"intent": "list", "entity": "product", "fields": ["name"], "filters": {"category_id": 2}},
        ],
    })
    assert result["success"] is True
    assert len(result["data"]) == 5
    print("  UNION ✓")


def test_write_operations():
    """Test CREATE, UPDATE, DELETE."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # CREATE
    result = mdbp.query({
        "intent": "create",
        "entity": "product",
        "data": {"id": 10, "name": "Keyboard", "price": 79.99, "category_id": 1, "stock": 150},
    })
    assert result["success"] is True
    print("  CREATE ✓")

    # Verify
    result = mdbp.query({"intent": "get", "entity": "product", "id": 10})
    assert result["success"] is True
    assert result["data"]["name"] == "Keyboard"
    print("  CREATE verified ✓")

    # UPDATE
    result = mdbp.query({
        "intent": "update",
        "entity": "product",
        "id": 10,
        "data": {"price": 89.99},
    })
    assert result["success"] is True
    print("  UPDATE ✓")

    # DELETE
    result = mdbp.query({
        "intent": "delete",
        "entity": "product",
        "id": 10,
    })
    assert result["success"] is True
    print("  DELETE ✓")


def test_allowed_intents():
    """Test allowed_intents restriction."""
    db_url = setup_test_db()

    # Read-only mode
    mdbp = MDBP(db_url=db_url, allowed_intents=["list", "get", "count", "aggregate"])

    # Read should work
    result = mdbp.query({"intent": "list", "entity": "product"})
    assert result["success"] is True
    print("  allowed: list ✓")

    # Write should be blocked
    result = mdbp.query({
        "intent": "delete",
        "entity": "product",
        "id": 1,
    })
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_INTENT_TYPE_NOT_ALLOWED"
    print("  blocked: delete ✓")

    result = mdbp.query({
        "intent": "create",
        "entity": "product",
        "data": {"name": "Hacked"},
    })
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_INTENT_TYPE_NOT_ALLOWED"
    print("  blocked: create ✓")

    result = mdbp.query({
        "intent": "update",
        "entity": "product",
        "id": 1,
        "data": {"name": "Hacked"},
    })
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_INTENT_TYPE_NOT_ALLOWED"
    print("  blocked: update ✓")


def test_policy():
    """Test policy enforcement."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)
    mdbp.add_policy(Policy(entity="user", role="*", denied_fields=["password_hash"], max_rows=50))

    # Denied field
    result = mdbp.query({"intent": "list", "entity": "user", "fields": ["username", "password_hash"]})
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_POLICY_FIELD_DENIED"
    assert "password_hash" in result["error"]["details"]["denied_fields"]
    print("  denied field ✓")

    # Allowed fields
    result = mdbp.query({"intent": "list", "entity": "user", "fields": ["username", "email"]})
    assert result["success"] is True
    print("  allowed fields ✓")


def test_hallucination_protection():
    """Test that non-existent entities and fields are rejected."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Unknown entity
    result = mdbp.query({"intent": "list", "entity": "nonexistent"})
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_SCHEMA_ENTITY_NOT_FOUND"
    assert "nonexistent" == result["error"]["details"]["entity"]
    assert "product" in result["error"]["details"]["available_entities"]
    print("  unknown entity ✓")

    # Hallucinated field
    result = mdbp.query({"intent": "list", "entity": "product", "fields": ["name", "fake_column"]})
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_SCHEMA_FIELD_NOT_FOUND"
    assert result["error"]["details"]["field"] == "fake_column"
    assert "name" in result["error"]["details"]["available_fields"]
    print("  hallucinated field ✓")


def test_sort_and_pagination():
    """Test ORDER BY + LIMIT/OFFSET."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "sort": [{"field": "price", "order": "desc"}],
        "limit": 2,
        "offset": 1,
    })
    assert result["success"] is True
    assert len(result["data"]) == 2
    assert result["data"][0]["name"] == "Monitor"  # 2nd most expensive
    print("  sort + pagination ✓")


def test_error_codes():
    """
    Comprehensive error code test.
    Triggers every MDBP error code and validates code, message, and details.
    """
    db_url = setup_test_db()

    import json

    def assert_error(result, expected_code, check_details=None):
        assert result["success"] is False, f"Expected failure but got success: {result}"
        err = result["error"]
        assert "code" in err, f"Missing 'code' in error: {err}"
        assert "message" in err, f"Missing 'message' in error: {err}"
        assert err["code"] == expected_code, f"Expected {expected_code}, got {err['code']}"
        assert isinstance(err["message"], str) and len(err["message"]) > 0
        if check_details:
            assert "details" in err, f"Missing 'details' in error: {err}"
            check_details(err["details"])

        # Print full error detail
        print(f"    code:    {err['code']}")
        print(f"    message: {err['message']}")
        if err.get("details"):
            print(f"    details: {json.dumps(err['details'], indent=None, default=str)}")

    # ─── MDBP_INTENT_VALIDATION_ERROR ─────────────────────────
    mdbp = MDBP(db_url=db_url)

    # Invalid intent type string
    result = mdbp.query({"intent": "invalid_type", "entity": "product"})
    assert_error(result, "MDBP_INTENT_VALIDATION_ERROR", lambda d: "errors" in d)
    print("  MDBP_INTENT_VALIDATION_ERROR (bad intent type) ✓")

    # Missing required field 'entity'
    result = mdbp.query({"intent": "list"})
    assert_error(result, "MDBP_INTENT_VALIDATION_ERROR", lambda d: "errors" in d)
    print("  MDBP_INTENT_VALIDATION_ERROR (missing entity) ✓")

    # ─── MDBP_SCHEMA_ENTITY_NOT_FOUND ─────────────────────────
    result = mdbp.query({"intent": "list", "entity": "nonexistent"})
    assert_error(result, "MDBP_SCHEMA_ENTITY_NOT_FOUND", lambda d: (
        d["entity"] == "nonexistent" and
        isinstance(d["available_entities"], list) and
        "product" in d["available_entities"]
    ))
    print("  MDBP_SCHEMA_ENTITY_NOT_FOUND ✓")

    # ─── MDBP_SCHEMA_FIELD_NOT_FOUND ──────────────────────────
    result = mdbp.query({"intent": "list", "entity": "product", "fields": ["name", "hallucinated_col"]})
    assert_error(result, "MDBP_SCHEMA_FIELD_NOT_FOUND", lambda d: (
        d["entity"] == "product" and
        d["field"] == "hallucinated_col" and
        "name" in d["available_fields"]
    ))
    print("  MDBP_SCHEMA_FIELD_NOT_FOUND ✓")

    # ─── MDBP_SCHEMA_FIELD_NOT_FOUND (dot notation, bad field on valid joined entity) ──
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "category.nonexistent"],
        "join": [{"entity": "category", "type": "inner", "on": {"category_id": "id"}}],
    })
    assert_error(result, "MDBP_SCHEMA_FIELD_NOT_FOUND", lambda d: (
        d["entity"] == "category" and
        d["field"] == "nonexistent"
    ))
    print("  MDBP_SCHEMA_FIELD_NOT_FOUND (join dot notation) ✓")

    # ─── MDBP_SCHEMA_ENTITY_NOT_FOUND (dot notation, bad entity ref) ──
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "fake_entity.name"],
        "join": [{"entity": "category", "type": "inner", "on": {"category_id": "id"}}],
    })
    assert_error(result, "MDBP_SCHEMA_ENTITY_NOT_FOUND", lambda d: (
        d["entity"] == "fake_entity"
    ))
    print("  MDBP_SCHEMA_ENTITY_NOT_FOUND (bad dot notation entity) ✓")

    # ─── MDBP_INTENT_TYPE_NOT_ALLOWED ─────────────────────────
    read_only = MDBP(db_url=db_url, allowed_intents=["list", "get"])

    result = read_only.query({"intent": "delete", "entity": "product", "id": 1})
    assert_error(result, "MDBP_INTENT_TYPE_NOT_ALLOWED", lambda d: (
        d["intent_type"] == "delete" and
        "list" in d["allowed_intents"] and
        "delete" not in d["allowed_intents"]
    ))
    print("  MDBP_INTENT_TYPE_NOT_ALLOWED ✓")

    # ─── MDBP_POLICY_FIELD_DENIED ─────────────────────────────
    mdcp_with_policy = MDBP(db_url=db_url)
    mdcp_with_policy.add_policy(Policy(
        entity="user", role="*",
        denied_fields=["password_hash", "email"],
    ))

    result = mdcp_with_policy.query({
        "intent": "list", "entity": "user",
        "fields": ["username", "password_hash"],
    })
    assert_error(result, "MDBP_POLICY_FIELD_DENIED", lambda d: (
        d["entity"] == "user" and
        "password_hash" in d["denied_fields"]
    ))
    print("  MDBP_POLICY_FIELD_DENIED ✓")

    # ─── MDBP_POLICY_FIELD_NOT_ALLOWED ────────────────────────
    mdcp_allowed = MDBP(db_url=db_url)
    mdcp_allowed.add_policy(Policy(
        entity="user", role="*",
        allowed_fields=["id", "username"],
    ))

    result = mdcp_allowed.query({
        "intent": "list", "entity": "user",
        "fields": ["username", "email"],
    })
    assert_error(result, "MDBP_POLICY_FIELD_NOT_ALLOWED", lambda d: (
        d["entity"] == "user" and
        "email" in d["disallowed_fields"] and
        "username" in d["allowed_fields"]
    ))
    print("  MDBP_POLICY_FIELD_NOT_ALLOWED ✓")

    # ─── MDBP_POLICY_INTENT_NOT_ALLOWED ───────────────────────
    mdcp_role_policy = MDBP(db_url=db_url)
    mdcp_role_policy.add_policy(Policy(
        entity="product", role="viewer",
        allowed_intents=["list", "get"],
    ))

    result = mdcp_role_policy.query({
        "intent": "delete", "entity": "product", "id": 1,
        "role": "viewer",
    })
    assert_error(result, "MDBP_POLICY_INTENT_NOT_ALLOWED", lambda d: (
        d["intent_type"] == "delete" and
        d["entity"] == "product" and
        d["role"] == "viewer"
    ))
    print("  MDBP_POLICY_INTENT_NOT_ALLOWED ✓")

    # ─── MDBP_QUERY_MISSING_FIELD (aggregate without aggregation) ──
    result = mdbp.query({"intent": "aggregate", "entity": "product"})
    assert_error(result, "MDBP_QUERY_MISSING_FIELD", lambda d: (
        d["intent_type"] == "aggregate" and
        d["required_field"] == "aggregation"
    ))
    print("  MDBP_QUERY_MISSING_FIELD (aggregate) ✓")

    # ─── MDBP_QUERY_MISSING_FIELD (create without data) ──────
    result = mdbp.query({"intent": "create", "entity": "product"})
    assert_error(result, "MDBP_QUERY_MISSING_FIELD", lambda d: (
        d["intent_type"] == "create" and
        d["required_field"] == "data"
    ))
    print("  MDBP_QUERY_MISSING_FIELD (create) ✓")

    # ─── MDBP_QUERY_MISSING_FIELD (update without data) ──────
    result = mdbp.query({"intent": "update", "entity": "product", "id": 1})
    assert_error(result, "MDBP_QUERY_MISSING_FIELD", lambda d: (
        d["intent_type"] == "update" and
        d["required_field"] == "data"
    ))
    print("  MDBP_QUERY_MISSING_FIELD (update) ✓")

    # ─── MDBP_QUERY_UNKNOWN_FILTER_OP ─────────────────────────
    result = mdbp.query({
        "intent": "list", "entity": "product",
        "where": {
            "logic": "and",
            "conditions": [{"field": "price", "op": "invalid_op", "value": 100}],
        },
    })
    assert_error(result, "MDBP_QUERY_UNKNOWN_FILTER_OP", lambda d: (
        d["op"] == "invalid_op" and
        isinstance(d["supported_ops"], list) and
        "eq" in d["supported_ops"]
    ))
    print("  MDBP_QUERY_UNKNOWN_FILTER_OP ✓")

    # ─── MDBP_QUERY_UNION_REQUIRES_SUBQUERIES ─────────────────
    result = mdbp.query({"intent": "union", "entity": "product"})
    assert_error(result, "MDBP_QUERY_UNION_REQUIRES_SUBQUERIES")
    print("  MDBP_QUERY_UNION_REQUIRES_SUBQUERIES (none) ✓")

    # Union with only 1 sub-query
    result = mdbp.query({
        "intent": "union", "entity": "product",
        "union_queries": [
            {"intent": "list", "entity": "product", "fields": ["name"]},
        ],
    })
    assert_error(result, "MDBP_QUERY_UNION_REQUIRES_SUBQUERIES")
    print("  MDBP_QUERY_UNION_REQUIRES_SUBQUERIES (only 1) ✓")

    # ─── MDBP_NOT_FOUND ──────────────────────────────────────
    result = mdbp.query({"intent": "get", "entity": "product", "id": 99999})
    assert_error(result, "MDBP_NOT_FOUND", lambda d: (
        d["entity"] == "product" and
        d["id"] == 99999
    ))
    print("  MDBP_NOT_FOUND ✓")

    # ─── Verify all errors return structured format ───────────
    print("  ---")
    all_error_results = [
        mdbp.query({"intent": "invalid"}),                                          # validation
        mdbp.query({"intent": "list", "entity": "nope"}),                           # entity
        mdbp.query({"intent": "list", "entity": "product", "fields": ["nope"]}),    # field
        mdbp.query({"intent": "get", "entity": "product", "id": 99999}),            # not found
        mdbp.query({"intent": "aggregate", "entity": "product"}),                   # missing field
        mdbp.query({"intent": "union", "entity": "product"}),                       # union
    ]
    for r in all_error_results:
        assert r["success"] is False
        assert isinstance(r["error"], dict)
        assert isinstance(r["error"]["code"], str)
        assert r["error"]["code"].startswith("MDBP_")
        assert isinstance(r["error"]["message"], str)
        assert len(r["error"]["message"]) > 0
    print("  all errors: structured format (code, message) ✓")


def test_subquery():
    """Test subquery in filters ($query)."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Products in category "electronics" (via subquery)
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "price"],
        "filters": {
            "category_id__in": {
                "$query": {
                    "intent": "list",
                    "entity": "category",
                    "fields": ["id"],
                    "filters": {"name": "electronics"},
                }
            }
        },
    })
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Laptop" in names
    assert "Desk" not in names
    print("  subquery in filter ✓")


def test_exists():
    """Test EXISTS subquery."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Categories that have at least one product (EXISTS)
    result = mdbp.query({
        "intent": "list",
        "entity": "category",
        "where": {
            "logic": "and",
            "conditions": [{
                "op": "exists",
                "subquery": {
                    "entity": "product",
                    "fields": ["id"],
                    "join_on": {"category_id": "id"},
                },
            }],
        },
    })
    assert result["success"] is True
    assert len(result["data"]) == 2  # both categories have products
    print("  EXISTS ✓")


def test_case_when():
    """Test CASE WHEN computed fields."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "price"],
        "computed_fields": [{
            "name": "price_tier",
            "case": {
                "when": [
                    {"condition": {"field": "price", "op": "gt", "value": 500}, "then": "expensive"},
                    {"condition": {"field": "price", "op": "gt", "value": 100}, "then": "medium"},
                ],
                "else_value": "cheap",
            },
        }],
    })
    assert result["success"] is True
    for row in result["data"]:
        assert "price_tier" in row
    # Laptop (999) → expensive, Mouse (29) → cheap
    tiers = {r["name"]: r["price_tier"] for r in result["data"]}
    assert tiers["Laptop"] == "expensive"
    assert tiers["Mouse"] == "cheap"
    assert tiers["Desk"] == "medium"
    print("  CASE WHEN ✓")


def test_window_functions():
    """Test window functions: RANK, ROW_NUMBER."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # RANK by price descending
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "price"],
        "computed_fields": [{
            "name": "price_rank",
            "window": {
                "function": "rank",
                "order_by": [{"field": "price", "order": "desc"}],
            },
        }],
    })
    assert result["success"] is True
    ranks = {r["name"]: r["price_rank"] for r in result["data"]}
    assert ranks["Laptop"] == 1  # most expensive
    print("  RANK() ✓")

    # ROW_NUMBER partitioned by category
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "category_id"],
        "computed_fields": [{
            "name": "row_num",
            "window": {
                "function": "row_number",
                "partition_by": ["category_id"],
                "order_by": [{"field": "price", "order": "desc"}],
            },
        }],
    })
    assert result["success"] is True
    for row in result["data"]:
        assert "row_num" in row
    print("  ROW_NUMBER() OVER (PARTITION BY ...) ✓")


def test_scalar_functions():
    """Test scalar functions: COALESCE, UPPER, LOWER, LENGTH."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # COALESCE: replace NULL description with 'N/A'
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name"],
        "computed_fields": [{
            "name": "desc_safe",
            "function": {"name": "coalesce", "args": ["description", {"literal": "N/A"}]},
        }],
    })
    assert result["success"] is True
    descs = {r["name"]: r["desc_safe"] for r in result["data"]}
    assert descs["Mouse"] == "N/A"  # was NULL
    assert descs["Laptop"] == "High-end laptop"
    print("  COALESCE ✓")

    # UPPER
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name"],
        "computed_fields": [{
            "name": "upper_name",
            "function": {"name": "upper", "args": ["name"]},
        }],
        "limit": 1,
    })
    assert result["success"] is True
    assert result["data"][0]["upper_name"] == result["data"][0]["name"].upper()
    print("  UPPER ✓")

    # LENGTH
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name"],
        "computed_fields": [{
            "name": "name_len",
            "function": {"name": "length", "args": ["name"]},
        }],
        "limit": 1,
    })
    assert result["success"] is True
    assert result["data"][0]["name_len"] == len(result["data"][0]["name"])
    print("  LENGTH ✓")


def test_self_join():
    """Test self-join via alias."""
    db_url = setup_test_db()

    # Create employees table with manager_id
    from sqlalchemy import Column, Integer, String, Table, MetaData, create_engine, insert
    engine = create_engine(db_url)
    meta = MetaData()
    employees = Table(
        "employees", meta,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("manager_id", Integer, nullable=True),
    )
    meta.create_all(engine)
    with engine.connect() as conn:
        conn.execute(insert(employees).values([
            {"id": 1, "name": "Alice", "manager_id": None},
            {"id": 2, "name": "Bob", "manager_id": 1},
            {"id": 3, "name": "Charlie", "manager_id": 1},
        ]))
        conn.commit()

    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "list",
        "entity": "employee",
        "fields": ["name", "manager.name"],
        "join": [{"entity": "employee", "alias": "manager", "type": "left", "on": {"manager_id": "id"}}],
    })
    assert result["success"] is True
    data = {r["name"]: r.get("name_1") for r in result["data"]}
    # Bob and Charlie's manager is Alice
    assert data["Bob"] == "Alice"
    assert data["Charlie"] == "Alice"
    assert data["Alice"] is None  # no manager
    print("  Self-JOIN ✓")


def test_cte():
    """Test CTE (Common Table Expressions)."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # CTE: get electronics category id, then filter products
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name"],
        "cte": [{
            "name": "elec_cats",
            "query": {
                "intent": "list",
                "entity": "category",
                "fields": ["id"],
                "filters": {"name": "electronics"},
            },
        }],
        "filters": {
            "category_id__in": {"$cte": "elec_cats", "field": "id"},
        },
    })
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Laptop" in names
    assert "Desk" not in names
    print("  CTE ✓")


def test_batch_create():
    """Test batch INSERT (multiple rows)."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "batch_create",
        "entity": "product",
        "rows": [
            {"id": 10, "name": "Keyboard", "price": 79.99, "category_id": 1, "stock": 100},
            {"id": 11, "name": "Webcam", "price": 49.99, "category_id": 1, "stock": 80},
            {"id": 12, "name": "Headset", "price": 129.99, "category_id": 1, "stock": 60},
        ],
    })
    assert result["success"] is True
    print("  batch_create ✓")

    # Verify all 3 were inserted
    result = mdbp.query({"intent": "count", "entity": "product"})
    assert result["data"]["count"] == 8  # 5 original + 3 new
    print("  batch_create verified ✓")


def test_upsert():
    """Test UPSERT (INSERT ON CONFLICT DO UPDATE)."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Insert new
    result = mdbp.query({
        "intent": "upsert",
        "entity": "product",
        "data": {"id": 20, "name": "Tablet", "price": 599.99, "category_id": 1, "stock": 40},
        "conflict_target": ["id"],
    })
    assert result["success"] is True
    print("  upsert (insert) ✓")

    # Upsert existing → update
    result = mdbp.query({
        "intent": "upsert",
        "entity": "product",
        "data": {"id": 20, "name": "Tablet Pro", "price": 699.99, "category_id": 1, "stock": 35},
        "conflict_target": ["id"],
    })
    assert result["success"] is True

    # Verify updated
    result = mdbp.query({"intent": "get", "entity": "product", "id": 20})
    assert result["success"] is True
    assert result["data"]["name"] == "Tablet Pro"
    print("  upsert (update) ✓")


def test_multiple_aggregations():
    """Test multiple aggregations in one query: SELECT SUM(x), AVG(y), COUNT(z)."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    result = mdbp.query({
        "intent": "aggregate",
        "entity": "product",
        "aggregations": [
            {"op": "sum", "field": "price"},
            {"op": "avg", "field": "price"},
            {"op": "count", "field": "id"},
            {"op": "min", "field": "price"},
            {"op": "max", "field": "price"},
        ],
    })
    assert result["success"] is True
    row = result["data"][0]
    assert "sum_price" in row
    assert "avg_price" in row
    assert "count_id" in row
    assert "min_price" in row
    assert "max_price" in row
    print("  multiple aggregations ✓")

    # With GROUP BY
    result = mdbp.query({
        "intent": "aggregate",
        "entity": "product",
        "aggregations": [
            {"op": "count", "field": "id"},
            {"op": "avg", "field": "price"},
        ],
        "group_by": ["category_id"],
    })
    assert result["success"] is True
    assert len(result["data"]) == 2  # 2 categories
    print("  multiple aggregations + GROUP BY ✓")


def test_intersect_except():
    """Test INTERSECT and EXCEPT."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # INTERSECT: products that are both electronics AND price > 100
    result = mdbp.query({
        "intent": "intersect",
        "entity": "product",
        "union_queries": [
            {"intent": "list", "entity": "product", "fields": ["name"], "filters": {"category_id": 1}},
            {"intent": "list", "entity": "product", "fields": ["name"], "filters": {"price__gt": 100}},
        ],
    })
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Laptop" in names
    assert "Monitor" in names
    assert "Mouse" not in names  # price 29.99, not > 100
    print("  INTERSECT ✓")

    # EXCEPT: electronics minus expensive (> 400)
    result = mdbp.query({
        "intent": "except",
        "entity": "product",
        "union_queries": [
            {"intent": "list", "entity": "product", "fields": ["name"], "filters": {"category_id": 1}},
            {"intent": "list", "entity": "product", "fields": ["name"], "filters": {"price__gt": 400}},
        ],
    })
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Mouse" in names  # electronics but not > 400
    assert "Laptop" not in names  # > 400, excluded
    print("  EXCEPT ✓")


def test_dry_run():
    """Test dry_run mode: returns SQL without executing."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Dry-run returns SQL + params, no execution
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "fields": ["name", "price"],
        "filters": {"price__gte": 100},
        "limit": 5,
        "dry_run": True,
    })
    assert result["success"] is True
    assert result["dry_run"] is True
    assert "sql" in result
    assert "params" in result
    assert "data" not in result
    assert "SELECT" in result["sql"]
    assert "products" in result["sql"]
    print("  dry_run list ✓")

    # Dry-run on write: SQL returned but NOT executed
    result = mdbp.query({
        "intent": "delete",
        "entity": "product",
        "filters": {"id": 1},
        "dry_run": True,
    })
    assert result["success"] is True
    assert result["dry_run"] is True
    assert "DELETE" in result["sql"]
    print("  dry_run delete (no execution) ✓")

    # Verify no row was deleted
    count = mdbp.query({"intent": "count", "entity": "product"})
    assert count["data"]["count"] == 5
    print("  data unchanged after dry_run ✓")

    # Dry-run still enforces schema validation
    result = mdbp.query({
        "intent": "list",
        "entity": "nonexistent",
        "dry_run": True,
    })
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_SCHEMA_ENTITY_NOT_FOUND"
    print("  dry_run schema validation ✓")

    # Dry-run still enforces policy
    from mdbp.core.policy import Policy
    mdbp.add_policy(Policy(entity="product", role="viewer", allowed_intents=["list"]))
    result = mdbp.query({
        "intent": "delete",
        "entity": "product",
        "role": "viewer",
        "dry_run": True,
    })
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_POLICY_INTENT_NOT_ALLOWED"
    print("  dry_run policy enforcement ✓")

    # Without dry_run, normal execution (backward compat)
    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "limit": 1,
    })
    assert result["success"] is True
    assert "data" in result
    assert "dry_run" not in result
    print("  backward compat ✓")


def test_update_with_join():
    """Test UPDATE ... FROM (join-based update)."""
    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # Update all furniture products: set stock = 0
    result = mdbp.query({
        "intent": "update",
        "entity": "product",
        "data": {"stock": 0},
        "from_entity": "category",
        "from_join_on": {"category_id": "id"},
        "from_filters": {"name": "furniture"},
    })
    assert result["success"] is True
    print("  UPDATE with JOIN ✓")

    # Verify: furniture products have stock=0
    result = mdbp.query({"intent": "list", "entity": "product", "filters": {"stock": 0}})
    assert result["success"] is True
    names = {r["name"] for r in result["data"]}
    assert "Desk" in names
    assert "Chair" in names
    assert "Laptop" not in names
    print("  UPDATE with JOIN verified ✓")


def test_data_masking():
    """Test data masking on query results."""
    from mdbp.core.masking import MaskingRule

    db_url = setup_test_db()
    mdbp = MDBP(db_url=db_url)

    # ── partial masking (list) ──
    mdbp.add_policy(Policy(
        entity="user",
        role="viewer",
        masked_fields={"username": "partial", "email": "email"},
    ))

    result = mdbp.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
        "role": "viewer",
    })
    assert result["success"] is True
    for row in result["data"]:
        # partial: first and last char visible, middle masked
        assert "*" in row["username"]
        assert row["username"][0] in ("a", "b")  # alice→a***e, bob→b*b
        # email: local part masked, domain intact
        assert "@" in row["email"]
        assert "***" in row["email"].split("@")[0]
        assert "test.com" in row["email"]
    print("  partial + email masking (list) ✓")

    # ── get intent masking ──
    result = mdbp.query({
        "intent": "get",
        "entity": "user",
        "id": 1,
        "fields": ["username", "email"],
        "role": "viewer",
    })
    assert result["success"] is True
    assert "*" in result["data"]["username"]
    assert "***" in result["data"]["email"]
    print("  masking on get intent ✓")

    # ── redact masking ──
    mdbp2 = MDBP(db_url=db_url)
    mdbp2.add_policy(Policy(
        entity="user",
        role="*",
        masked_fields={"password_hash": "redact"},
    ))

    result = mdbp2.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "password_hash"],
    })
    assert result["success"] is True
    for row in result["data"]:
        assert row["password_hash"] == "***"
        assert row["username"] in ("alice", "bob")  # unmasked field untouched
    print("  redact masking ✓")

    # ── last_n masking ──
    mdbp3 = MDBP(db_url=db_url)
    mdbp3.add_policy(Policy(
        entity="user",
        role="*",
        masked_fields={"email": MaskingRule(strategy="last_n", options={"n": 8})},
    ))

    result = mdbp3.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
    })
    assert result["success"] is True
    for row in result["data"]:
        # last 8 chars visible, rest masked
        assert row["email"].endswith("test.com")
        assert "*" in row["email"]
    print("  last_n masking ✓")

    # ── first_n masking ──
    mdbp4 = MDBP(db_url=db_url)
    mdbp4.add_policy(Policy(
        entity="user",
        role="*",
        masked_fields={"email": MaskingRule(strategy="first_n", options={"n": 3})},
    ))

    result = mdbp4.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
    })
    assert result["success"] is True
    for row in result["data"]:
        # first 3 chars visible, rest masked
        assert "*" in row["email"]
    print("  first_n masking ✓")

    # ── hash masking ──
    mdbp5 = MDBP(db_url=db_url)
    mdbp5.add_policy(Policy(
        entity="user",
        role="*",
        masked_fields={"email": "hash"},
    ))

    result = mdbp5.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
    })
    assert result["success"] is True
    for row in result["data"]:
        assert len(row["email"]) == 8  # default hash length
        assert "@" not in row["email"]  # no longer an email
    print("  hash masking ✓")

    # ── callable masking ──
    mdbp6 = MDBP(db_url=db_url)
    mdbp6.add_policy(Policy(
        entity="user",
        role="*",
        masked_fields={"email": lambda v: v.split("@")[0][0] + "***@" + v.split("@")[1]},
    ))

    result = mdbp6.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
    })
    assert result["success"] is True
    assert result["data"][0]["email"] == "a***@test.com"
    assert result["data"][1]["email"] == "b***@test.com"
    print("  callable masking ✓")

    # ── None values are not masked ──
    mdbp7 = MDBP(db_url=db_url)
    mdbp7.add_policy(Policy(
        entity="product",
        role="*",
        masked_fields={"description": "redact"},
    ))

    result = mdbp7.query({
        "intent": "get",
        "entity": "product",
        "id": 2,
        "fields": ["name", "description"],
    })
    assert result["success"] is True
    assert result["data"]["description"] is None  # None stays None
    print("  None value not masked ✓")

    # ── No masked_fields → data unchanged (backward compat) ──
    mdbp8 = MDBP(db_url=db_url)
    mdbp8.add_policy(Policy(
        entity="user",
        role="*",
        denied_fields=["password_hash"],
    ))

    result = mdbp8.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
    })
    assert result["success"] is True
    assert result["data"][0]["email"] == "alice@test.com"  # raw, unmasked
    print("  backward compatibility (no masking) ✓")

    # ── masked_fields + denied_fields coexist ──
    mdbp9 = MDBP(db_url=db_url)
    mdbp9.add_policy(Policy(
        entity="user",
        role="*",
        denied_fields=["password_hash"],
        masked_fields={"email": "email"},
    ))

    # denied field still blocked
    result = mdbp9.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "password_hash"],
    })
    assert result["success"] is False
    assert result["error"]["code"] == "MDBP_POLICY_FIELD_DENIED"

    # masked field works
    result = mdbp9.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email"],
    })
    assert result["success"] is True
    assert "***" in result["data"][0]["email"]
    print("  masked_fields + denied_fields coexist ✓")


def test_audit_logging():
    """Test audit logging with CallbackAuditLogger."""
    from mdbp.core.audit import CallbackAuditLogger, AuditEntry
    from mdbp.core.masking import MaskingRule

    db_url = setup_test_db()
    entries: list[AuditEntry] = []
    logger = CallbackAuditLogger(lambda e: entries.append(e))

    mdbp = MDBP(db_url=db_url, audit=logger)

    # ── Successful list query ──
    result = mdbp.query({"intent": "list", "entity": "product", "limit": 5})
    assert result["success"] is True
    assert len(entries) == 1
    e = entries[0]
    assert e.intent_type == "list"
    assert e.entity == "product"
    assert e.success is True
    assert e.error_code is None
    assert e.row_count == 5
    assert e.duration_ms > 0
    assert e.dry_run is False
    assert e.timestamp  # ISO 8601 string
    print("  successful query audit ✓")

    # ── Failed query (unknown entity) ──
    entries.clear()
    result = mdbp.query({"intent": "list", "entity": "nonexistent"})
    assert result["success"] is False
    assert len(entries) == 1
    e = entries[0]
    assert e.success is False
    assert e.error_code == "MDBP_SCHEMA_ENTITY_NOT_FOUND"
    assert e.row_count is None
    assert e.duration_ms > 0
    print("  failed query audit ✓")

    # ── Dry-run ──
    entries.clear()
    result = mdbp.query({"intent": "list", "entity": "product", "dry_run": True})
    assert result["success"] is True
    assert len(entries) == 1
    e = entries[0]
    assert e.success is True
    assert e.dry_run is True
    assert e.row_count is None
    print("  dry-run audit ✓")

    # ── Query with masking ──
    entries.clear()
    mdbp2 = MDBP(db_url=db_url, audit=logger)
    mdbp2.add_policy(Policy(
        entity="user",
        role="*",
        masked_fields={"email": "email", "password_hash": "redact"},
    ))
    result = mdbp2.query({
        "intent": "list",
        "entity": "user",
        "fields": ["username", "email", "password_hash"],
    })
    assert result["success"] is True
    assert len(entries) == 1
    e = entries[0]
    assert "email" in e.masked_fields
    assert "password_hash" in e.masked_fields
    print("  masked fields in audit ✓")

    # ── Role logged ──
    entries.clear()
    mdbp3 = MDBP(db_url=db_url, audit=logger)
    result = mdbp3.query({"intent": "list", "entity": "product", "role": "analyst"})
    assert len(entries) == 1
    assert entries[0].role == "analyst"
    print("  role in audit ✓")

    # ── No audit logger → no error (backward compat) ──
    mdbp_no_audit = MDBP(db_url=db_url)
    result = mdbp_no_audit.query({"intent": "list", "entity": "product"})
    assert result["success"] is True
    print("  backward compatibility (no audit) ✓")


if __name__ == "__main__":
    tests = [
        ("Basic operations", test_basic_operations),
        ("Filter operators", test_filter_operators),
        ("OR / AND / NOT conditions", test_or_conditions),
        ("DISTINCT", test_distinct),
        ("JOIN", test_join),
        ("HAVING", test_having),
        ("UNION", test_union),
        ("Write operations (CREATE/UPDATE/DELETE)", test_write_operations),
        ("allowed_intents", test_allowed_intents),
        ("Policy enforcement", test_policy),
        ("Hallucination protection", test_hallucination_protection),
        ("Sort + pagination", test_sort_and_pagination),
        ("Error codes", test_error_codes),
        ("Subquery ($query)", test_subquery),
        ("EXISTS", test_exists),
        ("CASE WHEN", test_case_when),
        ("Window functions", test_window_functions),
        ("Scalar functions", test_scalar_functions),
        ("Self-JOIN", test_self_join),
        ("CTE", test_cte),
        ("Batch CREATE", test_batch_create),
        ("UPSERT", test_upsert),
        ("Multiple aggregations", test_multiple_aggregations),
        ("INTERSECT / EXCEPT", test_intersect_except),
        ("UPDATE with JOIN", test_update_with_join),
        ("Dry-run mode", test_dry_run),
        ("Data masking", test_data_masking),
        ("Audit logging", test_audit_logging),
    ]

    passed = 0
    failed = 0
    for name, test_fn in tests:
        print(f"\n{'='*50}")
        print(f"TEST: {name}")
        print(f"{'='*50}")
        try:
            test_fn()
            passed += 1
            print(f"  --> PASSED")
        except Exception as e:
            failed += 1
            print(f"  --> FAILED: {e}")

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)}")
    if failed == 0:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")

    # Cleanup
    import os
    import gc
    gc.collect()
    # Dispose any remaining SQLAlchemy engines to release file locks on Windows
    temp_engine = create_engine(DB_URL)
    temp_engine.dispose()
    gc.collect()
    try:
        os.remove("test_mdbp.db")
    except (FileNotFoundError, PermissionError):
        pass
