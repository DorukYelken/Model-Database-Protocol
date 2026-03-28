"""
MDBP Intent Model

LLM produces intents, not SQL. This module defines the structured
intent format that MDBP understands.

Supports ALL SQL operations:
  - list, get, count, aggregate, create, update, delete, union
  - JOIN (inner/left/right/full) + self-join via alias
  - DISTINCT
  - OR / AND / NOT nested conditions
  - IS NULL / IS NOT NULL / BETWEEN
  - HAVING
  - SUBQUERY in filters ($query) and EXISTS
  - CASE WHEN / COALESCE / UPPER / LOWER / CAST / etc.
  - Window functions (RANK, ROW_NUMBER, LAG, LEAD, etc.)
  - CTE (Common Table Expressions)
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ─── Intent Types ───────────────────────────────────────────────

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


# ─── Sort ───────────────────────────────────────────────────────

class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


class SortField(BaseModel):
    field: str
    order: SortOrder = SortOrder.ASC


# ─── Aggregation ────────────────────────────────────────────────

class AggregateOp(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class Aggregation(BaseModel):
    op: AggregateOp
    field: str


# ─── JOIN ───────────────────────────────────────────────────────

class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class JoinSpec(BaseModel):
    """
    Supports self-join via alias:
        {"entity": "employee", "alias": "manager", "type": "left", "on": {"manager_id": "id"}}
    """
    entity: str
    alias: str | None = Field(default=None, description="Alias for self-joins or disambiguation")
    type: JoinType = JoinType.INNER
    on: dict[str, str] = Field(description="Mapping of local_field → foreign_field")


# ─── Complex Conditions (OR / AND / NOT / nested) ──────────────

class FilterCondition(BaseModel):
    """
    Supported ops:
        eq, ne, gt, gte, lt, lte,
        like, ilike, not_like,
        in, not_in, between,
        null, not_null,
        exists
    """
    field: str | None = None
    op: str = "eq"
    value: Any = None
    subquery: dict | None = Field(default=None, description="Subquery for EXISTS op")


class FilterGroup(BaseModel):
    logic: Literal["and", "or", "not"] = "and"
    conditions: list[FilterCondition | FilterGroup] = Field(default_factory=list)


# ─── HAVING ─────────────────────────────────────────────────────

class HavingCondition(BaseModel):
    op: AggregateOp
    field: str
    condition: str = "gt"
    value: Any = None


# ─── Computed Fields (CASE WHEN, Window Functions, Scalar Functions) ──

class CaseCondition(BaseModel):
    """A single WHEN ... THEN clause."""
    condition: FilterCondition
    then: Any


class CaseExpression(BaseModel):
    """
    CASE WHEN price > 500 THEN 'expensive'
         WHEN price > 100 THEN 'medium'
         ELSE 'cheap' END
    """
    when: list[CaseCondition]
    else_value: Any = None


class WindowSpec(BaseModel):
    """
    Window function: RANK() OVER (PARTITION BY category ORDER BY price DESC)

    Supported functions:
        rank, dense_rank, row_number,
        lag, lead, first_value, last_value, ntile,
        sum, avg, min, max, count
    """
    function: str
    partition_by: list[str] | None = None
    order_by: list[SortField] | None = None
    args: list[Any] | None = Field(default=None, description="Function args, e.g. [1] for LAG(field, 1)")


class FunctionCall(BaseModel):
    """
    Scalar SQL functions.

    Examples:
        {"name": "coalesce", "args": ["description", {"literal": "N/A"}]}
        {"name": "upper", "args": ["name"]}
        {"name": "cast", "args": ["price"], "cast_to": "integer"}
        {"name": "concat", "args": ["first_name", {"literal": " "}, "last_name"]}
        {"name": "round", "args": ["price", {"literal": 2}]}
        {"name": "extract", "args": [{"literal": "year"}, "created_at"]}
        {"name": "substring", "args": ["name", {"literal": 1}, {"literal": 3}]}

    Supported: coalesce, upper, lower, cast, concat, trim, length, abs,
               round, substring, extract, now, current_date, replace
    """
    name: str
    args: list[Any] = Field(default_factory=list, description="Field names (str) or literals ({literal: value})")
    cast_to: str | None = Field(default=None, description="Target type for CAST")


class ComputedField(BaseModel):
    """
    A computed/derived column added to the SELECT.

    Exactly one of case, window, or function must be set.

    Examples:
        CASE WHEN:
            {"name": "tier", "case": {"when": [{"condition": {"field": "price", "op": "gt", "value": 500}, "then": "expensive"}], "else_value": "cheap"}}

        Window:
            {"name": "rank", "window": {"function": "rank", "partition_by": ["category_id"], "order_by": [{"field": "price", "order": "desc"}]}}

        Function:
            {"name": "full_name", "function": {"name": "concat", "args": ["first_name", {"literal": " "}, "last_name"]}}
    """
    name: str = Field(description="Alias name for the computed column")
    case: CaseExpression | None = None
    window: WindowSpec | None = None
    function: FunctionCall | None = None


# ─── CTE (Common Table Expressions) ────────────────────────────

class CTEDefinition(BaseModel):
    """
    WITH [RECURSIVE] name AS (query [UNION ALL recursive_query])

    Normal:
        {"name": "top_spenders", "query": {"intent": "aggregate", ...}}

    Recursive:
        {"name": "tree", "recursive": true,
         "query": {"intent": "list", "entity": "employee", "filters": {"manager_id__null": true}},
         "recursive_query": {"intent": "list", "entity": "employee",
                             "join": [{"entity": "tree", ...}]}}
    """
    name: str
    query: Intent
    recursive: bool = False
    recursive_query: Intent | None = Field(default=None, description="The recursive part (UNION ALL with the base query)")


# ─── Main Intent ────────────────────────────────────────────────

class Intent(BaseModel):
    """
    The core MDBP intent object.

    Simple:
        {"intent": "list", "entity": "order", "filters": {"status": "pending"}}

    Subquery in filter:
        {"intent": "list", "entity": "order",
         "filters": {"customer_id__in": {"$query": {"intent": "list", "entity": "customer", "fields": ["id"], "filters": {"vip": true}}}}}

    CASE + Window + JOIN:
        {"intent": "list", "entity": "product",
         "fields": ["name", "price"],
         "computed_fields": [
             {"name": "tier", "case": {"when": [...], "else_value": "cheap"}},
             {"name": "rank", "window": {"function": "rank", "order_by": [{"field": "price", "order": "desc"}]}}
         ],
         "join": [{"entity": "category", "on": {"category_id": "id"}}]}

    CTE:
        {"intent": "list", "entity": "customer",
         "cte": [{"name": "vips", "query": {"intent": "list", "entity": "customer", "fields": ["id"], "filters": {"vip": true}}}],
         "filters": {"id__in": {"$cte": "vips", "field": "id"}}}
    """

    intent: IntentType
    entity: str = Field(description="Logical entity name")

    # ── Read ──
    filters: dict[str, Any] = Field(default_factory=dict, description="Simple AND filters. Values can be literals or {$query: ...} for subqueries")
    where: FilterGroup | None = Field(default=None, description="Complex nested conditions (AND/OR/NOT/EXISTS)")
    fields: list[str] | None = Field(default=None, description="Fields to return. 'entity.field' for JOINs")
    sort: list[SortField] | None = None
    limit: int | None = None
    offset: int | None = None
    distinct: bool = False

    # ── Get by ID ──
    id: Any | None = None

    # ── JOIN ──
    join: list[JoinSpec] | None = None

    # ── Aggregation ──
    aggregation: Aggregation | None = None
    aggregations: list[Aggregation] | None = Field(default=None, description="Multiple aggregations: SELECT SUM(x), AVG(y), COUNT(z)")
    group_by: list[str] | None = None
    group_by_mode: Literal["simple", "rollup", "cube", "grouping_sets"] | None = Field(default=None, description="Advanced GROUP BY: rollup, cube, or grouping_sets")
    grouping_sets: list[list[str]] | None = Field(default=None, description="Explicit grouping sets for group_by_mode='grouping_sets'")
    having: list[HavingCondition] | None = None

    # ── Computed Fields (CASE, Window, Functions) ──
    computed_fields: list[ComputedField] | None = None

    # ── CTE ──
    cte: list[CTEDefinition] | None = None

    # ── Write ──
    data: dict[str, Any] | None = Field(default=None, description="Data for create/update/upsert")
    rows: list[dict[str, Any]] | None = Field(default=None, description="Multiple rows for batch_create")
    returning: list[str] | None = Field(default=None, description="Fields to return after INSERT/UPDATE/DELETE (RETURNING clause)")

    # ── UPSERT ──
    conflict_target: list[str] | None = Field(default=None, description="Columns for ON CONFLICT (upsert)")
    conflict_update: list[str] | None = Field(default=None, description="Columns to update on conflict. None = update all from data")

    # ── UPDATE/DELETE with JOIN ──
    from_entity: str | None = Field(default=None, description="Secondary entity for UPDATE ... FROM or DELETE ... USING")
    from_join_on: dict[str, str] | None = Field(default=None, description="Join condition: {local_field: foreign_field}")
    from_filters: dict[str, Any] | None = Field(default=None, description="Filters on the secondary entity")

    # ── UNION / INTERSECT / EXCEPT ──
    union_queries: list[Intent] | None = None
    union_all: bool = False

    # ── Metadata ──
    role: str | None = None
