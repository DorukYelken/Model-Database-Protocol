"""
MDCP Query Planner

Converts a validated Intent into a SQLAlchemy query.
Never produces raw SQL strings — always uses parameterized queries.

Supports ALL SQL operations:
  SELECT, INSERT, UPDATE, DELETE,
  JOIN (inner/left/right/full + self-join), DISTINCT, GROUP BY, HAVING,
  UNION / UNION ALL, subqueries ($query), EXISTS,
  AND/OR/NOT nested conditions, IS NULL, BETWEEN, LIKE, IN,
  CASE WHEN, Window functions (RANK, ROW_NUMBER, etc.),
  CTE (WITH ... AS), scalar functions (COALESCE, UPPER, CAST, etc.)
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    Column,
    MetaData,
    Table,
    and_,
    case,
    cast as sa_cast,
    delete,
    distinct as sa_distinct,
    exists,
    func,
    insert,
    literal,
    not_,
    or_,
    select,
    union,
    union_all,
    update,
)
from sqlalchemy.types import Integer, Numeric, Text, Boolean, DateTime

from mdcp.core.errors import (
    EntityReferenceError,
    MissingRequiredFieldError,
    QueryPlanError,
    UnionRequiresSubqueriesError,
    UnknownFilterOpError,
)
from mdcp.core.intent import (
    AggregateOp,
    ComputedField,
    FilterCondition,
    FilterGroup,
    Intent,
    IntentType,
)
from mdcp.core.schema_registry import SchemaRegistry


# ─── Operator Mappings ──────────────────────────────────────────

FILTER_OPS: dict[str, str] = {
    "__gt": "__gt__",
    "__gte": "__ge__",
    "__lt": "__lt__",
    "__lte": "__le__",
    "__ne": "__ne__",
    "__like": "like",
    "__ilike": "ilike",
    "__not_like": "notlike",
    "__in": "in_",
    "__not_in": "not_in",
}

CONDITION_OPS: dict[str, str] = {
    "eq": "__eq__", "ne": "__ne__",
    "gt": "__gt__", "gte": "__ge__",
    "lt": "__lt__", "lte": "__le__",
    "like": "like", "ilike": "ilike", "not_like": "notlike",
    "in": "in_", "not_in": "not_in",
}

COMPARISON_OPS: dict[str, str] = {
    "eq": "__eq__", "ne": "__ne__",
    "gt": "__gt__", "gte": "__ge__",
    "lt": "__lt__", "lte": "__le__",
}

AGG_FUNCS = {
    AggregateOp.SUM: func.sum, AggregateOp.AVG: func.avg,
    AggregateOp.MIN: func.min, AggregateOp.MAX: func.max,
    AggregateOp.COUNT: func.count,
}

CAST_TYPES = {
    "integer": Integer, "int": Integer,
    "numeric": Numeric, "float": Numeric, "decimal": Numeric,
    "text": Text, "string": Text, "varchar": Text,
    "boolean": Boolean, "bool": Boolean,
    "datetime": DateTime, "timestamp": DateTime,
}

WINDOW_FUNCS = {
    "rank": func.rank,
    "dense_rank": func.dense_rank,
    "row_number": func.row_number,
    "ntile": func.ntile,
    "lag": func.lag,
    "lead": func.lead,
    "first_value": func.first_value,
    "last_value": func.last_value,
    "sum": func.sum, "avg": func.avg,
    "min": func.min, "max": func.max, "count": func.count,
}

SCALAR_FUNCS = {
    "coalesce": func.coalesce,
    "upper": func.upper,
    "lower": func.lower,
    "trim": func.trim,
    "length": func.length,
    "abs": func.abs,
    "round": func.round,
    "concat": func.concat,
    "replace": func.replace,
    "substring": func.substring,
    "now": func.now,
    "current_date": func.current_date,
}


class QueryPlanner:
    """Builds SQLAlchemy statements from MDCP intents."""

    def __init__(self, registry: SchemaRegistry, metadata: MetaData, dialect: str = "default") -> None:
        self.registry = registry
        self.metadata = metadata
        self.dialect = dialect
        self._table_cache: dict[str, Table] = {}

    # ─── Table / Column Resolution ──────────────────────────────

    def _get_table(self, entity: str) -> Table:
        table_name = self.registry.resolve_table(entity)
        if table_name not in self._table_cache:
            self._table_cache[table_name] = self.metadata.tables[table_name]
        return self._table_cache[table_name]

    def _get_column(self, table: Table, entity: str, field: str) -> Column:
        col_name = self.registry.resolve_column(entity, field)
        return table.c[col_name]

    def _resolve_field(self, intent: Intent, field: str, table: Table, joined: dict) -> Column:
        """Resolve dot-notation: 'category.name' → joined table column."""
        if "." in field:
            entity_ref, field_name = field.split(".", 1)
            if entity_ref in joined:
                jtable, jentity = joined[entity_ref]
                return self._get_column(jtable, jentity, field_name)
            raise EntityReferenceError(entity_ref=entity_ref, field=field)
        return self._get_column(table, intent.entity, field)

    def _resolve_func_arg(self, arg: Any, table: Table, entity: str) -> Any:
        """Resolve a function argument: string = field name, dict with 'literal' = literal value."""
        if isinstance(arg, dict) and "literal" in arg:
            return literal(arg["literal"])
        if isinstance(arg, str):
            return self._get_column(table, entity, arg)
        return literal(arg)

    # ─── Joined Tables Map ──────────────────────────────────────

    def _build_joined_tables_map(self, intent: Intent) -> dict[str, tuple[Table, str]]:
        """Pre-build joined tables map. Aliased tables are created once and reused."""
        joined: dict[str, tuple[Table, str]] = {}
        if intent.join:
            for j in intent.join:
                target_table = self._get_table(j.entity)
                if j.alias:
                    aliased_table = target_table.alias(j.alias)
                    joined[j.alias] = (aliased_table, j.entity)
                else:
                    joined[j.entity] = (target_table, j.entity)
        return joined

    # ─── Simple Filters (dict with suffixes + $query) ───────────

    def _apply_filters(self, stmt, table: Table, entity: str, filters: dict[str, Any]):
        conditions = self._build_simple_conditions(table, entity, filters)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        return stmt

    def _build_simple_conditions(self, table: Table, entity: str, filters: dict[str, Any]) -> list:
        conditions = []
        for key, value in filters.items():
            # Subquery value: {"$query": {...}}
            if isinstance(value, dict) and "$query" in value:
                sub = self._build_subquery(value["$query"])
                field = self._strip_suffix(key)
                col = self._get_column(table, entity, field)
                if "__in" in key or "__not_in" in key:
                    if "__not_in" in key:
                        conditions.append(col.not_in(sub))
                    else:
                        conditions.append(col.in_(sub))
                else:
                    conditions.append(col.in_(sub))
                continue

            # CTE reference: {"$cte": "name", "field": "col"}
            if isinstance(value, dict) and "$cte" in value:
                # Will be resolved after CTE is built
                conditions.append(("$cte", key, value))
                continue

            # __null / __not_null
            if key.endswith("__null"):
                field = key[:-6]
                col = self._get_column(table, entity, field)
                conditions.append(col.is_(None) if value else col.is_not(None))
                continue
            if key.endswith("__not_null"):
                field = key[:-10]
                col = self._get_column(table, entity, field)
                conditions.append(col.is_not(None))
                continue

            # __between
            if key.endswith("__between"):
                field = key[:-9]
                col = self._get_column(table, entity, field)
                conditions.append(col.between(value[0], value[1]))
                continue

            # Other operators
            op_found = False
            for suffix, method in FILTER_OPS.items():
                if key.endswith(suffix):
                    field = key[: -len(suffix)]
                    col = self._get_column(table, entity, field)
                    conditions.append(getattr(col, method)(value))
                    op_found = True
                    break

            if not op_found:
                col = self._get_column(table, entity, key)
                conditions.append(col == value)

        return conditions

    def _strip_suffix(self, key: str) -> str:
        """Strip operator suffix from filter key to get the field name."""
        for suffix in list(FILTER_OPS.keys()) + ["__null", "__not_null", "__between"]:
            if key.endswith(suffix):
                return key[: -len(suffix)]
        return key

    # ─── Subquery Builder ───────────────────────────────────────

    def _build_subquery(self, raw: dict) -> Any:
        """Build a subquery from a $query dict."""
        sub_intent = Intent(**raw)
        sub_table = self._get_table(sub_intent.entity)
        sub_stmt = self._plan_list(sub_intent, sub_table)
        return sub_stmt.scalar_subquery() if sub_intent.fields and len(sub_intent.fields) == 1 else sub_stmt

    # ─── Complex Conditions (AND/OR/NOT/EXISTS) ─────────────────

    def _build_where_clause(self, group: FilterGroup, table: Table, entity: str):
        parts = []
        for cond in group.conditions:
            if isinstance(cond, FilterGroup):
                parts.append(self._build_where_clause(cond, table, entity))
            elif isinstance(cond, FilterCondition):
                parts.append(self._build_single_condition(cond, table, entity))

        if not parts:
            return True

        if group.logic == "and":
            return and_(*parts)
        elif group.logic == "or":
            return or_(*parts)
        elif group.logic == "not":
            return not_(and_(*parts))
        return and_(*parts)

    def _build_single_condition(self, cond: FilterCondition, table: Table, entity: str):
        # EXISTS
        if cond.op == "exists" and cond.subquery:
            sub_entity = cond.subquery["entity"]
            sub_table = self._get_table(sub_entity)
            sub_fields = cond.subquery.get("fields")
            sub_filters = cond.subquery.get("filters", {})

            if sub_fields:
                sub_cols = [self._get_column(sub_table, sub_entity, f) for f in sub_fields]
            else:
                sub_cols = [literal(1)]

            sub_stmt = select(*sub_cols).select_from(sub_table)

            # Apply subquery's own filters
            if sub_filters:
                sub_stmt = self._apply_filters(sub_stmt, sub_table, sub_entity, sub_filters)

            # Correlation: join_on links inner field to outer field
            if cond.subquery.get("join_on"):
                for inner_field, outer_field in cond.subquery["join_on"].items():
                    inner_col = self._get_column(sub_table, sub_entity, inner_field)
                    outer_col = self._get_column(table, entity, outer_field)
                    sub_stmt = sub_stmt.where(inner_col == outer_col)

            return exists(sub_stmt)

        # NULL checks
        if cond.op == "null":
            col = self._get_column(table, entity, cond.field)
            return col.is_(None)
        if cond.op == "not_null":
            col = self._get_column(table, entity, cond.field)
            return col.is_not(None)
        if cond.op == "between":
            col = self._get_column(table, entity, cond.field)
            return col.between(cond.value[0], cond.value[1])

        method = CONDITION_OPS.get(cond.op)
        if method is None:
            raise UnknownFilterOpError(op=cond.op)

        col = self._get_column(table, entity, cond.field)
        return getattr(col, method)(cond.value)

    # ─── JOIN ───────────────────────────────────────────────────

    def _apply_joins(self, stmt, intent: Intent, table: Table, joined: dict) -> tuple[Any, dict]:
        """Apply JOINs using the pre-built joined tables map."""
        if not intent.join:
            return stmt, joined

        join_target = table
        for j in intent.join:
            key = j.alias or j.entity
            target_table = joined[key][0] if key in joined else self._get_table(j.entity)

            join_conditions = []
            for local_field, foreign_field in j.on.items():
                local_col = self._get_column(table, intent.entity, local_field)
                foreign_col_name = self.registry.resolve_column(j.entity, foreign_field)
                foreign_col = target_table.c[foreign_col_name]
                join_conditions.append(local_col == foreign_col)

            is_outer = j.type.value in ("left", "full")
            is_full = j.type.value == "full"
            join_target = join_target.join(target_table, and_(*join_conditions), isouter=is_outer, full=is_full)

        stmt = stmt.select_from(join_target)
        return stmt, joined

    # ─── Computed Fields (CASE, Window, Function) ───────────────

    def _build_computed_columns(self, intent: Intent, table: Table, joined: dict) -> list:
        """Build SQLAlchemy expressions for computed_fields."""
        if not intent.computed_fields:
            return []

        result = []
        for cf in intent.computed_fields:
            if cf.case:
                result.append(self._build_case(cf, table, intent.entity))
            elif cf.window:
                result.append(self._build_window(cf, table, intent.entity))
            elif cf.function:
                result.append(self._build_function(cf, table, intent.entity))
        return result

    def _build_case(self, cf: ComputedField, table: Table, entity: str):
        whens = []
        for w in cf.case.when:
            cond = self._build_single_condition(w.condition, table, entity)
            whens.append((cond, literal(w.then) if not isinstance(w.then, (int, float)) else w.then))

        expr = case(*whens, else_=literal(cf.case.else_value) if cf.case.else_value is not None else None)
        return expr.label(cf.name)

    def _build_window(self, cf: ComputedField, table: Table, entity: str):
        w = cf.window
        fn = WINDOW_FUNCS.get(w.function)
        if fn is None:
            raise QueryPlanError(f"Unknown window function: '{w.function}'")

        # Build function call with args
        needs_field = w.function in ("lag", "lead", "first_value", "last_value", "sum", "avg", "min", "max", "count", "ntile")
        if needs_field and w.args:
            # e.g. LAG(price, 1) → func.lag(col, 1)
            resolved_args = []
            for a in w.args:
                if isinstance(a, str):
                    resolved_args.append(self._get_column(table, entity, a))
                else:
                    resolved_args.append(a)
            fn_call = fn(*resolved_args)
        elif w.function == "ntile" and w.args:
            fn_call = fn(w.args[0])
        else:
            fn_call = fn()

        # Build OVER clause
        over_kwargs = {}
        if w.partition_by:
            over_kwargs["partition_by"] = [self._get_column(table, entity, f) for f in w.partition_by]
        if w.order_by:
            order_cols = []
            for s in w.order_by:
                col = self._get_column(table, entity, s.field)
                order_cols.append(col.desc() if s.order.value == "desc" else col.asc())
            over_kwargs["order_by"] = order_cols

        return fn_call.over(**over_kwargs).label(cf.name)

    def _build_function(self, cf: ComputedField, table: Table, entity: str):
        f = cf.function

        # CAST special handling
        if f.name == "cast":
            if not f.args or not f.cast_to:
                raise QueryPlanError("CAST requires 'args' and 'cast_to'")
            col = self._resolve_func_arg(f.args[0], table, entity)
            target_type = CAST_TYPES.get(f.cast_to.lower())
            if target_type is None:
                raise QueryPlanError(f"Unknown cast type: '{f.cast_to}'")
            return sa_cast(col, target_type()).label(cf.name)

        # EXTRACT special handling
        if f.name == "extract":
            if len(f.args) < 2:
                raise QueryPlanError("EXTRACT requires [part, field]")
            part = f.args[0]["literal"] if isinstance(f.args[0], dict) else f.args[0]
            col = self._resolve_func_arg(f.args[1], table, entity)
            return func.extract(part, col).label(cf.name)

        # NOW / CURRENT_DATE (no args)
        if f.name in ("now", "current_date"):
            return SCALAR_FUNCS[f.name]().label(cf.name)

        fn = SCALAR_FUNCS.get(f.name)
        if fn is None:
            raise QueryPlanError(f"Unknown function: '{f.name}'")

        resolved_args = [self._resolve_func_arg(a, table, entity) for a in f.args]
        return fn(*resolved_args).label(cf.name)

    # ─── CTE ────────────────────────────────────────────────────

    def _build_ctes(self, intent: Intent) -> dict[str, Any]:
        """Build CTE objects and return name → cte mapping."""
        ctes = {}
        if not intent.cte:
            return ctes

        for cte_def in intent.cte:
            sub_intent = cte_def.query
            sub_table = self._get_table(sub_intent.entity)
            sub_stmt = self.plan(sub_intent)
            ctes[cte_def.name] = sub_stmt.cte(cte_def.name)

        return ctes

    def _apply_cte_filters(self, stmt, table: Table, entity: str, filters: dict, ctes: dict):
        """Handle $cte references in filters after CTEs are built."""
        for key, value in filters.items():
            if isinstance(value, dict) and "$cte" in value:
                cte_name = value["$cte"]
                cte_field = value.get("field")
                if cte_name in ctes:
                    cte_obj = ctes[cte_name]
                    field = self._strip_suffix(key)
                    col = self._get_column(table, entity, field)
                    if cte_field:
                        cte_col = cte_obj.c[cte_field]
                    else:
                        cte_col = list(cte_obj.c)[0]
                    if "__in" in key or "__not_in" in key:
                        if "__not_in" in key:
                            stmt = stmt.where(col.not_in(select(cte_col)))
                        else:
                            stmt = stmt.where(col.in_(select(cte_col)))
                    else:
                        stmt = stmt.where(col.in_(select(cte_col)))
        return stmt

    # ─── Main Plan Method ───────────────────────────────────────

    def plan(self, intent: Intent) -> Any:
        if intent.intent in (IntentType.UNION, IntentType.INTERSECT, IntentType.EXCEPT):
            return self._plan_set_operation(intent)

        table = self._get_table(intent.entity)

        planners = {
            IntentType.LIST: self._plan_list,
            IntentType.GET: self._plan_get,
            IntentType.COUNT: self._plan_count,
            IntentType.AGGREGATE: self._plan_aggregate,
            IntentType.CREATE: self._plan_create,
            IntentType.BATCH_CREATE: self._plan_batch_create,
            IntentType.UPSERT: self._plan_upsert,
            IntentType.UPDATE: self._plan_update,
            IntentType.DELETE: self._plan_delete,
        }

        planner = planners.get(intent.intent)
        if planner is None:
            raise QueryPlanError(f"Unknown intent type: {intent.intent}")
        return planner(intent, table)

    # ─── LIST ───────────────────────────────────────────────────

    def _plan_list(self, intent: Intent, table: Table):
        joined = self._build_joined_tables_map(intent)
        columns = self._resolve_columns(intent, table, joined)

        # Add computed fields
        computed = self._build_computed_columns(intent, table, joined)
        all_cols = columns + computed

        stmt = select(*all_cols)

        if intent.distinct:
            stmt = stmt.distinct()

        # JOINs
        stmt, joined = self._apply_joins(stmt, intent, table, joined)

        # CTEs
        ctes = self._build_ctes(intent)

        # Simple filters (skip $cte entries)
        plain_filters = {k: v for k, v in intent.filters.items() if not (isinstance(v, dict) and "$cte" in v)}
        stmt = self._apply_filters(stmt, table, intent.entity, plain_filters)

        # CTE filters
        if ctes:
            stmt = self._apply_cte_filters(stmt, table, intent.entity, intent.filters, ctes)

        # Complex conditions
        if intent.where:
            stmt = stmt.where(self._build_where_clause(intent.where, table, intent.entity))

        # Sort
        if intent.sort:
            for s in intent.sort:
                col = self._resolve_field(intent, s.field, table, joined)
                stmt = stmt.order_by(col.desc() if s.order.value == "desc" else col.asc())

        if intent.limit:
            stmt = stmt.limit(intent.limit)
        if intent.offset:
            stmt = stmt.offset(intent.offset)

        return stmt

    # ─── GET ────────────────────────────────────────────────────

    def _plan_get(self, intent: Intent, table: Table):
        joined = self._build_joined_tables_map(intent)
        columns = self._resolve_columns(intent, table, joined)
        computed = self._build_computed_columns(intent, table, joined)
        stmt = select(*(columns + computed))

        stmt, joined = self._apply_joins(stmt, intent, table, joined)

        pk_col_name = self.registry.get(intent.entity).primary_key
        pk_col = self._get_column(table, intent.entity, pk_col_name)
        return stmt.where(pk_col == intent.id).limit(1)

    # ─── COUNT ──────────────────────────────────────────────────

    def _plan_count(self, intent: Intent, table: Table):
        joined = self._build_joined_tables_map(intent)

        if intent.distinct and intent.fields:
            col = self._get_column(table, intent.entity, intent.fields[0])
            stmt = select(func.count(sa_distinct(col)).label("count"))
        else:
            stmt = select(func.count().label("count")).select_from(table)

        if intent.join:
            stmt, _ = self._apply_joins(stmt, intent, table, joined)

        plain_filters = {k: v for k, v in intent.filters.items() if not (isinstance(v, dict) and "$cte" in v)}
        stmt = self._apply_filters(stmt, table, intent.entity, plain_filters)

        if intent.where:
            stmt = stmt.where(self._build_where_clause(intent.where, table, intent.entity))

        return stmt

    # ─── AGGREGATE ──────────────────────────────────────────────

    def _plan_aggregate(self, intent: Intent, table: Table):
        if not intent.aggregation and not intent.aggregations:
            raise MissingRequiredFieldError(intent_type="aggregate", field="aggregation")

        # Build aggregation columns
        agg_cols = []
        if intent.aggregations:
            for i, agg in enumerate(intent.aggregations):
                col = self._get_column(table, intent.entity, agg.field)
                label = f"{agg.op.value}_{agg.field}"
                agg_cols.append(AGG_FUNCS[agg.op](col).label(label))
        elif intent.aggregation:
            col = self._get_column(table, intent.entity, intent.aggregation.field)
            agg_cols.append(AGG_FUNCS[intent.aggregation.op](col).label("result"))

        joined = self._build_joined_tables_map(intent)

        if intent.group_by:
            group_cols = []
            for f in intent.group_by:
                if "." in f and joined:
                    group_cols.append(self._resolve_field(intent, f, table, joined))
                else:
                    group_cols.append(self._get_column(table, intent.entity, f))

            stmt = select(*group_cols, *agg_cols)
            if intent.join:
                stmt, joined = self._apply_joins(stmt, intent, table, joined)

            # Advanced GROUP BY modes
            if intent.group_by_mode == "rollup":
                stmt = stmt.group_by(func.rollup(*group_cols))
            elif intent.group_by_mode == "cube":
                stmt = stmt.group_by(func.cube(*group_cols))
            elif intent.group_by_mode == "grouping_sets" and intent.grouping_sets:
                sets = []
                for gs in intent.grouping_sets:
                    gs_cols = tuple(self._get_column(table, intent.entity, f) for f in gs)
                    sets.append(gs_cols)
                stmt = stmt.group_by(func.grouping_sets(*sets))
            else:
                stmt = stmt.group_by(*group_cols)
        else:
            stmt = select(*agg_cols)
            if intent.join:
                stmt, joined = self._apply_joins(stmt, intent, table, joined)

        plain_filters = {k: v for k, v in intent.filters.items() if not (isinstance(v, dict) and "$cte" in v)}
        stmt = self._apply_filters(stmt, table, intent.entity, plain_filters)

        if intent.where:
            stmt = stmt.where(self._build_where_clause(intent.where, table, intent.entity))

        if intent.having:
            for h in intent.having:
                h_col = self._get_column(table, intent.entity, h.field)
                h_agg = AGG_FUNCS[h.op](h_col)
                comp = COMPARISON_OPS.get(h.condition, "__gt__")
                stmt = stmt.having(getattr(h_agg, comp)(h.value))

        return stmt

    # ─── CREATE ─────────────────────────────────────────────────

    def _plan_create(self, intent: Intent, table: Table):
        if not intent.data:
            raise MissingRequiredFieldError(intent_type="create", field="data")
        mapped = {self.registry.resolve_column(intent.entity, k): v for k, v in intent.data.items()}
        stmt = insert(table).values(**mapped)
        if intent.returning:
            ret_cols = [self._get_column(table, intent.entity, f) for f in intent.returning]
            stmt = stmt.returning(*ret_cols)
        return stmt

    # ─── BATCH CREATE ───────────────────────────────────────────

    def _plan_batch_create(self, intent: Intent, table: Table):
        if not intent.rows:
            raise MissingRequiredFieldError(intent_type="batch_create", field="rows")
        mapped_rows = [
            {self.registry.resolve_column(intent.entity, k): v for k, v in row.items()}
            for row in intent.rows
        ]
        stmt = insert(table).values(mapped_rows)
        if intent.returning:
            ret_cols = [self._get_column(table, intent.entity, f) for f in intent.returning]
            stmt = stmt.returning(*ret_cols)
        return stmt

    # ─── UPSERT ─────────────────────────────────────────────────

    def _plan_upsert(self, intent: Intent, table: Table):
        if not intent.data:
            raise MissingRequiredFieldError(intent_type="upsert", field="data")
        mapped = {self.registry.resolve_column(intent.entity, k): v for k, v in intent.data.items()}

        # Use dialect-specific insert for ON CONFLICT support
        dialect = self.dialect
        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        elif dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        else:
            # Fallback: try sqlite dialect (works for most)
            from sqlalchemy.dialects.sqlite import insert as dialect_insert

        stmt = dialect_insert(table).values(**mapped)

        # ON CONFLICT
        if intent.conflict_target:
            index_elements = [self.registry.resolve_column(intent.entity, f) for f in intent.conflict_target]
            if intent.conflict_update:
                update_cols = {
                    self.registry.resolve_column(intent.entity, f): stmt.excluded[self.registry.resolve_column(intent.entity, f)]
                    for f in intent.conflict_update
                }
            else:
                update_cols = {
                    k: stmt.excluded[k]
                    for k in mapped.keys()
                    if k not in index_elements
                }
            stmt = stmt.on_conflict_do_update(index_elements=index_elements, set_=update_cols)
        else:
            pk = self.registry.get(intent.entity).primary_key
            pk_col = self.registry.resolve_column(intent.entity, pk)
            update_cols = {k: stmt.excluded[k] for k in mapped.keys() if k != pk_col}
            stmt = stmt.on_conflict_do_update(index_elements=[pk_col], set_=update_cols)

        if intent.returning:
            ret_cols = [self._get_column(table, intent.entity, f) for f in intent.returning]
            stmt = stmt.returning(*ret_cols)
        return stmt

    # ─── UPDATE ─────────────────────────────────────────────────

    def _plan_update(self, intent: Intent, table: Table):
        if not intent.data:
            raise MissingRequiredFieldError(intent_type="update", field="data")
        mapped = {self.registry.resolve_column(intent.entity, k): v for k, v in intent.data.items()}
        stmt = update(table).values(**mapped)
        stmt = self._apply_filters(stmt, table, intent.entity, intent.filters)

        if intent.where:
            stmt = stmt.where(self._build_where_clause(intent.where, table, intent.entity))
        if intent.id is not None:
            pk = self._get_column(table, intent.entity, self.registry.get(intent.entity).primary_key)
            stmt = stmt.where(pk == intent.id)

        # UPDATE ... FROM (join-based update)
        if intent.from_entity and intent.from_join_on:
            from_table = self._get_table(intent.from_entity)
            join_conds = []
            for local_f, foreign_f in intent.from_join_on.items():
                local_col = self._get_column(table, intent.entity, local_f)
                foreign_col = self._get_column(from_table, intent.from_entity, foreign_f)
                join_conds.append(local_col == foreign_col)
            stmt = stmt.where(and_(*join_conds))
            if intent.from_filters:
                for k, v in intent.from_filters.items():
                    col = self._get_column(from_table, intent.from_entity, k)
                    stmt = stmt.where(col == v)

        if intent.returning:
            ret_cols = [self._get_column(table, intent.entity, f) for f in intent.returning]
            stmt = stmt.returning(*ret_cols)
        return stmt

    # ─── DELETE ─────────────────────────────────────────────────

    def _plan_delete(self, intent: Intent, table: Table):
        stmt = delete(table)
        stmt = self._apply_filters(stmt, table, intent.entity, intent.filters)

        if intent.where:
            stmt = stmt.where(self._build_where_clause(intent.where, table, intent.entity))
        if intent.id is not None:
            pk = self._get_column(table, intent.entity, self.registry.get(intent.entity).primary_key)
            stmt = stmt.where(pk == intent.id)

        # DELETE ... USING (join-based delete)
        if intent.from_entity and intent.from_join_on:
            from_table = self._get_table(intent.from_entity)
            join_conds = []
            for local_f, foreign_f in intent.from_join_on.items():
                local_col = self._get_column(table, intent.entity, local_f)
                foreign_col = self._get_column(from_table, intent.from_entity, foreign_f)
                join_conds.append(local_col == foreign_col)
            stmt = stmt.where(and_(*join_conds))
            if intent.from_filters:
                for k, v in intent.from_filters.items():
                    col = self._get_column(from_table, intent.from_entity, k)
                    stmt = stmt.where(col == v)

        if intent.returning:
            ret_cols = [self._get_column(table, intent.entity, f) for f in intent.returning]
            stmt = stmt.returning(*ret_cols)
        return stmt

    # ─── SET OPERATIONS (UNION / INTERSECT / EXCEPT) ────────────

    def _plan_set_operation(self, intent: Intent):
        if not intent.union_queries or len(intent.union_queries) < 2:
            raise UnionRequiresSubqueriesError()

        sub_stmts = []
        for sub in intent.union_queries:
            sub_table = self._get_table(sub.entity)
            sub_stmts.append(self._plan_list(sub, sub_table))

        if intent.intent == IntentType.INTERSECT:
            from sqlalchemy import intersect, intersect_all
            return intersect_all(*sub_stmts) if intent.union_all else intersect(*sub_stmts)
        elif intent.intent == IntentType.EXCEPT:
            from sqlalchemy import except_, except_all
            return except_all(*sub_stmts) if intent.union_all else except_(*sub_stmts)
        else:
            return union_all(*sub_stmts) if intent.union_all else union(*sub_stmts)

    # ─── Column Resolution ──────────────────────────────────────

    def _resolve_columns(self, intent: Intent, table: Table, joined: dict) -> list:
        if intent.fields:
            return [self._resolve_field(intent, f, table, joined) for f in intent.fields]
        all_fields = self.registry.list_fields(intent.entity)
        return [self._get_column(table, intent.entity, f) for f in all_fields]
