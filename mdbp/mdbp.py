"""
MDBP — Model Database Protocol

Main entry point. Wires together the full pipeline:
  Intent → Schema Validation → Policy Check → Query Plan → Execute → Format

Usage as a library:
    from mdbp import MDBP

    mdcp = MDBP(db_url="sqlite:///my.db")
    # That's it. All tables, columns, and types are auto-discovered.

    result = mdbp.query({
        "intent": "list",
        "entity": "product",
        "filters": {"price__gte": 10},
        "limit": 5,
    })
"""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from mdbp.connectors.sql import SQLConnector
from mdbp.core.errors import (
    DatabaseExecutionError,
    IntentTypeNotAllowedError,
    IntentValidationError,
    MDBPError,
)
from mdbp.core.intent import Intent, IntentType
from mdbp.core.policy import Policy, PolicyEngine
from mdbp.core.query_planner import QueryPlanner
from mdbp.core.response import MDBPResponse, ResponseFormatter
from mdbp.core.schema_registry import EntitySchema, SchemaRegistry


class MDBP:
    """
    Main MDBP class.

    Provides the full intent-based data access pipeline:
    parse → validate → enforce policy → plan query → execute → format response
    """

    def __init__(
        self,
        db_url: str,
        auto_discover: bool = True,
        allowed_intents: list[str] | None = None,
    ) -> None:
        """
        Args:
            db_url: SQLAlchemy database URL.
            auto_discover: Auto-discover tables/columns from DB. Default True.
            allowed_intents: Whitelist of allowed intent types.
                             e.g. ["list", "get", "count"] → read-only mode.
                             None = all intents allowed.
        """
        self.connector = SQLConnector(db_url)
        self.registry = SchemaRegistry()
        self.policy_engine = PolicyEngine()
        self.planner = QueryPlanner(self.registry, self.connector.metadata, dialect=self.connector.engine.dialect.name)
        self.formatter = ResponseFormatter()
        self.allowed_intents: set[IntentType] | None = (
            {IntentType(i) for i in allowed_intents} if allowed_intents else None
        )

        if auto_discover:
            self.registry.auto_discover(self.connector.metadata)

    def register_entity(self, schema: EntitySchema) -> None:
        """Register an entity schema mapping."""
        self.registry.register(schema)

    def dispose(self) -> None:
        """Dispose database connections and release resources."""
        self.connector.dispose()

    def add_policy(self, policy: Policy) -> None:
        """Add an access policy."""
        self.policy_engine.add_policy(policy)

    def query(self, raw_intent: dict[str, Any] | Intent) -> dict:
        """
        Execute the full MDBP pipeline.

        Accepts either a dict (from LLM JSON output) or an Intent object.
        Returns a dict suitable for sending back to the LLM.
        """
        intent_type = "unknown"
        entity = "unknown"

        try:
            # 1. Parse intent
            if isinstance(raw_intent, dict):
                intent_type = raw_intent.get("intent", "unknown")
                entity = raw_intent.get("entity", "unknown")
                intent = Intent(**raw_intent)
            else:
                intent = raw_intent

            intent_type = intent.intent.value
            entity = intent.entity

            # 2. Check allowed intents
            if self.allowed_intents and intent.intent not in self.allowed_intents:
                raise IntentTypeNotAllowedError(
                    intent_type=intent.intent.value,
                    allowed=[i.value for i in self.allowed_intents],
                )

            # 3. Validate entity exists in schema
            self.registry.get(intent.entity)

            # 4. Validate requested fields exist
            # Build alias → entity map for JOINs
            alias_map: dict[str, str] = {}
            if intent.join:
                for j in intent.join:
                    if j.alias:
                        alias_map[j.alias] = j.entity

            if intent.fields:
                for field in intent.fields:
                    if "." in field:
                        entity_ref, field_name = field.split(".", 1)
                        # Resolve alias to actual entity
                        actual_entity = alias_map.get(entity_ref, entity_ref)
                        self.registry.resolve_column(actual_entity, field_name)
                    else:
                        self.registry.resolve_column(intent.entity, field)

            # 5. Enforce policies
            intent = self.policy_engine.enforce(intent)

            # 6. Plan query
            statement = self.planner.plan(intent)

            # 7. Execute
            try:
                result = self.connector.execute(statement)
            except Exception as e:
                raise DatabaseExecutionError(
                    message="Query execution failed.",
                    original_error=str(e),
                ) from e

            # 8. Format response
            response = self.formatter.format(intent, result)
            return response.to_dict()

        except ValidationError as e:
            error = IntentValidationError(
                message=f"Invalid intent structure: {e.error_count()} validation error(s).",
                details={"errors": e.errors()},
            )
            return MDBPResponse(
                success=False, intent_type=intent_type, entity=entity, error=error,
            ).to_dict()

        except MDBPError as e:
            return MDBPResponse(
                success=False, intent_type=intent_type, entity=entity, error=e,
            ).to_dict()

        except Exception as e:
            error = MDBPError(
                message=f"Unexpected error: {e}",
                details={"type": type(e).__name__},
            )
            return MDBPResponse(
                success=False, intent_type=intent_type, entity=entity, error=error,
            ).to_dict()

    def describe_schema(self) -> dict:
        """Return the full schema description (useful for LLM system prompts)."""
        return self.registry.describe()
