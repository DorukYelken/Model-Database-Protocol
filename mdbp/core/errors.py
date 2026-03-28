"""
MDBP Error System

Every MDBP error has:
  - code:    Machine-readable error code (e.g. "MDBP_ENTITY_NOT_FOUND")
  - message: Human-readable description
  - details: Structured context for debugging

Error code prefixes:
  MDBP_SCHEMA_*   → Schema registry errors (entity/field resolution)
  MDBP_POLICY_*   → Policy enforcement errors (access denied)
  MDBP_QUERY_*    → Query planning errors (invalid intent structure)
  MDBP_CONN_*     → Database connection/execution errors
  MDBP_INTENT_*   → Intent validation errors
"""

from __future__ import annotations

from typing import Any


class MDBPError(Exception):
    """Base exception for all MDBP errors."""

    code: str = "MDBP_UNKNOWN_ERROR"
    status: str = "error"

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.details:
            result["details"] = self.details
        return result


# ─── Schema Errors ──────────────────────────────────────────────

class EntityNotFoundError(MDBPError):
    """Raised when an intent references an entity that doesn't exist in the registry."""

    code = "MDBP_SCHEMA_ENTITY_NOT_FOUND"

    def __init__(self, entity: str, available: list[str]) -> None:
        super().__init__(
            message=f"Entity '{entity}' not found in schema registry.",
            details={"entity": entity, "available_entities": available},
        )


class FieldNotFoundError(MDBPError):
    """Raised when an intent references a field that doesn't exist on an entity."""

    code = "MDBP_SCHEMA_FIELD_NOT_FOUND"

    def __init__(self, entity: str, field: str, available: list[str]) -> None:
        super().__init__(
            message=f"Field '{field}' not found on entity '{entity}'.",
            details={"entity": entity, "field": field, "available_fields": available},
        )


class EntityReferenceError(MDBPError):
    """Raised when a dot-notation field references an unknown joined entity."""

    code = "MDBP_SCHEMA_ENTITY_REF_NOT_FOUND"

    def __init__(self, entity_ref: str, field: str) -> None:
        super().__init__(
            message=f"Unknown entity reference '{entity_ref}' in field '{field}'.",
            details={"entity_reference": entity_ref, "field": field},
        )


# ─── Policy Errors ──────────────────────────────────────────────

class PolicyViolation(MDBPError):
    """Base class for all policy violations."""

    code = "MDBP_POLICY_VIOLATION"


class IntentNotAllowedError(PolicyViolation):
    """Raised when the intent type is not in the policy's allowed list."""

    code = "MDBP_POLICY_INTENT_NOT_ALLOWED"

    def __init__(self, intent_type: str, entity: str, role: str | None) -> None:
        super().__init__(
            message=f"Intent type '{intent_type}' not allowed for entity '{entity}'.",
            details={"intent_type": intent_type, "entity": entity, "role": role},
        )


class FieldAccessDeniedError(PolicyViolation):
    """Raised when accessing a denied field."""

    code = "MDBP_POLICY_FIELD_DENIED"

    def __init__(self, entity: str, denied_fields: list[str]) -> None:
        super().__init__(
            message=f"Access denied to fields {denied_fields} on entity '{entity}'.",
            details={"entity": entity, "denied_fields": denied_fields},
        )


class FieldNotAllowedError(PolicyViolation):
    """Raised when accessing a field not in the allowed list."""

    code = "MDBP_POLICY_FIELD_NOT_ALLOWED"

    def __init__(self, entity: str, disallowed_fields: list[str], allowed_fields: list[str]) -> None:
        super().__init__(
            message=f"Fields {disallowed_fields} not in allowed list for entity '{entity}'.",
            details={
                "entity": entity,
                "disallowed_fields": disallowed_fields,
                "allowed_fields": allowed_fields,
            },
        )


# ─── Intent Errors ──────────────────────────────────────────────

class IntentTypeNotAllowedError(MDBPError):
    """Raised when the intent type is globally blocked via allowed_intents."""

    code = "MDBP_INTENT_TYPE_NOT_ALLOWED"

    def __init__(self, intent_type: str, allowed: list[str]) -> None:
        super().__init__(
            message=f"Intent type '{intent_type}' is not allowed.",
            details={"intent_type": intent_type, "allowed_intents": allowed},
        )


class IntentValidationError(MDBPError):
    """Raised when the intent structure is invalid."""

    code = "MDBP_INTENT_VALIDATION_ERROR"


# ─── Query Errors ───────────────────────────────────────────────

class QueryPlanError(MDBPError):
    """Raised when the query planner cannot build a valid query."""

    code = "MDBP_QUERY_PLAN_ERROR"


class MissingRequiredFieldError(QueryPlanError):
    """Raised when a required field is missing for an intent type."""

    code = "MDBP_QUERY_MISSING_FIELD"

    def __init__(self, intent_type: str, field: str) -> None:
        super().__init__(
            message=f"'{intent_type}' intent requires '{field}' field.",
            details={"intent_type": intent_type, "required_field": field},
        )


class UnknownFilterOpError(QueryPlanError):
    """Raised when an unknown filter operator is used."""

    code = "MDBP_QUERY_UNKNOWN_FILTER_OP"

    def __init__(self, op: str) -> None:
        super().__init__(
            message=f"Unknown filter operator: '{op}'.",
            details={
                "op": op,
                "supported_ops": [
                    "eq", "ne", "gt", "gte", "lt", "lte",
                    "like", "ilike", "not_like",
                    "in", "not_in", "between",
                    "null", "not_null",
                ],
            },
        )


class UnionRequiresSubqueriesError(QueryPlanError):
    """Raised when a union intent doesn't have enough sub-queries."""

    code = "MDBP_QUERY_UNION_REQUIRES_SUBQUERIES"

    def __init__(self) -> None:
        super().__init__(
            message="Union intent requires at least 2 sub-queries in 'union_queries'.",
            details={},
        )


# ─── Connection Errors ──────────────────────────────────────────

class DatabaseConnectionError(MDBPError):
    """Raised when the database connection fails."""

    code = "MDBP_CONN_FAILED"


class DatabaseExecutionError(MDBPError):
    """Raised when a query execution fails."""

    code = "MDBP_CONN_EXECUTION_ERROR"

    def __init__(self, message: str, original_error: str | None = None) -> None:
        super().__init__(
            message=message,
            details={"original_error": original_error} if original_error else {},
        )


class NotFoundError(MDBPError):
    """Raised when a GET query returns no results."""

    code = "MDBP_NOT_FOUND"

    def __init__(self, entity: str, id_value: Any) -> None:
        super().__init__(
            message=f"{entity} with id '{id_value}' not found.",
            details={"entity": entity, "id": id_value},
        )


# ─── Config Errors ──────────────────────────────────────────────

class ConfigFileNotFoundError(MDBPError):
    """Raised when the MDBP config file is not found."""

    code = "MDBP_CONFIG_FILE_NOT_FOUND"

    def __init__(self, path: str) -> None:
        super().__init__(
            message=f"Config file not found: {path}.",
            details={"path": path},
        )
