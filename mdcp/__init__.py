"""MDCP — Model Database Context Protocol"""

from mdcp.core.errors import (
    DatabaseConnectionError,
    DatabaseExecutionError,
    EntityNotFoundError,
    EntityReferenceError,
    FieldAccessDeniedError,
    FieldNotAllowedError,
    FieldNotFoundError,
    IntentNotAllowedError,
    IntentTypeNotAllowedError,
    IntentValidationError,
    MDCPError,
    MissingRequiredFieldError,
    NotFoundError,
    PolicyViolation,
    QueryPlanError,
    UnionRequiresSubqueriesError,
    UnknownFilterOpError,
)
from mdcp.mdcp import MDCP

__all__ = [
    "MDCP",
    "MDCPError",
    "EntityNotFoundError",
    "FieldNotFoundError",
    "EntityReferenceError",
    "PolicyViolation",
    "IntentNotAllowedError",
    "FieldAccessDeniedError",
    "FieldNotAllowedError",
    "IntentTypeNotAllowedError",
    "IntentValidationError",
    "QueryPlanError",
    "MissingRequiredFieldError",
    "UnknownFilterOpError",
    "UnionRequiresSubqueriesError",
    "DatabaseConnectionError",
    "DatabaseExecutionError",
    "NotFoundError",
]
__version__ = "0.1.0"
