"""MDBP — Model Database Protocol"""

from mdbp.core.errors import (
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
    MDBPError,
    MissingRequiredFieldError,
    NotFoundError,
    PolicyViolation,
    QueryPlanError,
    UnionRequiresSubqueriesError,
    UnknownFilterOpError,
)
from mdbp.mdbp import MDBP

__all__ = [
    "MDBP",
    "MDBPError",
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
__version__ = "0.3.1"
