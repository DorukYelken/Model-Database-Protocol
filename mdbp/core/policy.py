"""
MDBP Policy Engine

Controls what data can be accessed, by whom, and how much.
Prevents PII leaks, over-fetching, and unauthorized access.

Usage:
    engine = PolicyEngine()
    engine.add_policy(Policy(
        entity="user",
        role="analyst",
        allowed_fields=["id", "name", "email"],
        denied_fields=["ssn", "password_hash"],
        max_rows=100,
        allowed_intents=["list", "get", "count"],
    ))

    engine.enforce(intent, role="analyst")  # raises PolicyViolation if denied
"""

from __future__ import annotations

from typing import Any, Callable

from pydantic import BaseModel, Field

from mdbp.core.errors import (
    FieldAccessDeniedError,
    FieldNotAllowedError,
    IntentNotAllowedError,
    PolicyViolation,
)
from mdbp.core.intent import Intent, IntentType
from mdbp.core.masking import MaskingRule


class Policy(BaseModel):
    """Access policy for a specific entity + role combination."""

    entity: str
    role: str = Field(default="*", description="Role this policy applies to. '*' = all roles")

    allowed_fields: list[str] | None = Field(default=None, description="Allowed fields. None = all fields allowed")
    denied_fields: list[str] = Field(default_factory=list, description="Explicitly denied fields (overrides allowed)")

    max_rows: int = Field(default=1000, description="Maximum rows that can be returned")

    allowed_intents: list[IntentType] = Field(
        default_factory=lambda: [IntentType.LIST, IntentType.GET, IntentType.COUNT, IntentType.AGGREGATE],
        description="Which intent types this role can use",
    )

    row_filter: dict | None = Field(
        default=None,
        description="Automatic filter injected into every query (e.g. tenant isolation)",
    )

    masked_fields: dict[str, str | MaskingRule | Callable[..., Any]] = Field(
        default_factory=dict,
        description="Fields to mask in results. Key: field name, Value: strategy name, MaskingRule, or callable",
    )

    model_config = {"arbitrary_types_allowed": True}


class PolicyEngine:
    """Evaluates and enforces policies against intents."""

    def __init__(self) -> None:
        self._policies: list[Policy] = []

    def add_policy(self, policy: Policy) -> None:
        self._policies.append(policy)

    def find_policy(self, entity: str, role: str | None) -> Policy | None:
        """Find the most specific matching policy."""
        for p in self._policies:
            if p.entity == entity and p.role == role:
                return p
        for p in self._policies:
            if p.entity == entity and p.role == "*":
                return p
        return None

    def enforce(self, intent: Intent) -> Intent:
        """
        Validate intent against policies. Returns a (possibly modified)
        intent with policies applied, or raises a PolicyViolation subclass.
        """
        policy = self.find_policy(intent.entity, intent.role)

        if policy is None:
            return intent

        # Check intent type
        if intent.intent not in policy.allowed_intents:
            raise IntentNotAllowedError(
                intent_type=intent.intent.value,
                entity=intent.entity,
                role=intent.role,
            )

        # Check denied fields
        if intent.fields:
            denied = [f for f in intent.fields if f in policy.denied_fields]
            if denied:
                raise FieldAccessDeniedError(
                    entity=intent.entity,
                    denied_fields=denied,
                )

        # Check allowed fields
        if policy.allowed_fields is not None and intent.fields:
            disallowed = [f for f in intent.fields if f not in policy.allowed_fields]
            if disallowed:
                raise FieldNotAllowedError(
                    entity=intent.entity,
                    disallowed_fields=disallowed,
                    allowed_fields=policy.allowed_fields,
                )

        # Apply max_rows cap
        if intent.limit is None or intent.limit > policy.max_rows:
            intent = intent.model_copy(update={"limit": policy.max_rows})

        # Inject row filter
        if policy.row_filter:
            merged = {**policy.row_filter, **intent.filters}
            intent = intent.model_copy(update={"filters": merged})

        return intent
