"""
MDBP Data Masking

Applies deterministic masking to query result fields based on policy rules.
Masking happens after query execution, before response formatting.

Usage:
    from mdbp.core.masking import MaskingRule, apply_masking

    # String shorthand
    masked_fields = {"email": "email", "phone": "last_n"}

    # MaskingRule with options
    masked_fields = {"phone": MaskingRule(strategy="last_n", options={"n": 4})}

    # Custom callable
    masked_fields = {"ssn": lambda v: "***-**-" + str(v)[-4:]}

    masked_rows = apply_masking(rows, masked_fields)
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from pydantic import BaseModel, Field


class MaskingRule(BaseModel):
    """Defines how a field should be masked."""

    strategy: str = Field(description="Masking strategy: partial, redact, email, last_n, first_n, hash")
    options: dict[str, Any] = Field(default_factory=dict, description="Strategy-specific options, e.g. {'n': 4}")


def _mask_partial(value: str, **options: Any) -> str:
    """Show first and last character, mask the rest. 'doruk' → 'd***k'"""
    if len(value) <= 2:
        return "*" * len(value)
    return value[0] + "*" * (len(value) - 2) + value[-1]


def _mask_redact(value: str, **options: Any) -> str:
    """Replace entirely. 'doruk' → '***'"""
    return "***"


def _mask_email(value: str, **options: Any) -> str:
    """Mask local part, keep domain. 'doruk@example.com' → 'd***@example.com'"""
    if "@" not in value:
        return _mask_partial(value)
    local, domain = value.rsplit("@", 1)
    if len(local) <= 1:
        masked_local = "*"
    else:
        masked_local = local[0] + "***"
    return f"{masked_local}@{domain}"


def _mask_last_n(value: str, **options: Any) -> str:
    """Show only last N characters. '5551234567' → '******4567'"""
    n = options.get("n", 4)
    if len(value) <= n:
        return value
    return "*" * (len(value) - n) + value[-n:]


def _mask_first_n(value: str, **options: Any) -> str:
    """Show only first N characters. '5551234567' → '5551******'"""
    n = options.get("n", 4)
    if len(value) <= n:
        return value
    return value[:n] + "*" * (len(value) - n)


def _mask_hash(value: str, **options: Any) -> str:
    """SHA-256 hash, first 8 chars. 'doruk' → 'a1b2c3d4'"""
    length = options.get("length", 8)
    return hashlib.sha256(value.encode()).hexdigest()[:length]


_STRATEGIES: dict[str, Callable[..., str]] = {
    "partial": _mask_partial,
    "redact": _mask_redact,
    "email": _mask_email,
    "last_n": _mask_last_n,
    "first_n": _mask_first_n,
    "hash": _mask_hash,
}


def mask_value(value: Any, rule: str | MaskingRule | Callable, options: dict[str, Any] | None = None) -> Any:
    """Mask a single value using the given rule."""
    if value is None:
        return None

    str_value = str(value)
    if not str_value:
        return value

    # Callable: user-provided function
    if callable(rule) and not isinstance(rule, MaskingRule):
        return rule(value)

    # Resolve strategy name and options
    if isinstance(rule, MaskingRule):
        strategy_name = rule.strategy
        opts = rule.options
    else:
        strategy_name = rule
        opts = options or {}

    strategy_fn = _STRATEGIES.get(strategy_name)
    if strategy_fn is None:
        return value

    return strategy_fn(str_value, **opts)


def apply_masking(
    rows: list[dict[str, Any]],
    masked_fields: dict[str, str | MaskingRule | Callable],
) -> list[dict[str, Any]]:
    """Apply masking rules to a list of row dicts. Only specified fields are masked."""
    if not masked_fields or not rows:
        return rows

    masked_rows = []
    for row in rows:
        masked_row = dict(row)
        for field, rule in masked_fields.items():
            if field in masked_row:
                masked_row[field] = mask_value(masked_row[field], rule)
        masked_rows.append(masked_row)
    return masked_rows
