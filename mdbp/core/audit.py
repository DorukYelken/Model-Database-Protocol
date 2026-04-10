"""
MDBP Audit Logging

Logs every query execution with structured metadata.
Pluggable backends: Python logging, JSON stream, or custom callback.

Usage:
    from mdbp.core.audit import PythonAuditLogger, StreamAuditLogger, CallbackAuditLogger

    # Python logging
    mdbp = MDBP(db_url="...", audit=PythonAuditLogger())

    # JSON lines to stdout
    mdbp = MDBP(db_url="...", audit=StreamAuditLogger())

    # Custom callback
    mdbp = MDBP(db_url="...", audit=CallbackAuditLogger(my_function))
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from typing import IO, Any, Callable


@dataclass
class AuditEntry:
    """Structured log record for a single query execution."""

    timestamp: str
    intent_type: str
    entity: str
    role: str | None
    success: bool
    error_code: str | None = None
    row_count: int | None = None
    duration_ms: float = 0.0
    dry_run: bool = False
    masked_fields: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None and v != [] and v != 0.0 or k in ("success", "dry_run")}


class AuditLogger:
    """Base audit logger — does nothing. Subclass to add behavior."""

    def log(self, entry: AuditEntry) -> None:
        pass


class CallbackAuditLogger(AuditLogger):
    """Calls a user-provided function for each audit entry."""

    def __init__(self, callback: Callable[[AuditEntry], None]) -> None:
        self._callback = callback

    def log(self, entry: AuditEntry) -> None:
        self._callback(entry)


class PythonAuditLogger(AuditLogger):
    """Logs audit entries via Python's logging module."""

    def __init__(self, logger_name: str = "mdbp.audit") -> None:
        self._logger = logging.getLogger(logger_name)

    def log(self, entry: AuditEntry) -> None:
        self._logger.info(json.dumps(entry.to_dict(), default=str))


class StreamAuditLogger(AuditLogger):
    """Writes JSON lines to a stream (stdout, file, etc.)."""

    def __init__(self, stream: IO[str] | None = None) -> None:
        self._stream = stream or sys.stdout

    def log(self, entry: AuditEntry) -> None:
        self._stream.write(json.dumps(entry.to_dict(), default=str) + "\n")
        self._stream.flush()
