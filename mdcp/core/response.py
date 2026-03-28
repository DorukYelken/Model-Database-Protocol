"""
MDCP Response Formatter

Transforms raw database results into LLM-friendly structured responses.
The goal is to give the LLM exactly the context it needs — no more, no less.

Error responses include structured error objects:
  {
      "success": false,
      "error": {
          "code": "MDCP_SCHEMA_FIELD_NOT_FOUND",
          "message": "Field 'fake' not found on entity 'product'.",
          "details": {"entity": "product", "field": "fake", "available_fields": [...]}
      }
  }
"""

from __future__ import annotations

from typing import Any

from mdcp.connectors.sql import QueryResult
from mdcp.core.errors import MDCPError, NotFoundError
from mdcp.core.intent import Intent, IntentType


class MDCPResponse:
    """Structured response from MDCP."""

    def __init__(
        self,
        success: bool,
        intent_type: str,
        entity: str,
        data: Any = None,
        summary: str = "",
        error: MDCPError | None = None,
    ) -> None:
        self.success = success
        self.intent_type = intent_type
        self.entity = entity
        self.data = data
        self.summary = summary
        self.error = error

    def to_dict(self) -> dict:
        result: dict[str, Any] = {
            "success": self.success,
            "intent": self.intent_type,
            "entity": self.entity,
        }
        if self.error:
            result["error"] = self.error.to_dict()
        else:
            result["summary"] = self.summary
            result["data"] = self.data
        return result


class ResponseFormatter:
    """Formats QueryResult into MDCPResponse."""

    def format(self, intent: Intent, result: QueryResult) -> MDCPResponse:
        if intent.intent == IntentType.LIST:
            return self._format_list(intent, result)
        elif intent.intent == IntentType.GET:
            return self._format_get(intent, result)
        elif intent.intent == IntentType.COUNT:
            return self._format_count(intent, result)
        elif intent.intent == IntentType.AGGREGATE:
            return self._format_aggregate(intent, result)
        elif intent.intent in (IntentType.CREATE, IntentType.BATCH_CREATE, IntentType.UPSERT, IntentType.UPDATE, IntentType.DELETE):
            return self._format_mutation(intent, result)
        return MDCPResponse(
            success=True,
            intent_type=intent.intent.value,
            entity=intent.entity,
            data=result.rows,
        )

    def _format_list(self, intent: Intent, result: QueryResult) -> MDCPResponse:
        return MDCPResponse(
            success=True,
            intent_type="list",
            entity=intent.entity,
            data=result.rows,
            summary=f"{result.row_count} {intent.entity}(s) found",
        )

    def _format_get(self, intent: Intent, result: QueryResult) -> MDCPResponse:
        if result.rows:
            return MDCPResponse(
                success=True,
                intent_type="get",
                entity=intent.entity,
                data=result.rows[0],
                summary=f"{intent.entity} found",
            )
        return MDCPResponse(
            success=False,
            intent_type="get",
            entity=intent.entity,
            error=NotFoundError(entity=intent.entity, id_value=intent.id),
        )

    def _format_count(self, intent: Intent, result: QueryResult) -> MDCPResponse:
        count = result.rows[0].get("count", 0) if result.rows else 0
        return MDCPResponse(
            success=True,
            intent_type="count",
            entity=intent.entity,
            data={"count": count},
            summary=f"{count} {intent.entity}(s) match the criteria",
        )

    def _format_aggregate(self, intent: Intent, result: QueryResult) -> MDCPResponse:
        return MDCPResponse(
            success=True,
            intent_type="aggregate",
            entity=intent.entity,
            data=result.rows,
            summary=f"Aggregation result for {intent.entity}",
        )

    def _format_mutation(self, intent: Intent, result: QueryResult) -> MDCPResponse:
        action = intent.intent.value
        data: Any = {"affected_rows": result.row_count}
        if intent.returning and result.rows:
            data["returning"] = result.rows
        return MDCPResponse(
            success=True,
            intent_type=action,
            entity=intent.entity,
            data=data,
            summary=f"{action} on {intent.entity}: {result.row_count} row(s) affected",
        )
