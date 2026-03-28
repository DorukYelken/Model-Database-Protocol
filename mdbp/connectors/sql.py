"""
MDBP SQL Connector

Executes planned queries against a database via SQLAlchemy.
Supports any database that SQLAlchemy supports:
PostgreSQL, MySQL, SQLite, MSSQL, Oracle, etc.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import CursorResult, MetaData, create_engine, text
from sqlalchemy.engine import Engine


class SQLConnector:
    """Manages database connection and query execution."""

    def __init__(self, db_url: str) -> None:
        self.engine: Engine = create_engine(db_url)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    def execute(self, statement: Any) -> QueryResult:
        """Execute a SQLAlchemy statement and return results."""
        with self.engine.connect() as conn:
            result: CursorResult = conn.execute(statement)

            if result.returns_rows:
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                )
            else:
                conn.commit()
                return QueryResult(
                    columns=[],
                    rows=[],
                    row_count=result.rowcount,
                    is_mutation=True,
                )

    def dispose(self) -> None:
        """Dispose the engine and release all connections."""
        self.engine.dispose()


class QueryResult:
    """Raw result from a database query."""

    def __init__(
        self,
        columns: list[str],
        rows: list[dict[str, Any]],
        row_count: int,
        is_mutation: bool = False,
    ) -> None:
        self.columns = columns
        self.rows = rows
        self.row_count = row_count
        self.is_mutation = is_mutation
