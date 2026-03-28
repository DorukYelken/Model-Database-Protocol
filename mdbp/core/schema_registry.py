"""
MDBP Schema Registry

Maps logical entity names to physical database tables and columns.
This is the layer that prevents hallucinated column/table names from
reaching the database.

Usage:
    registry = SchemaRegistry()
    registry.register(EntitySchema(
        entity="order",
        table="orders",
        primary_key="id",
        fields={
            "id":            FieldSchema(column="id", dtype="integer"),
            "customer_name": FieldSchema(column="customer_name", dtype="text"),
            "total":         FieldSchema(column="total_amount", dtype="numeric"),
            "status":        FieldSchema(column="order_status", dtype="text"),
        },
    ))
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from mdbp.core.errors import EntityNotFoundError, FieldNotFoundError

if TYPE_CHECKING:
    from sqlalchemy import MetaData


class FieldSchema(BaseModel):
    """Maps a logical field name to a physical column."""

    column: str = Field(description="Physical column name in the database")
    dtype: str = Field(default="text", description="Data type hint: text, integer, numeric, boolean, datetime")
    description: str | None = Field(default=None, description="Human-readable description for the LLM")
    filterable: bool = Field(default=True, description="Whether this field can be used in filters")
    sortable: bool = Field(default=True, description="Whether this field can be used in sort")


class RelationSchema(BaseModel):
    """Defines a join relationship between entities."""

    target_entity: str
    join_column: str = Field(description="Column on this entity's table")
    target_column: str = Field(description="Column on the target entity's table")
    relation_type: str = Field(default="many_to_one", description="one_to_one, many_to_one, one_to_many")


class EntitySchema(BaseModel):
    """Full schema definition for a logical entity."""

    entity: str = Field(description="Logical entity name (what the LLM uses)")
    table: str = Field(description="Physical table name in the database")
    primary_key: str = Field(default="id")
    fields: dict[str, FieldSchema]
    relations: dict[str, RelationSchema] = Field(default_factory=dict)
    description: str | None = Field(default=None, description="Entity description for the LLM")


class SchemaRegistry:
    """
    Central registry of all entity schemas.

    Validates that entities and fields referenced in intents actually exist,
    and translates logical names to physical column/table names.
    """

    def __init__(self) -> None:
        self._entities: dict[str, EntitySchema] = {}

    def register(self, schema: EntitySchema) -> None:
        self._entities[schema.entity] = schema

    def get(self, entity: str) -> EntitySchema:
        if entity not in self._entities:
            raise EntityNotFoundError(entity, list(self._entities.keys()))
        return self._entities[entity]

    def has(self, entity: str) -> bool:
        return entity in self._entities

    def resolve_column(self, entity: str, field: str) -> str:
        """Translate a logical field name to a physical column name."""
        schema = self.get(entity)
        if field not in schema.fields:
            raise FieldNotFoundError(entity, field, list(schema.fields.keys()))
        return schema.fields[field].column

    def resolve_table(self, entity: str) -> str:
        return self.get(entity).table

    def get_field_schema(self, entity: str, field: str) -> FieldSchema:
        schema = self.get(entity)
        if field not in schema.fields:
            raise FieldNotFoundError(entity, field, list(schema.fields.keys()))
        return schema.fields[field]

    def auto_discover(self, metadata: MetaData) -> None:
        """
        Automatically discover all tables from the database metadata
        and register them as entities. No manual schema definition needed.

        SQLAlchemy type → MDBP dtype mapping is automatic.
        Table name → entity name (e.g. 'products' → 'product', 'users' → 'user').
        """
        from sqlalchemy import types as sa_types

        type_map = {
            sa_types.Integer: "integer",
            sa_types.SmallInteger: "integer",
            sa_types.BigInteger: "integer",
            sa_types.Float: "numeric",
            sa_types.Numeric: "numeric",
            sa_types.String: "text",
            sa_types.Text: "text",
            sa_types.Boolean: "boolean",
            sa_types.DateTime: "datetime",
            sa_types.Date: "datetime",
            sa_types.Time: "datetime",
            sa_types.LargeBinary: "binary",
        }

        def resolve_dtype(sa_type) -> str:
            for sa_cls, dtype in type_map.items():
                if isinstance(sa_type, sa_cls):
                    return dtype
            return "text"

        def table_to_entity(table_name: str) -> str:
            """Simple pluralization reversal: 'products' → 'product', 'users' → 'user'."""
            if table_name.endswith("ies"):
                return table_name[:-3] + "y"
            if table_name.endswith("ses") or table_name.endswith("xes"):
                return table_name[:-2]
            if table_name.endswith("s") and not table_name.endswith("ss"):
                return table_name[:-1]
            return table_name

        for table_name, table in metadata.tables.items():
            entity_name = table_to_entity(table_name)

            # Find primary key
            pk_cols = [col.name for col in table.primary_key.columns]
            primary_key = pk_cols[0] if pk_cols else "id"

            # Build fields from columns
            fields: dict[str, FieldSchema] = {}
            for col in table.columns:
                fields[col.name] = FieldSchema(
                    column=col.name,
                    dtype=resolve_dtype(col.type),
                )

            schema = EntitySchema(
                entity=entity_name,
                table=table_name,
                primary_key=primary_key,
                fields=fields,
            )
            self.register(schema)

    def list_entities(self) -> list[str]:
        return list(self._entities.keys())

    def list_fields(self, entity: str) -> list[str]:
        return list(self.get(entity).fields.keys())

    def describe(self) -> dict:
        """Return a LLM-friendly description of the entire schema."""
        result = {}
        for name, schema in self._entities.items():
            result[name] = {
                "description": schema.description,
                "fields": {
                    fname: {
                        "type": f.dtype,
                        "description": f.description,
                        "filterable": f.filterable,
                        "sortable": f.sortable,
                    }
                    for fname, f in schema.fields.items()
                },
            }
        return result


