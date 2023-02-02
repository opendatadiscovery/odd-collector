from typing import Optional

from pydantic import BaseModel


class ColumnType(BaseModel):
    field_type: Optional[str]
    logical_type: Optional[str]


class PrimitiveColumnType(ColumnType):
    ...


class ArrayColumnType(ColumnType):
    field_type = "array"
    specific_type: ColumnType


class MapColumnType(ColumnType):
    field_type = "map"
    key_type: ColumnType
    value_type: ColumnType


class StructColumnType(ColumnType):
    field_type = "struct"
    fields: dict[str, ColumnType] = dict()


class UnionColumnType(ColumnType):
    field_type = "union"
    types: list[ColumnType] = []


class UnknownColumnType(PrimitiveColumnType):
    field_type = "unknown"
