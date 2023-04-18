from dataclasses import dataclass
from typing import Optional

from odd_collector.adapters.clickhouse.mappers.types import TYPES_SQL_TO_ODD
from odd_models.models import Type

@dataclass
class ColumnType:
    field_type: str
    logical_type: Optional[str]


class PrimitiveColumnType(ColumnType):
    ...


class NestedColumnType(ColumnType):
    field_type = "Nested"
    fields: dict[str, ColumnType] = dict()


class ArrayColumnType(ColumnType):
    field_type = "Array"
    specific_type: ColumnType


class EnumColumnType(ColumnType):
    field_type = "Enum8"
    specific_type: ColumnType


def map_column_type(column_type: ColumnType):
    odd_type = TYPES_SQL_TO_ODD.get(column_type.field_type)

    if not odd_type:
        odd_type = Type.TYPE_UNKNOWN

    return odd_type

