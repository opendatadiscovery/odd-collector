from dataclasses import dataclass
from typing import Optional

from odd_models.models import Type


TYPES_CLICKHOUSE_TO_ODD = {
    "int": Type.TYPE_INTEGER,
    "double": Type.TYPE_NUMBER,
    "decimal": Type.TYPE_NUMBER,
    "float": Type.TYPE_NUMBER,
    "string": Type.TYPE_STRING,
    "json": Type.TYPE_STRUCT,
    "uuid": Type.TYPE_STRING,
    "enum": Type.TYPE_STRING,
    "boolean": Type.TYPE_BOOLEAN,
    "nested": Type.TYPE_STRUCT,
    "date": Type.TYPE_DATETIME,
    "binary": Type.TYPE_BINARY,
    "array": Type.TYPE_LIST,
    "map": Type.TYPE_MAP,
    "tuples": Type.TYPE_STRUCT,
    "union": Type.TYPE_UNION,
    "null": Type.TYPE_UNKNOWN,
}

@dataclass
class ColumnType:
    field_type: Optional[str]
    logical_type: Optional[str]


class NestedColumnType(ColumnType):
    field_type = "nested"
    fields: dict[str, ColumnType] = dict()


def map_column_type(column_type: ColumnType):
    odd_type = TYPES_CLICKHOUSE_TO_ODD.get(column_type.field_type)

    if not odd_type:
        odd_type = Type.TYPE_UNKNOWN

    return odd_type
