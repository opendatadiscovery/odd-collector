from typing import Any, NamedTuple


class ColumnMetadata(NamedTuple):
    table_cat: Any
    table_schem: Any
    table_name: Any
    column_name: Any
    type_name: Any


class TableMetadata(NamedTuple):
    table_cat: Any
    table_schem: Any
    table_name: Any
    table_type: Any
