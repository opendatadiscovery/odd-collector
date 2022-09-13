from typing import NamedTuple, Any


class ColumnMetadata(NamedTuple):
    table_cat: Any
    table_schem: Any
    table_name: Any
    column_name: Any
    type_name: Any

    @classmethod
    def get_str_fields(cls):
        return ", ".join(cls._fields)


class TableMetadata(NamedTuple):
    table_cat: Any
    table_schem: Any
    table_name: Any
    table_type: Any

    @classmethod
    def get_str_fields(cls):
        return ", ".join(cls._fields)
