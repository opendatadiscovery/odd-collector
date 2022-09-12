from typing import NamedTuple, Any


class ColumnMetadata(NamedTuple):
    table_cat: Any
    table_schem: Any
    table_name: Any
    column_name: Any
    data_type: Any
    type_name: Any
    column_size: Any
    buffer_length: Any
    decimal_digits: Any
    num_prec_radix: Any
    nullable: Any
    remarks: Any
    column_def: Any
    sql_data_type: Any
    sql_datetime_sub: Any
    char_octet_length: Any
    ordinal_position: Any
    is_nullable: Any
    scope_catalog: Any
    scope_schema: Any
    scope_table: Any
    source_data_type: Any
    is_autoincrement: Any
    is_generatedcolumn: Any

    @classmethod
    def get_str_fields(cls):
        return ", ".join(cls._fields)
