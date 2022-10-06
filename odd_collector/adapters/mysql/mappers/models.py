from typing import Any, NamedTuple


class TableMetadata(NamedTuple):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    table_type: Any
    engine: Any
    version: Any
    row_format: Any
    table_rows: Any
    avg_row_length: Any
    data_length: Any
    max_data_length: Any
    index_length: Any
    data_free: Any
    auto_increment: Any
    create_time: Any
    update_time: Any
    check_time: Any
    table_collation: Any
    checksum: Any
    create_options: Any
    table_comment: Any
    view_definition: Any


class ColumnMetadata(NamedTuple):
    # , srs_id'  # srs_id column is not included in MariaDB (MySQL only)
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_scale: Any
    datetime_precision: Any
    character_set_name: Any
    collation_name: Any
    column_type: Any
    column_key: Any
    extra: Any
    privileges: Any
    column_comment: Any
    generation_expression: Any

    @classmethod
    def get_str_fields(cls):
        return ", ".join(cls._fields)
