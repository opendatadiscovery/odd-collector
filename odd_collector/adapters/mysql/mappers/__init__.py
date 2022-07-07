from typing import Any, NamedTuple

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/mysql.json#/definitions/Mysql"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)

_data_set_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "table_type",
    "table_rows",
    "create_time",
    "update_time",
    "table_comment",
    "view_definition",
}

_data_set_field_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "column_default",
    "is_nullable",
    "column_type",
    "column_comment",
}

_column_table: str = (
    "information_schema.columns "
    "where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
)
_column_order_by: str = "table_catalog, table_schema, table_name, ordinal_position"


class MetadataNamedtuple(NamedTuple):
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


class ColumnMetadataNamedtuple(NamedTuple):
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
