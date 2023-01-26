_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/singlestore.json#/definitions/SingleStore"
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
