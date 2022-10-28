_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/snowflake.json#/definitions/Snowflake"
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
    "table_owner",
    "row_count",
    "comment",
    "last_altered",
    "created",
}
_data_set_field_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "column_default",
    "is_nullable",
    "data_type",
    "comment",
}
