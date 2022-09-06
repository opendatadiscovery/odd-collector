_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/clickhouse.json#/definitions/ClickHouse"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)

_data_set_metadata_excluded_keys: set = {"name", "database"}
_data_set_field_metadata_excluded_keys: set = {
    "database",
    "table",
    "name",
    "type",
    "default_kind",
    "default_expression",
}
