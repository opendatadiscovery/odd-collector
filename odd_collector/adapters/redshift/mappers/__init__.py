_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/redshift.json#/definitions/Redshift"
)

_data_set_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionBase"
)
_data_set_metadata_schema_url_all: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionAll"
)
_data_set_metadata_schema_url_redshift: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionRedshift"
)
_data_set_metadata_schema_url_external: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionExternal"
)
_data_set_metadata_schema_url_info: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
)

_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtensionBase"
)
_data_set_field_metadata_schema_url_redshift: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)
_data_set_field_metadata_schema_url_external: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtensionExternal"
)

_data_set_metadata_schema_url_function: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionFunction"
)
_data_set_metadata_schema_url_call: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetExtensionCall"
)

_data_set_metadata_excluded_keys_info: set = {"database", "schema", "table"}

_data_set_field_metadata_excluded_keys_redshift: set = {
    "database_name",
    "schema_name",
    "table_name",
    "column_name",
    "data_type",
    "column_default",
    "is_nullable",
    "remarks",
}
