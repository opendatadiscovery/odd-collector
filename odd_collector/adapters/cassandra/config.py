_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/cassandra.json#/definitions/Cassandra"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)
_data_set_metadata_excluded_keys = ""
_data_set_field_metadata_excluded_keys = ""
