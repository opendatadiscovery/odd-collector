_DATETIME_FORMAT = "%Y-%m-%d %H:%M%Z"

_METADATA_SCHEMA_URL_PREFIX: str = \
    'https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/' \
    'extensions/dbt.json#/definitions/Dbt'

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + 'DataSetExtension'
_data_set_field_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + 'DataSetFieldExtension'
_data_transformer_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + 'DataTransformerExtension'

_data_set_metadata_excluded_keys: set = {'metadata', 'columns', 'stats'}

_data_set_field_metadata_excluded_keys: set = {'type', 'comment', 'name'}

_data_transformer_metadata_excluded_keys: set = {'name'}
