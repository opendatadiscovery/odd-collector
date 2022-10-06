from typing import Union

from odd_models.models import MetadataExtension

from ..domain.table import Column, Table

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/vertica.json#/definitions/Vertica"
)

_data_set_metadata_schema_url: str = _METADATA_SCHEMA_URL_PREFIX + "DataSetExtension"
_data_set_field_metadata_schema_url: str = (
    _METADATA_SCHEMA_URL_PREFIX + "DataSetFieldExtension"
)


_data_set_metadata_excluded_keys: set = {
    "table_schema",
    "table_name",
    "table_type",
    "view_definition",
    "description",
    "owner_name",
    "table_rows",
    "columns",
}


_data_set_field_metadata_excluded_keys: set = {
    "table_schema",
    "table_name",
    "column_name",
    "column_default",
    "is_nullable",
    "data_type",
    "description",
    "is_primary_key",
}


def map_metadata(metadata: Union[Column, Table]) -> MetadataExtension:
    metadata_dict = metadata.__dict__
    if metadata is None or len(metadata_dict) == 0:
        return None
    excluded_keys = None
    schema_url = None
    if isinstance(metadata, Table):
        excluded_keys = _data_set_metadata_excluded_keys
        schema_url = _data_set_metadata_schema_url

    if isinstance(metadata, Column):
        excluded_keys = _data_set_field_metadata_excluded_keys
        schema_url = _data_set_field_metadata_schema_url

    metadata_to_add = {}
    for key, value in metadata_dict.items():
        if key not in excluded_keys and value is not None:
            metadata_to_add[key] = value

    md_extension = MetadataExtension(schema_url=schema_url, metadata=metadata_to_add)
    return md_extension
