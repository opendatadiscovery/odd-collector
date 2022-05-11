from typing import NamedTuple

from odd_models.models import MetadataExtension

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/Max3kkk/odd-tarantool-adapter/dev/metadata_schema.json"
    "#/definitions/Tarantool"
)
data_set_metadata_schema_url: str = f"{_METADATA_SCHEMA_URL_PREFIX}DataSetExtension"
data_set_field_metadata_schema_url: str = (
    f"{_METADATA_SCHEMA_URL_PREFIX}DataSetFieldExtension"
)


def append_metadata_extension(
    metadata_list: list[MetadataExtension],
    schema_url: str,
    named_tuple: NamedTuple,
    excluded_keys: set = None,
):
    if named_tuple is None or len(named_tuple) <= 0:
        return
    metadata: dict = named_tuple._asdict()
    if excluded_keys is not None:
        for key in excluded_keys:
            metadata.pop(key)
    metadata_wo_none: dict = {
        key: value for key, value in metadata.items() if value is not None
    }

    metadata_list.append(
        MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
    )
