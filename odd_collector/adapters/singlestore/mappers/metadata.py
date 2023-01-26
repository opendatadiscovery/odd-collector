from dataclasses import asdict
from typing import Optional

from odd_models.models import MetadataExtension

from odd_collector.adapters.singlestore.mappers.models import SingleStoreMetadata

_data_set_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/singlestore.json#/definitions/SingleStoreDataSetExtension"
)
_data_set_field_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/singlestore.json#/definitions/SingleStoreDataSetFieldExtension"
)


def append_metadata_extension(
    metadata_list: list[MetadataExtension],
    schema_url: str,
    metadata_dataclass: SingleStoreMetadata,
    excluded_keys: set = None,
):
    if metadata_dataclass:
        metadata: dict = asdict(metadata_dataclass)
        if excluded_keys is not None:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {}
        for key, value in metadata.items():
            if value is not None and convert_bytes_to_str(value) != "":
                metadata_wo_none[key] = convert_bytes_to_str(value)
        metadata_list.append(
            MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
        )


def convert_bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    return value if type(value) is not bytes else value.decode("utf-8")
