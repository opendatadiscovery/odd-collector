
from dataclasses import asdict, astuple
from typing import Union

from odd_models.models import MetadataExtension
from odd_collector.adapters.snowflake.mappers.models import (
    ColumnMetadata,
    TableMetadata,
)


def append_metadata_extension(
    metadata_list: list[MetadataExtension],
    schema_url: str,
    metadata: Union[ColumnMetadata, TableMetadata],
    excluded_keys: set = None,
):
    if metadata is not None and len(astuple(metadata)) > 0:
        metadata: dict = asdict(metadata)
        if excluded_keys is not None:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {}
        for key, value in metadata.items():
            if value is not None:
                metadata_wo_none[key] = value
        metadata_list.append(
            MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
        )
