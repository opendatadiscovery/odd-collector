from dataclasses import asdict, astuple
from typing import List, Union

from odd_models.models import MetadataExtension

from ..models import ColumnMetadata, TableMetadata


def append_metadata_extension(
    metadata_list: List[MetadataExtension],
    schema_url: str,
    metadata: Union[ColumnMetadata, TableMetadata],
    excluded_keys: set = None,
) -> None:
    if metadata is None or len(astuple(metadata)) <= 0:
        return None
    metadata: dict = asdict(metadata)
    if excluded_keys is not None:
        for key in excluded_keys:
            metadata.pop(key)
    metadata_wo_none: dict = {
        key: value for key, value in metadata.items() if value is not None
    }

    metadata_list.append(
        MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
    )
