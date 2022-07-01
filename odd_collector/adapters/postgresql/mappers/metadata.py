from dataclasses import dataclass, astuple, asdict
from typing import List

from odd_models.models import MetadataExtension


def append_metadata_extension(
    metadata_list: List[MetadataExtension],
    schema_url: str,
    metadata_dataclass: dataclass,
    excluded_keys: set = None,
):
    if metadata_dataclass is not None and len(astuple(metadata_dataclass)) > 0:
        metadata: dict = asdict(metadata_dataclass)
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
