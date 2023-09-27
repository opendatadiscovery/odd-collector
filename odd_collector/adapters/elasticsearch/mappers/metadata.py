import json
from dataclasses import dataclass
from typing import Any

from funcy import walk_values
from odd_collector_sdk.utils.metadata import (
    DefinitionType,
    HasMetadata,
    MetadataExtension,
    extract_metadata,
)

from ..logger import logger


@dataclass
class MetadataWrapper(HasMetadata):
    odd_metadata: dict[str, Any]


def extract_index_metadata(data: dict[str, Any]) -> MetadataExtension:
    meta_wrapper = MetadataWrapper(odd_metadata=data)
    return extract_metadata("elasticsearch", meta_wrapper, DefinitionType.DATASET)


def extract_template_metadata(data: dict[str, Any]) -> MetadataExtension:
    metadata = data

    try:
        metadata = walk_values(json.dumps, metadata)
    except Exception as e:
        logger.warning(f"Can't convert template metadata to json. {str(e)}")
        logger.debug(f"Template metadata: {data!r}")

    meta_wrapper = MetadataWrapper(odd_metadata=metadata)

    return extract_metadata("elasticsearch", meta_wrapper, DefinitionType.DATASET)


def extract_data_stream_metadata(data: dict[str, Any]) -> MetadataExtension:
    meta_wrapper = MetadataWrapper(odd_metadata=data)
    return extract_metadata(
        "elasticsearch", meta_wrapper, DefinitionType.DATASET, flatten=True
    )
