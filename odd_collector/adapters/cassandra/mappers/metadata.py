from dataclasses import asdict, astuple
from typing import NamedTuple, Union

from odd_models.models import MetadataExtension

from odd_collector.adapters.cassandra.mappers.models import (
    ColumnMetadata,
    TableMetadata,
)


def get_metadata_extension(
    schema_url: str,
    metadata: Union[ColumnMetadata, TableMetadata],
    excluded_keys: set = None,
) -> Union[MetadataExtension, None]:
    """
    A method to generate the MetadataExtension of named tuple, excluding un-needed keys and the keys whose values are
    Nones.
    :param schema_url: the url where the schema of the named tuple's field can be found.
    :param metadata: the tuple where the metadata extension will be extracted.
    :param excluded_keys: the un-needed keys in the metadata extension.
    :return: a MetadataExtension object containing the necessary information without the None values.
    """
    if metadata is not None and len(astuple(metadata)) > 0:
        metadata: dict = asdict(metadata)
        if excluded_keys:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {
            key: value for key, value in metadata.items() if value is not None
        }
        return MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
