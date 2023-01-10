from typing import Union

from funcy import select_values
from odd_models.models import MetadataExtension

from ..domain import Column, Table, View

_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/snowflake.json#/definitions/Snowflake"
)


def map_metadata(model: Union[Table, View, Column]) -> MetadataExtension:
    suffix = (
        "DataSetFieldExtension" if isinstance(model, Column) else "DataSetExtension"
    )
    schema_url = _METADATA_SCHEMA_URL_PREFIX + suffix

    return MetadataExtension(
        schema_url=schema_url,
        metadata=select_values(lambda v: v is not None, model.metadata),
    )
