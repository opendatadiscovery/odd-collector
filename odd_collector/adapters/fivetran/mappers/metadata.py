from functools import singledispatch

from odd_models.models import MetadataExtension

from odd_collector.adapters.fivetran.domain.column import ColumnMetadata
from odd_collector.adapters.fivetran.domain.table import TableMetadata

data_set_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/fivetran.json#/definitions/FivetranDataSetExtension"
)

data_set_field_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/fivetran.json#/definitions/FivetranDataSetFieldExtension"
)


@singledispatch
def map_metadata(arg) -> MetadataExtension:
    pass


@map_metadata.register
def _(arg: TableMetadata) -> MetadataExtension:
    return MetadataExtension(
        schema_url=data_set_metadata_schema_url, metadata=arg.metadata
    )


@map_metadata.register
def _(arg: ColumnMetadata) -> MetadataExtension:
    return MetadataExtension(
        schema_url=data_set_field_metadata_schema_url, metadata=arg.metadata
    )
