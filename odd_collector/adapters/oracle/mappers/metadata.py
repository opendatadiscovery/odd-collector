from functools import singledispatch
from typing import Union

from odd_models.models import MetadataExtension

from odd_collector.adapters.oracle.domain import Column, Table, View

data_set_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/oracle.json#/definitions/OracleDataSetExtension"
)

data_set_field_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/oracle.json#/definitions/OracleDataSetFieldExtension"
)


@singledispatch
def map_metadata(arg) -> MetadataExtension:
    pass


@map_metadata.register(View)
@map_metadata.register(Table)
def _(arg: Union[View, Table]) -> MetadataExtension:
    return MetadataExtension(
        schema_url=data_set_metadata_schema_url, metadata=arg.metadata
    )


@map_metadata.register
def _(arg: Column) -> MetadataExtension:
    return MetadataExtension(
        schema_url=data_set_field_metadata_schema_url, metadata=arg.metadata
    )
