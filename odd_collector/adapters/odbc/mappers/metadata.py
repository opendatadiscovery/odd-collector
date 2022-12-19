from functools import singledispatch

from odd_models.models import MetadataExtension

from ..domain import BaseTable, Column

data_set_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/odbc.json#/definitions/OdbcDataSetExtension"
)

data_set_field_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/odbc.json#/definitions/OdbcDataSetFieldExtension"
)


@singledispatch
def map_metadata(arg) -> MetadataExtension:
    pass


@map_metadata.register
def _(arg: BaseTable) -> MetadataExtension:
    return MetadataExtension(
        schema_url=data_set_metadata_schema_url, metadata=arg.metadata
    )


@map_metadata.register
def _(arg: Column) -> MetadataExtension:
    return MetadataExtension(
        schema_url=data_set_field_metadata_schema_url, metadata=arg.metadata
    )
