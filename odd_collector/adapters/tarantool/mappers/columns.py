from collections import namedtuple

from oddrn_generator import TarantoolGenerator
from odd_models.models import DataSetField, DataSetFieldType, Type

from .metadata import append_metadata_extension, data_set_field_metadata_schema_url
from .types import TYPES_Tarantool_TO_ODD

_column_metadata: str = "name, type, is_nullable"
ColumnMetadata = namedtuple("ColumnMetadataNamedtuple", _column_metadata)


def map_column(
    column_metadata: ColumnMetadata,
    oddrn_generator: TarantoolGenerator,
    owner: str,
    parent_oddrn_path: str,
) -> DataSetField:
    name: str = column_metadata.name
    data_type: str = column_metadata.type
    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(f"{parent_oddrn_path}", name),
        name=name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_Tarantool_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=column_metadata.type,
            is_nullable=column_metadata.is_nullable == "true",
        ),
    )

    append_metadata_extension(
        dsf.metadata, data_set_field_metadata_schema_url, column_metadata
    )
    return dsf
