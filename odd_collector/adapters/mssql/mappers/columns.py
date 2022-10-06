from odd_models.models import (
    DataSetField,
    DataSetFieldStat,
    DataSetFieldType,
    MetadataExtension,
    Type,
)
from oddrn_generator import MssqlGenerator

from . import ColumnMetadataNamedtuple, _data_set_field_metadata_schema_url
from .helpers import __convert_bytes_to_str, __convert_bytes_to_str_in_dict
from .types import TYPES_SQL_TO_ODD


def map_column(
    column_metadata: ColumnMetadataNamedtuple,
    oddrn_generator: MssqlGenerator,
    owner: str,
    parent_oddrn_path: str,
) -> DataSetField:
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(
            f"{parent_oddrn_path}_columns", column_metadata.column_name
        ),  # getting tables_columns or views_columns
        name=column_metadata.column_name,
        owner=owner,
        metadata=[
            MetadataExtension(
                schema_url=_data_set_field_metadata_schema_url,
                metadata=__convert_bytes_to_str_in_dict(column_metadata._asdict()),
            )
        ],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(
                __convert_bytes_to_str(column_metadata.data_type), Type.TYPE_UNKNOWN
            ),
            logical_type=__convert_bytes_to_str(column_metadata.data_type),
            is_nullable=column_metadata.is_nullable == "YES",
        ),
        default_value=__convert_bytes_to_str(column_metadata.column_default),
        description=None,
        stats=DataSetFieldStat(),
        is_key=False,
        is_value=False,
    )
