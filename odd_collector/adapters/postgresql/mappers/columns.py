from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import PostgresqlGenerator

from odd_collector.adapters.postgresql.config import (
    _data_set_field_metadata_schema_url,
    _data_set_field_metadata_excluded_keys,
)
from .metadata import append_metadata_extension
from .models import ColumnMetadata
from .types import TYPES_SQL_TO_ODD


def map_column(
    column_metadata: ColumnMetadata,
    oddrn_generator: PostgresqlGenerator,
    owner: str,
    parent_oddrn_path: str,
) -> DataSetField:
    name: str = column_metadata.column_name
    data_type: str = column_metadata.data_type

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(
            f"{parent_oddrn_path}_columns", name
        ),  # getting tables_columns or views_columns
        name=name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=column_metadata.data_type,
            is_nullable=column_metadata.is_nullable == "YES",
        ),
        default_value=column_metadata.column_default,
        description=column_metadata.description,
    )

    append_metadata_extension(
        dsf.metadata,
        _data_set_field_metadata_schema_url,
        column_metadata,
        _data_set_field_metadata_excluded_keys,
    )
    return dsf
