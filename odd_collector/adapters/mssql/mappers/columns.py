from odd_models.models import DataSetField, DataSetFieldStat, DataSetFieldType, Type
from oddrn_generator import MssqlGenerator

from ..models import Column
from .metadata import column_metadata
from .types import TYPES_SQL_TO_ODD


def map_column(
    column: Column,
    oddrn_generator: MssqlGenerator,
    parent_oddrn_path: str,
) -> DataSetField:
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(
            f"{parent_oddrn_path}_columns", column.column_name
        ),  # getting tables_columns or views_columns
        name=column.column_name,
        owner=None,
        metadata=[column_metadata(entity=column)],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(column.data_type, Type.TYPE_UNKNOWN),
            logical_type=column.data_type,
            is_nullable=column.is_nullable == "YES",
        ),
        default_value=column.column_default,
        description=None,
        stats=DataSetFieldStat(),
        is_key=False,
        is_value=False,
        is_primary_key=column.is_primary_key,
    )
