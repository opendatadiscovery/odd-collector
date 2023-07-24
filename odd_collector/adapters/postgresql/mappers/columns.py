from odd_models.models import (
    DataSetField,
    DataSetFieldEnumValue,
    DataSetFieldType,
    Type,
)
from oddrn_generator import PostgresqlGenerator

from ..models import Column
from .metadata import get_column_metadata
from .types import TYPES_SQL_TO_ODD


def is_enum(column: Column) -> bool:
    return column.enums is not None and len(column.enums) > 0


def map_column(
    column: Column,
    generator: PostgresqlGenerator,
    path: str,
) -> DataSetField:
    name: str = column.column_name
    enum_values = None

    if is_enum(column):
        data_type = Type.TYPE_STRING
        logical_type = column.enums[0].type_name
        enum_values = [DataSetFieldEnumValue(name=etl.label) for etl in column.enums]
    else:
        data_type = TYPES_SQL_TO_ODD.get(column.data_type, Type.TYPE_UNKNOWN)
        logical_type = column.data_type

    return DataSetField(
        owner=None,
        oddrn=generator.get_oddrn_by_path(
            f"{path}_columns", name
        ),  # getting tables_columns or views_columns
        name=name,
        metadata=[get_column_metadata(entity=column)],
        is_primary_key=column.is_primary,
        type=DataSetFieldType(
            type=data_type,
            logical_type=logical_type,
            is_nullable=column.is_nullable == "YES",
        ),
        default_value=column.column_default,
        description=column.description,
        enum_values=enum_values,
    )
