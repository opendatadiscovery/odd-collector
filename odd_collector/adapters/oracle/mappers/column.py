from odd_models.models import DataSetField, DataSetFieldType

from ..domain import Column
from ..generator import OracleGenerator
from .colum_type import map_type


def map_column(
    generator: OracleGenerator, column_path: str, column: Column
) -> DataSetField:
    """
    Maps column to DataSetField
    :param generator - Oddrn generator
    :param column_path - parent type 'tables_column' | 'views_column'
    :param column - Column model
    """
    generator.set_oddrn_paths(**{column_path: column.name})
    return DataSetField(
        name=column.name,
        oddrn=generator.get_oddrn_by_path(column_path),
        type=DataSetFieldType(
            type=map_type(colum_type=column.type),
            is_nullable=column.nullable,
            logical_type=str(column.logical_type),
        ),
    )
