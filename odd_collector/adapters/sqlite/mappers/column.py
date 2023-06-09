from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import SQLiteGenerator

from .column_type import map_type
from ..domain import Column


def map_column(
    generator: SQLiteGenerator, column_path: str, column: Column
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
            type=map_type(column.type),
            is_nullable=column.nullable,
            logical_type=str(column.logical_type),
        ),
        is_primary_key=column.primary_key,
        metadata=[extract_metadata("sqlite", column, DefinitionType.DATASET_FIELD)],
    )
