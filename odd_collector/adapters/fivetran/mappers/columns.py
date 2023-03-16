from odd_models.models import DataSetField, DataSetFieldType

from .metadata import map_metadata
from .types import TYPES_FIVETRAN_TO_ODD
from ..domain.column import ColumnMetadata
from oddrn_generator import FivetranGenerator
from odd_models.models import Type


def map_column(
    generator: FivetranGenerator, column_path: str, column: ColumnMetadata
) -> DataSetField:
    """
    Maps column to DataSetField
    :param generator - Oddrn generator
    :param column_path - parent type 'columns'
    :param column - Column model
    """
    generator.set_oddrn_paths(**{column_path: column.name_in_source})
    return DataSetField(
        name=column.name_in_source,
        oddrn=generator.get_oddrn_by_path(column_path),
        metadata=[map_metadata(column)],
        type=DataSetFieldType(
            type=TYPES_FIVETRAN_TO_ODD.get(column.type_in_source, Type.TYPE_UNKNOWN),
            is_nullable=not column.is_primary_key,
            logical_type=str(column.type_in_source),
        ),
        is_primary_key=column.is_primary_key,
    )
