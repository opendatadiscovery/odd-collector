from copy import deepcopy

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import OdbcGenerator

from odd_collector.helpers.bytes_to_str import convert_bytes_to_str

from ..domain import Column
from .metadata import map_metadata
from .types import TYPES_SQL_TO_ODD


def map_column(
    oddrn_generator: OdbcGenerator, parent_oddrn_path: str, column: Column
) -> DataSetField:
    generator = deepcopy(oddrn_generator)

    type_name = column.type_name

    return DataSetField(
        oddrn=generator.get_oddrn_by_path(
            f"{parent_oddrn_path}_columns", column.column_name
        ),
        name=column.column_name,
        metadata=[map_metadata(column)],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(
                convert_bytes_to_str(type_name), Type.TYPE_UNKNOWN
            ),
            logical_type=convert_bytes_to_str(type_name),
            is_nullable=column.is_nullable == "YES",
        ),
        default_value=convert_bytes_to_str(column.column_def),
        description=convert_bytes_to_str(column.remarks),
    )
