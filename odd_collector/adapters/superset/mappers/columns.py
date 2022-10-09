from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator.generators import Generator
from odd_models.models import Type as ColumnType
from ..domain.column import Column


def map_column(oddrn_generator: Generator, column: Column) -> DataSetField:
    column_name = column.name
    column_type = column.remote_type

    try:
        return DataSetField(
            oddrn=oddrn_generator.get_oddrn_by_path("columns", column_name),
            name=column_name,
            type=DataSetFieldType(
                type=ColumnType.TYPE_UNKNOWN,
                logical_type=column_type,
                is_nullable=False,
            ),
        )
    except Exception as e:
        raise MappingDataError(f"Mapping column {column_name} failed") from e
