from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import VerticaGenerator

from ..domain.column import Column
from ..mapper.metadata import map_metadata
from ..mapper.types import TYPES_SQL_TO_ODD


def map_column(oddrn_generator: VerticaGenerator, column: Column, owner: str) -> DataSetField:
    try:
        name: str = column.column_name
        data_type: str = column.data_type

        dsf: DataSetField = DataSetField(
            oddrn=oddrn_generator.get_oddrn_by_path(
                f"tables_columns", name
            ),
            name=name,
            owner=owner,
            metadata=[map_metadata(column)],
            is_primary_key=column.is_primary_key,
            type=DataSetFieldType(
                type=TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
                logical_type=column.data_type,
                is_nullable=column.is_nullable == "YES",
            ),
            default_value=column.column_default,
            description=column.description,
        )

        return dsf
    except Exception as e:
        raise MappingDataError(f"Mapping column {column.column_name} failed") from e
