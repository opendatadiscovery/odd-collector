from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from oddrn_generator import DuckDBGenerator
from odd_models.models import DataSetField, DataSetFieldType, Type
from .models import DuckDBColumn
from .types import TYPES_SQL_TO_ODD


def split_by_braces(value: str) -> str:
    if isinstance(value, str):
        return value.split("(")[0].split("<")[0]
    return value


def map_column(oddrn_generator: DuckDBGenerator, column: DuckDBColumn) -> DataSetField:
    name = column.name
    _type = column.type
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", name),
        name=name,
        metadata=[extract_metadata("duckdb", column, DefinitionType.DATASET_FIELD)],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(split_by_braces(_type), Type.TYPE_UNKNOWN),
            logical_type=_type,
            is_nullable=False,
        ),
    )
