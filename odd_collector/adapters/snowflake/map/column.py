from copy import deepcopy
from typing import Dict, List

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import SnowflakeGenerator

from odd_collector.adapters.snowflake.domain import Column

from .entity_type_path_key import EntityTypePathKey

data_types_mapping: Dict[str, Type] = {
    "TIMESTAMP_TZ": Type.TYPE_DATETIME,
    "REAL": Type.TYPE_INTEGER,
    "TIMESTAMP_NTZ": Type.TYPE_DATETIME,
    "BOOLEAN": Type.TYPE_BOOLEAN,
    "VARIANT": Type.TYPE_CHAR,
    "TIME": Type.TYPE_TIME,
    "TEXT": Type.TYPE_STRING,
    "FIXED": Type.TYPE_NUMBER,
    "DATE": Type.TYPE_DATETIME,
    "TIMESTAMP_LTZ": Type.TYPE_DATETIME,
    "INT": Type.TYPE_INTEGER,
    "INTEGER": Type.TYPE_INTEGER,
    "SMALLINT": Type.TYPE_INTEGER,
    "BIGINT": Type.TYPE_INTEGER,
    "NUMBER": Type.TYPE_NUMBER,
    "DECIMAL": Type.TYPE_NUMBER,
    "NUMERIC": Type.TYPE_NUMBER,
    "DOUBLE": Type.TYPE_NUMBER,
    "FLOAT": Type.TYPE_NUMBER,
    "STRING": Type.TYPE_STRING,
    "VARCHAR": Type.TYPE_STRING,
    "CHAR": Type.TYPE_CHAR,
    "CHARACTER": Type.TYPE_CHAR,
    "DATETIME": Type.TYPE_DATETIME,
    "TIMESTAMP": Type.TYPE_DATETIME,
    "ARRAY": Type.TYPE_LIST,
}


def get_field_type(column: Column) -> DataSetFieldType:
    return DataSetFieldType(
        type=data_types_mapping.get(column.data_type, Type.TYPE_UNKNOWN),
        logical_type=column.data_type,
        is_nullable=column.is_nullable == "YES",
    )


def map_columns(
    columns: List[Column], parent_path: EntityTypePathKey, generator: SnowflakeGenerator
) -> List[DataSetField]:
    generator = deepcopy(generator)
    result: List[DataSetField] = []

    for column in columns:
        column_oddrn_key = f"{parent_path.value}_columns"
        generator_params = {column_oddrn_key: column.column_name}
        generator.set_oddrn_paths(**generator_params)
        dataset_set_field = DataSetField(
            oddrn=generator.get_oddrn_by_path(column_oddrn_key),
            name=column.column_name,
            type=get_field_type(column),
            default_value=column.column_default,
            is_sort_key=bool(column.is_clustering_key),
            is_primary_key=column.is_primary_key,
        )
        result.append(dataset_set_field)

    return result
