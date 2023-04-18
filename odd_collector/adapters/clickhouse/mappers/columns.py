import re
from typing import Optional, List

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import ClickHouseGenerator

from ..domain import Column
from ..logger import logger
from . import (
    _data_set_field_metadata_excluded_keys,
    _data_set_field_metadata_schema_url,
)
from .metadata import extract_metadata
from .types import TYPES_SQL_TO_ODD


def is_nested(column: Column):
    # Check if column contains nested columns
    return '.' in column.name \
        and 'Array(' in str(column.type)


def is_complex_nested(column: Column):
    return is_nested(column) and 'Nested' in str(column.type)


def build_nested_columns(
    column: Column,
    parent_oddrn_path: str,
    oddrn_generator: ClickHouseGenerator,
    owner: Optional[str] = None
):
    result = []
    # Get the sequence of nested columns
    column_names = column.name.split('.')

    # Oddrn of first-order column
    parent_oddrn = oddrn_generator.get_oddrn_by_path(
        f"{parent_oddrn_path}_columns", column_names[0] 
    )

    # Parse the nested cell 
    parent_dataset_field = DataSetField(
        oddrn=parent_oddrn,
        name=column_names[0],
        owner=owner,
        type=DataSetFieldType(
            type=_get_column_type(column.type),
            is_nullable=column.type.startswith("Nullable"),
            logical_type=column.type,
        ),
    )
    result.append(parent_dataset_field)

    # Parse nested columns
    for name in column_names[1:]:
        dataset_field = DataSetField(
            oddrn=f"{parent_oddrn}/keys/{name}",
            name=name,
            type=DataSetFieldType(
                type=_get_nested_column_type(str(column.type)),
                is_nullable=False,
                # TODO: need clickhosue origin type 
                logical_type=str(_get_nested_column_type(str(column.type)))
            ),
            is_key=False,
            parent_field_oddrn=parent_oddrn,
            owner=owner
        )
        result.append(dataset_field)
    return result


def map_column(
    column: Column,
    oddrn_generator: ClickHouseGenerator,
    owner: Optional[str],
    parent_oddrn_path: str,
) -> List[DataSetField]:
    logger.info(column)
    if is_nested(column) and not is_complex_nested(column):
        return build_nested_columns(column, parent_oddrn_path, oddrn_generator, owner)
    else:
        dataset_field = DataSetField(
            oddrn=oddrn_generator.get_oddrn_by_path(
                f"{parent_oddrn_path}_columns", column.name
            ),  # getting tables_columns or views_columns
            name=column.name,
            owner=owner,
            metadata=[
                extract_metadata(
                    _data_set_field_metadata_schema_url,
                    _data_set_field_metadata_excluded_keys,
                    column,
                )
            ],
            type=DataSetFieldType(
                type=_get_column_type(str(column.type)),
                is_nullable=str(column.type).startswith("Nullable"),
                logical_type=str(column.type),
            ),
            default_value=column.default_kind,
            is_primary_key=column.is_in_primary_key,
            is_sort_key=column.is_in_sorting_key,
            is_key=False,
            is_value=False,
        )
        return [dataset_field]

def _get_nested_column_type(data_type: str):
    if 'Nested' in data_type:
        # Stub for complex nested structures
        return Type.TYPE_STRING
    data_type = data_type.replace('Array', '').replace('(', '').replace(')', '')
    return _get_column_type(data_type)


def _get_column_type(data_type: str):
    # trim Nullable
    trimmed = re.search("Nullable\((.+?)\)", data_type)
    if trimmed:
        data_type = trimmed.group(1)

    # trim LowCardinality
    trimmed = re.search("LowCardinality\((.+?)\)", data_type)
    if trimmed:
        data_type = trimmed.group(1)

    if data_type.startswith("Array"):
        data_type = "Array"
    elif data_type.startswith("Enum8"):
        data_type = "Enum8"
    elif type := re.search("SimpleAggregateFunction\(\w+,\s(\w+)\)", data_type):
        data_type = type.group(1)

    logger.debug(f"Data type after parsing: {data_type}")
    return TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN)
