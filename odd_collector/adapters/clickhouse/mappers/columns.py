import re

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


def map_column(
    column: Column,
    oddrn_generator: ClickHouseGenerator,
    owner: str,
    parent_oddrn_path: str,
) -> DataSetField:
    return DataSetField(
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
            type=_get_column_type(column.type),
            is_nullable=column.type.startswith("Nullable"),
            logical_type=column.type,
        ),
        default_value=column.default_kind,
        description="",
        is_primary_key=column.is_in_primary_key,
        is_sort_key=column.is_in_sorting_key,
        is_key=False,
        is_value=False,
    )


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
