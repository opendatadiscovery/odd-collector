import re

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import ClickHouseGenerator

from . import ColumnMetadataNamedtuple, _data_set_field_metadata_schema_url, _data_set_field_metadata_excluded_keys
from .metadata import _append_metadata_extension
from .types import TYPES_SQL_TO_ODD


def map_column(
        mcolumn: ColumnMetadataNamedtuple, oddrn_generator: ClickHouseGenerator, owner: str, parent_oddrn_path: str
) -> DataSetField:
    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(f"{parent_oddrn_path}_columns", mcolumn.name),  # getting tables_columns or views_columns
        name=mcolumn.name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=_get_column_type(mcolumn.type),
            is_nullable=mcolumn.type.startswith("Nullable"),
            logical_type=mcolumn.type,
        ),
        default_value=mcolumn.default_kind,
        description="",
        is_key=False,
        is_value=False,
    )
    _append_metadata_extension(dsf.metadata, _data_set_field_metadata_schema_url, mcolumn,
                               _data_set_field_metadata_excluded_keys)
    return dsf


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

    return TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN)
