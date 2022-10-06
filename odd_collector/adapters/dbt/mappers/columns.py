from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import DbtGenerator

from . import (
    _data_set_field_metadata_excluded_keys,
    _data_set_field_metadata_schema_url,
)
from .metadata import _append_metadata_extension
from .types import TYPES_SNOWFLAKE_TO_ODD


def map_column(
    column: dict, oddrn_generator: DbtGenerator, owner: str, parent_oddrn_path: str
) -> DataSetField:
    name: str = column["name"]

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(
            f"{parent_oddrn_path}_columns", name
        ),  # getting tables_columns or views_columns
        name=name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_SNOWFLAKE_TO_ODD.get(column["type"], Type.TYPE_UNKNOWN),
            logical_type=column["type"],
            is_nullable=True,
            description=column.get("comment"),
        ),
    )
    _append_metadata_extension(
        dsf.metadata,
        _data_set_field_metadata_schema_url,
        column,
        _data_set_field_metadata_excluded_keys,
    )

    return dsf
