from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import TableauGenerator

from . import (
    DATA_SET_FIELD_SCHEMA,
    DATA_SET_FIELD_EXCLUDED_KEYS,
)
from .metadata import extract_metadata
from .types import map_type


def map_column(
    column: dict, oddrn_generator: TableauGenerator, owner: str
) -> DataSetField:
    column_name = column.get("name")
    column_type = column.get("remoteType")

    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", column_name),
        name=column_name,
        owner=owner,
        metadata=extract_metadata(
            DATA_SET_FIELD_SCHEMA,
            column,
            DATA_SET_FIELD_EXCLUDED_KEYS,
        ),
        type=DataSetFieldType(
            type=map_type(column_type),
            logical_type=column_type,
            is_nullable=True,
        ),
        description=column["description"] or None,
    )
