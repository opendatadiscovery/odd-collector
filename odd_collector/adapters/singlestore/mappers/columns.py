from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import SingleStoreGenerator

from .metadata import (
    append_metadata_extension,
    convert_bytes_to_str,
    _data_set_field_metadata_schema_url,
)
from .models import ColumnMetadata
from .types import TYPES_SQL_TO_ODD

_data_set_field_metadata_excluded_keys: set = {
    "table_catalog",
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "column_default",
    "is_nullable",
    "column_type",
    "column_comment",
}


def map_column(
    column_metadata: ColumnMetadata,
    oddrn_generator: SingleStoreGenerator,
    owner: str,
    oddrn_path: str,
) -> DataSetField:
    name: str = column_metadata.column_name
    data_type: str = convert_bytes_to_str(column_metadata.data_type)
    description = convert_bytes_to_str(column_metadata.column_comment)

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(
            f"{oddrn_path}_columns", name
        ),  # getting tables_columns or views_columns
        name=name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=convert_bytes_to_str(column_metadata.data_type),
            is_nullable=column_metadata.is_nullable == "YES",
        ),
        default_value=convert_bytes_to_str(column_metadata.column_default),
        description=description or None,
        is_primary_key=bool(column_metadata.column_key),
    )

    append_metadata_extension(
        dsf.metadata,
        _data_set_field_metadata_schema_url,
        column_metadata,
        _data_set_field_metadata_excluded_keys,
    )

    return dsf
