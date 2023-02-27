from typing import List, Optional, Tuple

from odd_models.models import DataSetField, DataSetFieldType, Type, DataSetFieldEnumValue
from oddrn_generator import PostgresqlGenerator

from odd_collector.adapters.postgresql.config import (
    _data_set_field_metadata_excluded_keys,
    _data_set_field_metadata_schema_url,
)

from .metadata import append_metadata_extension
from ..models import ColumnMetadata, EnumTypeLabel
from .types import TYPES_SQL_TO_ODD


def map_column(
    column_metadata: ColumnMetadata,
    oddrn_generator: PostgresqlGenerator,
    owner: str,
    parent_oddrn_path: str,
    enum_type_labels: Optional[List[EnumTypeLabel]],
    is_primary: bool = False
) -> DataSetField:
    name: str = column_metadata.column_name

    data_type: Type = TYPES_SQL_TO_ODD.get(column_metadata.data_type, Type.TYPE_UNKNOWN) \
        if enum_type_labels is None or not len(enum_type_labels) \
        else Type.TYPE_STRING

    logical_type: str = column_metadata.data_type \
        if enum_type_labels is None or not len(enum_type_labels) \
        else enum_type_labels[0].type_name

    enum_values = [DataSetFieldEnumValue(name=etl.label) for etl in enum_type_labels] \
        if enum_type_labels is not None \
        else None

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(
            f"{parent_oddrn_path}_columns", name
        ),  # getting tables_columns or views_columns
        name=name,
        owner=owner,
        metadata=[],
        is_primary_key=is_primary,
        type=DataSetFieldType(
            type=data_type,
            logical_type=logical_type,
            is_nullable=column_metadata.is_nullable == "YES",
        ),
        default_value=column_metadata.column_default,
        description=column_metadata.description,
        enum_values=enum_values
    )

    append_metadata_extension(
        dsf.metadata,
        _data_set_field_metadata_schema_url,
        column_metadata,
        _data_set_field_metadata_excluded_keys,
    )
    return dsf
