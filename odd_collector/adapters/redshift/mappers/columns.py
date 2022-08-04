from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import RedshiftGenerator

from . import (
    _data_set_field_metadata_schema_url_redshift,
    _data_set_field_metadata_excluded_keys_redshift,
)
from .metadata import _append_metadata_extension, MetadataColumn
from .types import TYPES_SQL_TO_ODD


def map_column(
    mcolumn: MetadataColumn,
    oddrn_generator: RedshiftGenerator,
    owner: str,
    parent_oddrn_path,
    is_primary: bool = False
) -> DataSetField:
    name: str = mcolumn.base.column_name

    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(f"{parent_oddrn_path}_columns", name),
        # getting tables_columns or views_columns
        name=name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(mcolumn.base.data_type, Type.TYPE_UNKNOWN),
            logical_type=mcolumn.redshift.data_type
            if mcolumn.redshift.data_type is not None
            else mcolumn.base.data_type,
            is_nullable=mcolumn.base.is_nullable == "YES",
        ),
        default_value=mcolumn.base.column_default,
        description=mcolumn.base.remarks,
        is_primary_key=is_primary,
        is_sort_key=bool(mcolumn.redshift.sortkey)
    )
    _append_metadata_extension(
        dsf.metadata,
        _data_set_field_metadata_schema_url_redshift,
        mcolumn.redshift,
        _data_set_field_metadata_excluded_keys_redshift,
    )
    return dsf
