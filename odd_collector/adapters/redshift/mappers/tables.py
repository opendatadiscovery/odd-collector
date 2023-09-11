import pytz
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import RedshiftGenerator

from . import _data_set_metadata_excluded_keys_info, _data_set_metadata_schema_url_info
from .columns import map_column
from .metadata import (
    MetadataColumns,
    _append_metadata_extension,
    MetadataTable,
)
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data


def map_table(
    oddrn_generator: RedshiftGenerator,
    mtable: MetadataTable,
    mcolumns: MetadataColumns,
    primary_keys: list[tuple],
) -> DataEntity:
    primary_keys: list[dict] = [
        {"table_name": pk[0], "column_name": pk[1]} for pk in primary_keys
    ]

    # for mtable in mtables.items:
    data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
        mtable.base.table_type, DataEntityType.UNKNOWN
    )
    oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

    oddrn_generator.set_oddrn_paths(**{oddrn_path: mtable.table_name})

    primary_key_columns = [
        pk.get("column_name")
        for pk in primary_keys
        if pk.get("table_name") == mtable.table_name
    ]

    # DataEntity
    data_entity: DataEntity = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path, mtable.table_name),
        name=mtable.table_name,
        owner=mtable.all.table_owner,
        metadata=[],
        type=data_entity_type,
        dataset=DataSet(
            field_list=[
                map_column(
                    mcolumn,
                    oddrn_generator,
                    mtable.all.table_owner,
                    oddrn_path,
                    mcolumn.base.column_name in primary_key_columns,
                )
                for mcolumn in mcolumns.items
            ],
        ),
        description=mtable.base.remarks if mtable.base is not None else None,
    )

    if mtable.all.table_type == "TABLE":
        # data_entity.dataset.subtype == 'DATASET_TABLE'
        # it is for full tables only
        _append_metadata_extension(
            metadata_list=data_entity.metadata,
            schema_url=_data_set_metadata_schema_url_info,
            metadata_dataclass=mtable.info,
            excluded_keys=_data_set_metadata_excluded_keys_info,
        )

    if mtable.all.table_creation_time is not None:
        data_entity.updated_at = mtable.all.table_creation_time.replace(
            tzinfo=pytz.utc
        ).isoformat()
        data_entity.created_at = mtable.all.table_creation_time.replace(
            tzinfo=pytz.utc
        ).isoformat()

    if mtable.info is not None:
        if mtable.info.estimated_visible_rows is not None:
            data_entity.dataset.rows_number = int(mtable.info.estimated_visible_rows)
        else:
            if mtable.info.tbl_rows is not None:
                data_entity.dataset.rows_number = int(mtable.info.tbl_rows)

    # DataTransformer
    if data_entity.type == DataEntityType.VIEW:
        data_entity.data_transformer = extract_transformer_data(
            mtable.all.view_ddl, oddrn_generator
        )

    return data_entity
