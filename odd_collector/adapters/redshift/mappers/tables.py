import pytz
from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup
from oddrn_generator import RedshiftGenerator

from . import _data_set_metadata_schema_url_info, _data_set_metadata_excluded_keys_info
from .columns import map_column
from .metadata import (
    _append_metadata_extension,
    MetadataTables,
    MetadataColumns,
    MetadataColumn,
)
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data
from typing import List


def map_table(
    oddrn_generator: RedshiftGenerator,
    mtables: MetadataTables,
    mcolumns: MetadataColumns,
    database: str,
    schemas: List[str]
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    column_index: int = 0

    for mtable in mtables.items:
        if mtable.schema_name not in schemas and len(schemas): continue
        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
            mtable.base.table_type, DataEntityType.UNKNOWN
        )
        oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

        oddrn_generator.set_oddrn_paths(
            **{"schemas": mtable.schema_name, oddrn_path: mtable.table_name}
        )

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
            name=mtable.table_name,
            owner=mtable.all.table_owner,
            metadata=[],
            type=data_entity_type,
            dataset=DataSet(
                field_list=[],
            ),
            description=mtable.base.remarks if mtable.base is not None else None,
        )
        data_entities.append(data_entity)

        if (
            mtable.all.table_type == "TABLE"
        ):  # data_entity.dataset.subtype == 'DATASET_TABLE'
            # it is for full tables only
            _append_metadata_extension(
                data_entity.metadata,
                _data_set_metadata_schema_url_info,
                mtable.info,
                _data_set_metadata_excluded_keys_info,
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
                data_entity.dataset.rows_number = int(
                    mtable.info.estimated_visible_rows
                )
            else:
                if mtable.info.tbl_rows is not None:
                    data_entity.dataset.rows_number = int(mtable.info.tbl_rows)

        # DataTransformer
        if data_entity.type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(
                mtable.all.view_ddl, oddrn_generator
            )

        # DatasetField
        while column_index < len(mcolumns.items):  # exclude right only rows
            mcolumn: MetadataColumn = mcolumns.items[column_index]

            if (
                mcolumn.schema_name == mtable.schema_name
                and mcolumn.table_name == mtable.table_name
            ):
                data_entity.dataset.field_list.append(
                    map_column(mcolumn, oddrn_generator, data_entity.owner, oddrn_path)
                )
                column_index += 1
            else:
                break
    data_entities.append(
        DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("databases"),
            name=database,
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            data_entity_group=DataEntityGroup(
                entities_list=[de.oddrn for de in data_entities]
            ),
        )
    )

    return data_entities
