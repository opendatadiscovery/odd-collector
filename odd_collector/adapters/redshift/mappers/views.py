from functools import partial

import pytz
from funcy import lmap
from odd_models import DataEntity, DataEntityType, DataSet
from odd_models.models import DataTransformer
from oddrn_generator import RedshiftGenerator

from odd_collector.adapters.redshift.mappers.columns import map_column
from odd_collector.adapters.redshift.mappers.metadata import MetadataTable


def map_view(generator: RedshiftGenerator, view: MetadataTable) -> DataEntity:
    generator.set_oddrn_paths(**{"schemas": view.schema_name, "views": view.table_name})
    map_view_column = partial(
        map_column,
        oddrn_generator=generator,
        owner=view.all.table_owner,
        parent_oddrn_path="views",
    )

    data_entity: DataEntity = DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.table_name,
        type=DataEntityType.VIEW,
        owner=view.all.table_owner,
        description=view.base.remarks,
        metadata=[],
        dataset=DataSet(field_list=lmap(map_view_column, view.columns)),
        data_transformer=DataTransformer(sql=view.all.view_ddl, inputs=[], outputs=[]),
    )

    if view.all.table_creation_time is not None:
        data_entity.updated_at = view.all.table_creation_time.replace(
            tzinfo=pytz.utc
        ).isoformat()
        data_entity.created_at = view.all.table_creation_time.replace(
            tzinfo=pytz.utc
        ).isoformat()

    if view.info is not None:
        if view.info.estimated_visible_rows is not None:
            data_entity.dataset.rows_number = int(view.info.estimated_visible_rows)
        else:
            if view.info.tbl_rows is not None:
                data_entity.dataset.rows_number = int(view.info.tbl_rows)

    return data_entity
