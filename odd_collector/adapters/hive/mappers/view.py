from funcy import lflatten
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.mappers.column import VIEW_CONTEXT, map_column
from odd_collector.adapters.hive.mappers.metadata import map_metadata
from odd_collector.adapters.hive.models.view import View


def map_view(table: View, generator: HiveGenerator) -> DataEntity:
    generator.set_oddrn_paths(views=table.table_name)

    dataset_fields = lflatten(
        [map_column(column, generator, VIEW_CONTEXT) for column in table.columns]
    )
    data_transformer = DataTransformer(inputs=[], outputs=[])

    data_entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=table.table_name,
        type=DataEntityType.VIEW,
        metadata=map_metadata(table),
        dataset=DataSet(field_list=dataset_fields, rows_number=table.rows_number),
        data_transformer=data_transformer,
        created_at=table.create_time.isoformat() if table.create_time else None,
    )

    return data_entity
