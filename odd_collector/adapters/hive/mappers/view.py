from funcy import lflatten
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.mappers.column import VIEW_CONTEXT, map_column
from odd_collector.adapters.hive.models.view import View


def map_view(view: View, generator: HiveGenerator) -> DataEntity:
    generator.set_oddrn_paths(views=view.name)

    dataset_fields = lflatten(
        [map_column(column, generator, VIEW_CONTEXT) for column in view.columns]
    )

    data_entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.name,
        type=DataEntityType.VIEW,
        metadata=[extract_metadata("hive", view, DefinitionType.DATASET)],
        dataset=DataSet(field_list=dataset_fields, rows_number=view.rows_number),
        data_transformer=DataTransformer(inputs=[], outputs=[]),
    )

    return data_entity
