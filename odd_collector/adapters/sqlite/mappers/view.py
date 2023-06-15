from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import SQLiteGenerator

from odd_collector.domain.utils import extract_transformer_data
from .column import map_column
from ..domain import View


def map_view(generator: SQLiteGenerator, view: View) -> DataEntity:
    generator.set_oddrn_paths(views=view.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.name,
        type=DataEntityType.VIEW,
        dataset=DataSet(
            field_list=[
                map_column(generator, "views_columns", column)
                for column in view.columns
            ]
        ),
        data_transformer=extract_transformer_data(
            view.view_definition, generator, "tables"
        ),
        metadata=[extract_metadata("sqlite", view, DefinitionType.DATASET)],
    )
