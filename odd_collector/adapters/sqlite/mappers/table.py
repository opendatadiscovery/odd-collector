from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import SQLiteGenerator

from .column import map_column
from ..domain import Table


def map_table(generator: SQLiteGenerator, table: Table) -> DataEntity:
    generator.set_oddrn_paths(tables=table.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.name,
        type=DataEntityType.TABLE,
        dataset=DataSet(
            field_list=[
                map_column(generator, "tables_columns", column)
                for column in table.columns
            ]
        ),
        metadata=[extract_metadata("sqlite", table, DefinitionType.DATASET)],
    )
