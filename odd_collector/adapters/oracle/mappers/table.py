from odd_models.models import DataEntity, DataEntityType, DataSet

from ..domain import Table
from ..generator import OracleGenerator
from .column import map_column


def map_table(generator: OracleGenerator, table: Table) -> DataEntity:
    generator.set_oddrn_paths(tables=table.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.name,
        type=DataEntityType.TABLE,
        description=table.description,
        dataset=DataSet(
            field_list=[
                map_column(generator, "tables_columns", column)
                for column in table.columns
            ]
        ),
    )
