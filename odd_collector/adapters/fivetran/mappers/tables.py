from typing import List

from odd_models.models import DataEntity, DataEntityType, DataSet

from .columns import map_column
from .metadata import map_metadata
from ..domain.column import ColumnMetadata
from ..domain.table import TableMetadata
from oddrn_generator import FivetranGenerator


def map_tables(
    generator: FivetranGenerator,
    tables: List[TableMetadata],
    columns: List[ColumnMetadata],
) -> List[DataEntity]:
    data_entities: List = []
    for table in tables:
        generator.set_oddrn_paths(tables=table.name_in_source)
        data_entities.append(
            DataEntity(
                oddrn=generator.get_oddrn_by_path("tables"),
                name=table.name_in_source,
                type=DataEntityType.TABLE,
                metadata=[map_metadata(table)],
                dataset=DataSet(
                    field_list=[
                        map_column(generator, "columns", column)
                        for column in columns
                        if table.id == column.parent_id
                    ]
                ),
            )
        )

    return data_entities
