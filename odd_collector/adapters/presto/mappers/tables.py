from typing import Dict, List, Any
from oddrn_generator.generators import PrestoGenerator
from .columns import map_column
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSet,
)


def map_table(oddrn_generator: PrestoGenerator, table_node_name: str, columns_nodes: List[Dict[str, Any]]) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table_node_name),
        name=table_node_name,
        type=DataEntityType.TABLE,
        metadata=[],
        dataset=DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("tables"),
            field_list=[map_column(oddrn_generator, column_node)
                        for column_node in columns_nodes],
        )
    )
