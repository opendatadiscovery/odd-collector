from oddrn_generator import DatabricksLakehouseGenerator
from typing import Dict, Any, List
from odd_models.models import DataEntityType, DataSet
from odd_models.models import DataEntity
from .column import map_column


def map_table(
        oddrn_generator: DatabricksLakehouseGenerator,
        table_node_name: str,
        columns_nodes: List[Dict[str, Any]],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table_node_name),
        name=table_node_name,
        type=DataEntityType.TABLE,
        metadata=[],
        dataset=DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("tables"),
            field_list=[
                map_column(oddrn_generator, column_node)
                for column_node in columns_nodes
            ],
        ),
    )
