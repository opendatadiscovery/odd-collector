from oddrn_generator import DatabricksLakehouseGenerator
from odd_models.models import DataEntityGroup, DataEntityType
from odd_models.models import DataEntity
from typing import Dict


def map_database(
        oddrn_generator: DatabricksLakehouseGenerator,
        database_node_name: str,
        tables_node: Dict[str, dict],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("databases", database_node_name),
        name=database_node_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=[
                oddrn_generator.get_oddrn_by_path("tables", table_node_name)
                for table_node_name in tables_node.keys()
            ]
        ),
    )
