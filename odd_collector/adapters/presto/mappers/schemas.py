from typing import Dict

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator.generators import PrestoGenerator


def map_schema(
    oddrn_generator: PrestoGenerator,
    schema_node_name: str,
    tables_node: Dict[str, dict],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("schemas", schema_node_name),
        name=schema_node_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=[
                oddrn_generator.get_oddrn_by_path("tables", table_node_name)
                for table_node_name in tables_node.keys()
            ]
        ),
    )
