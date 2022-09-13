from typing import Dict
from oddrn_generator.generators import PrestoGenerator
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataEntityGroup
)


def map_catalog(oddrn_generator: PrestoGenerator, catalog_node_name: str, schemas_node: Dict[str, dict]) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("catalogs", catalog_node_name),
        name=catalog_node_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=[oddrn_generator.get_oddrn_by_path("schemas", schema_node_name) for
                           schema_node_name in
                           schemas_node.keys()
                           ]
        ),
    )
