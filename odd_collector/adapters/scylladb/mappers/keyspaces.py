from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import ScyllaDBGenerator


def map_keyspace(
    generator: ScyllaDBGenerator, service_name: str, entities: List[str]
) -> DataEntity:
    """
    :param generator - ScyllaDBGenerator class
    :param service_name - service name
    :param entities - list of Table | View oddrn
    """
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("keyspaces"),
        name=service_name,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
