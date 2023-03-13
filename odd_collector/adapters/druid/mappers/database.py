from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType

from odd_collector.adapters.druid.generator import DruidGenerator


def to_data_entity_group(
    generator: DruidGenerator, service_name: str, entities: List[str]
) -> DataEntity:
    """
    :param service_name:
    :param generator:
    :param entities - list of Table | View oddrn
    """
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("catalogs"),
        name=service_name,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
