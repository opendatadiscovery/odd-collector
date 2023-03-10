from typing import List

from odd_models.models import DataEntity, DataEntityType, DataEntityGroup

from odd_collector.adapters.druid.generator import DruidGenerator


def map_database(generator: DruidGenerator, service_name: str, entities: List[str]) -> DataEntity:
    """
    :param entities - list of Table | View oddrn
    """
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("catalogs"),
        name=service_name,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
