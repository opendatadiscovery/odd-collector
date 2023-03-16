from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType

from ..generator import FivetranGenerator


def map_database(
    generator: FivetranGenerator, database_name: str, entities: List[str]
) -> DataEntity:
    """
    :param entities - list of Tables
    :param generator - FivetranGenerator class
    :param database_name - db name
    """
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        name=database_name,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
