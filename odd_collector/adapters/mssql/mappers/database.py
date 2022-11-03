from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import MssqlGenerator


def map_database(
    name: str,
    group_oddrns: List[str],
    oddrn_generator: MssqlGenerator,
) -> DataEntity:
    return DataEntity(
        type=DataEntityType.DATABASE_SERVICE,
        name=name,
        oddrn=oddrn_generator.get_oddrn_by_path("databases", name),
        data_entity_group=DataEntityGroup(entities_list=group_oddrns),
    )
