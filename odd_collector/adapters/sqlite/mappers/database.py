from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import SQLiteGenerator


def map_database(
    generator: SQLiteGenerator, data_source: str, entities: List[str]
) -> DataEntity:
    """
    :param entities - list of Table | View oddrn
    :param generator - SQLiteGenerator
    :param data_source - name of data source
    """
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        name=data_source,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
