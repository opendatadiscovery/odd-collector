from copy import deepcopy
from typing import List

from funcy import lpluck_attr
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType

from ..generator import SnowflakeGenerator


def map_database(
    database_name: str,
    schemas_entities: List[DataEntity],
    generator: SnowflakeGenerator,
) -> DataEntity:
    generator = deepcopy(generator)

    oddrn = generator.get_oddrn_by_path("databases", database_name)
    return DataEntity(
        type=DataEntityType.DATABASE_SERVICE,
        name=database_name,
        oddrn=oddrn,
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", schemas_entities)
        ),
    )
