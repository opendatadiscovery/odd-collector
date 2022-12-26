from typing import Dict, Set

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import MssqlGenerator


def map_schemas(
    value: Dict[str, Set[str]],
    generator: MssqlGenerator,
):
    for schema_name, values in value.items():
        oddrn = generator.get_oddrn_by_path("schemas", schema_name)

        yield DataEntity(
            type=DataEntityType.DATABASE_SERVICE,
            name=schema_name,
            oddrn=oddrn,
            data_entity_group=DataEntityGroup(entities_list=list(values)),
        )
