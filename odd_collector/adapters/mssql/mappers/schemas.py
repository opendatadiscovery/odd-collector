from typing import Iterable

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import MssqlGenerator


def map_schemas(
    value: dict[str, set[str]],
    generator: MssqlGenerator,
) -> Iterable[DataEntity]:
    for schema_name, values in value.items():
        oddrn = generator.get_oddrn_by_path("schemas", schema_name)

        yield DataEntity(
            type=DataEntityType.DATABASE_SERVICE,
            name=schema_name,
            owner=None,
            oddrn=oddrn,
            data_entity_group=DataEntityGroup(entities_list=list(values)),
        )
