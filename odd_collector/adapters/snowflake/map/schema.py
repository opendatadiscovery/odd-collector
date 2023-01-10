from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Tuple

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType

from ..domain import Table
from ..generator import SnowflakeGenerator


def map_schemas(
    tables_with_entities: List[Tuple[Table, DataEntity]], generator: SnowflakeGenerator
) -> List[DataEntity]:
    generator = deepcopy(generator)

    grouped: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))

    for table, entity in tables_with_entities:
        grouped[table.table_catalog][table.table_schema].add(entity.oddrn)

    entities = []
    for catalog, schemas in grouped.items():
        for schema, oddrns in schemas.items():
            generator.set_oddrn_paths(databases=catalog, schemas=schema)
            oddrn = generator.get_oddrn_by_path("schemas")
            entity = DataEntity(
                type=DataEntityType.DATABASE_SERVICE,
                name=schema,
                oddrn=oddrn,
                data_entity_group=DataEntityGroup(entities_list=list(oddrns)),
            )
            entities.append(entity)

    return entities
