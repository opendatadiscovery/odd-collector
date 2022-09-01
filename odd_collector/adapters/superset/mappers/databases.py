from ..domain.dataset import Dataset
from typing import List, Dict
from odd_collector.adapters.superset.plugin.plugin import SupersetGenerator
from odd_models.models import DataEntity, DataEntityType, DataEntityGroup


def create_databases_entities(
    oddrn_generator: SupersetGenerator, datasets: List[Dataset]
) -> List[DataEntity]:
    databases_ids_names: Dict[int, str] = {
        dataset.database_id: dataset.database_name for dataset in datasets
    }
    databases_entities: List[DataEntity] = []
    for database_id, database_name in databases_ids_names.items():
        databases_entities.append(
            DataEntity(
                oddrn=oddrn_generator.get_oddrn_by_path("databases", database_name),
                name=database_name,
                type=DataEntityType.DATABASE_SERVICE,
                metadata=[],
                data_entity_group=DataEntityGroup(
                    entities_list=[
                        dataset.get_oddrn(oddrn_generator)
                        for dataset in datasets
                        if dataset.database_id == database_id
                    ]
                ),
            )
        )
    return databases_entities
