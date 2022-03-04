from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup
from typing import Dict, List
from .columns import map_column



def map_collection(oddrn_generator, collections: List[Dict], database: str) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    column_index: int = 0

    for collection in collections:
        metadata: Dict = collection

        data_entity_type = DataEntityType.TABLE

        collection_name: str = metadata['title']

        oddrn_generator.set_oddrn_paths(**{'databases': database, "collections" : collection_name})

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("collections"),
            name=collection_name,
            type=data_entity_type,
            metadata=[],
        )
        data_entities.append(data_entity)

        data_entity.dataset = DataSet(
            field_list=[]
        )

        for key, value in collection['properties'].items():
            data_entity.dataset.field_list.append(map_column({**value, 'title': key}, oddrn_generator, "collections"))


    data_entities.append(DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("databases"),
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=[de.oddrn for de in data_entities]
        ),
    ))

    return data_entities
