from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup
from typing import Dict, List
from .columns import map_columns

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/mongodb.json"
)


def map_collection(
    oddrn_generator, collections: List[Dict], database: str
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    de_group = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("databases"),
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
    )

    for metadata in collections:
        data_entity_type = DataEntityType.TABLE

        collection_name: str = metadata["title"]

        oddrn_generator.set_oddrn_paths(
            **{"databases": database, "collections": collection_name}
        )

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("collections"),
            name=collection_name,
            type=data_entity_type,
            updated_at=metadata["modification_date"],
            created_at=metadata["creation_date"],
            metadata=[
                {
                    "schema_url": f"{SCHEMA_FILE_URL}#/definitions/MongodbDataSetExtension",
                    "metadata": metadata["metadata"],
                }
            ],
        )
        data_entity.dataset = DataSet(
            field_list=[],
            parent_oddrn=de_group.oddrn,
            rows_number=metadata["row_number"],
        )
        data_entity.dataset.field_list = map_columns(metadata["data"], oddrn_generator)
        data_entities.append(data_entity)

    de_group.data_entity_group = DataEntityGroup(
        entities_list=[de.oddrn for de in data_entities]
    )

    data_entities.append(de_group)

    return data_entities
