from typing import Dict, List

from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    DataSet,
    MetadataExtension,
)
from oddrn_generator import Generator

from .columns import map_columns

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/couchbase.json"
)


def map_collection(
    oddrn_generator: Generator, collections: List[Dict], bucket: str
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    de_group = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("buckets"),
        name=bucket,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
    )

    for collection in collections:
        metadata: dict = collection["metadata"]
        name: str = metadata["name"]
        scope: str = metadata["scope"]

        oddrn_generator.set_oddrn_paths(
            **{"buckets": bucket, "scopes": scope, "collections": name}
        )
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("collections"),
            name=name,
            type=DataEntityType.TABLE,
            metadata=[
                MetadataExtension(
                    schema_url=f"{SCHEMA_FILE_URL}#/definitions/CouchbaseDataSetExtension",
                    metadata=metadata,
                ),
            ],
        )
        data_entity.dataset = DataSet(
            field_list=map_columns(collection["data"], oddrn_generator),
            parent_oddrn=de_group.oddrn,
        )
        data_entities.append(data_entity)

    de_group.data_entity_group = DataEntityGroup(
        entities_list=[de.oddrn for de in data_entities]
    )
    data_entities.append(de_group)

    return data_entities
