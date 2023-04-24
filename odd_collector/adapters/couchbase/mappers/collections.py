from typing import List

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSet,
)
from oddrn_generator import CouchbaseGenerator

from .columns import map_columns
from ..models import Collection


def _map_collection(generator: CouchbaseGenerator, collection: Collection):
    generator.set_oddrn_paths(
        **{
            "buckets": collection.bucket,
            "scopes": collection.scope,
            "collections": collection.name,
        }
    )
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("collections"),
        name=collection.name,
        type=DataEntityType.TABLE,
        metadata=[extract_metadata("couchbase", collection, DefinitionType.DATASET)],
        dataset=DataSet(field_list=map_columns(collection.columns, generator)),
    )


def map_collections(
    generator: CouchbaseGenerator,
    collections: List[Collection],
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []

    for collection in collections:
        data_entity = _map_collection(generator, collection)
        data_entities.append(data_entity)
    return data_entities
