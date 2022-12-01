from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import Generator

from ..domain import Collection
from .metadata import get_metadata


def map_collection(collection: Collection, generator: Generator) -> DataEntity:
    generator.set_oddrn_paths(collections=collection.id)

    cards_oddrn = [
        generator.get_oddrn_by_path("cards", id) for id in collection.cards_id
    ]
    dashboards_oddrn = [
        generator.get_oddrn_by_path("dashboards", id) for id in collection.dashboards_id
    ]

    return DataEntity(
        name=collection.name,
        oddrn=generator.get_oddrn_by_path("collections", collection.id),
        description=collection.description,
        metadata=get_metadata(collection),
        tags=None,
        type=DataEntityType.FILE,
        data_entity_group=DataEntityGroup(
            entities_list=[*cards_oddrn, *dashboards_oddrn]
        ),
    )
