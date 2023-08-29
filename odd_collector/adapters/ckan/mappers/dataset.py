from funcy import lpluck_attr
from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models import DataEntityGroup
from odd_models.models import DataEntity, DataEntityType
from oddrn_generator import CKANGenerator

from odd_collector.adapters.ckan.mappers.models import CKANDataset


def map_dataset(
    oddrn_generator: CKANGenerator,
    dataset: CKANDataset,
    resources_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("datasets", dataset.name),
        name=dataset.name,
        type=DataEntityType.DAG,
        metadata=[
            extract_metadata("ckan", dataset, DefinitionType.DATASET, flatten=True)
        ],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", resources_entities)
        ),
    )
