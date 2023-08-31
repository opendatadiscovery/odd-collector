from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import CKANGenerator
from odd_collector.adapters.ckan.mappers.models import Group


def map_group(
    oddrn_generator: CKANGenerator,
    group: Group,
) -> DataEntity:
    datasets_oddrns: list[str] = []
    for dataset in group.datasets:
        dataset_name = dataset["name"]
        organization_name = dataset["organization"]["name"]
        oddrn_generator.set_oddrn_paths(organizations=organization_name)
        datasets_oddrns.append(
            oddrn_generator.get_oddrn_by_path("datasets", dataset_name)
        )
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("groups", group.name),
        name=group.name,
        type=DataEntityType.DAG,
        metadata=[
            extract_metadata("ckan", group, DefinitionType.DATASET, flatten=True)
        ],
        data_entity_group=DataEntityGroup(entities_list=datasets_oddrns),
        owner=None,
    )
