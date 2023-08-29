from funcy import lpluck_attr
from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import CKANGenerator
from odd_collector.adapters.ckan.mappers.models import Organization


def map_organization(
    oddrn_generator: CKANGenerator,
    organization: Organization,
    datasets_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("organizations", organization.name),
        name=organization.name,
        type=DataEntityType.DAG,
        metadata=[
            extract_metadata("ckan", organization, DefinitionType.DATASET, flatten=True)
        ],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", datasets_entities)
        ),
        owner=None,
    )
