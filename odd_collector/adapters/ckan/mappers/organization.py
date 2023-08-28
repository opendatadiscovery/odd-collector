from funcy import lpluck_attr
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import CKANGenerator


def map_organization(
    oddrn_generator: CKANGenerator,
    organization_name: str,
    datasets_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("organizations", organization_name),
        name=organization_name,
        type=DataEntityType.DAG,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", datasets_entities)
        ),
        owner=None,
    )
