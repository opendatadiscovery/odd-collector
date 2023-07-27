from odd_models.models import DataConsumer, DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator.generators import SupersetGenerator

from ..domain.dashboard import Dashboard


def map_dashboard(
    generator: SupersetGenerator,
    dashboard: Dashboard,
) -> DataEntity:
    name = dashboard.dashboard_title
    generator.set_oddrn_paths(dashboards=dashboard.id)
    oddrn = generator.get_oddrn_by_path("dashboards")

    return DataEntity(
        oddrn=oddrn,
        name=name,
        owner=None,
        description=None,
        metadata=[],
        type=DataEntityType.DASHBOARD,
        data_entity_group=DataEntityGroup(entities_list=[]),
        data_consumer=DataConsumer(inputs=[]),
    )


def add_to_group(group_entity: DataEntity, entity: DataEntity) -> None:
    entity_list = group_entity.data_entity_group.entities_list

    if entity.oddrn not in entity_list:
        entity_list.append(entity.oddrn)
