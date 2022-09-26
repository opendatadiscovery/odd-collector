from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import Generator

from ..domain import Dashboard


def map_dashboard(
    dashboard: Dashboard, generator: Generator, entities: List[str]
) -> DataEntity:
    generator.set_oddrn_paths(
        collections=dashboard.collection_id or "root",
        dashboards=dashboard.id,
    )

    return DataEntity(
        name=dashboard.name,
        oddrn=generator.get_oddrn_by_path("dashboards"),
        description=dashboard.description,
        owner=dashboard.get_owner(),
        metadata=None,
        tags=None,
        updated_at=dashboard.updated_at,
        created_at=dashboard.created_at,
        type=DataEntityType.DASHBOARD,
        data_entity_group=DataEntityGroup(
            entities_list=entities or [],
            group_oddrn=generator.get_oddrn_by_path("collections"),
        ),
    )
