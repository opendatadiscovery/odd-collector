from ..domain.dashboard import Dashboard
from typing import Dict
from oddrn_generator.generators import RedashGenerator
from odd_models.models import DataEntity, DataEntityType, DataConsumer


def map_dashboard(
    oddrn_generator: RedashGenerator,
    views_entities_dict: Dict[int, DataEntity],
    dashboard: Dashboard,
) -> DataEntity:
    oddrn_generator.set_oddrn_paths(dashboards=dashboard.name)
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("dashboards", dashboard.name),
        name=dashboard.name,
        type=DataEntityType.DASHBOARD,
        metadata=dashboard.metadata,
        data_consumer=DataConsumer(
            inputs=[
                view_entity.oddrn
                for view_id, view_entity in views_entities_dict.items()
                if view_id in dashboard.queries_ids
            ]
        ),
    )
