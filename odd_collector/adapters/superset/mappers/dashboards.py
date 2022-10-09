from ..domain.dataset import Dataset
from ..domain.dashboard import Dashboard
from typing import Dict
from oddrn_generator.generators import SupersetGenerator
from odd_models.models import DataEntity, DataEntityType, DataConsumer


def map_dashboard(
    oddrn_generator: SupersetGenerator, datasets_oddrns_dict: Dict[int, str], dashboard: Dashboard
) -> DataEntity:
    oddrn_generator.set_oddrn_paths(dashboards=dashboard.name)
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("dashboards", dashboard.name),
        name=dashboard.name,
        owner=dashboard.owner,
        description=dashboard.description,
        metadata=dashboard.metadata,
        type=DataEntityType.DASHBOARD,
        data_consumer=DataConsumer(
            inputs=[
                dataset_oddrn
                for dataset_id, dataset_oddrn in datasets_oddrns_dict.items()
                if dataset_id in dashboard.datasets_ids
            ]
        ),
    )
