from ..domain.dataset import Dataset
from ..domain.dashboard import Dashboard
from typing import List
from odd_collector.adapters.superset.plugin.plugin import SupersetGenerator
from odd_models.models import DataEntity, DataEntityType, DataConsumer


def map_dashboard(
    oddrn_generator: SupersetGenerator, datasets: List[Dataset], dashboard: Dashboard
) -> DataEntity:
    oddrn_generator.set_oddrn_paths(dashboards=dashboard.name)
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("dashboards", dashboard.name),
        name=dashboard.name,
        type=DataEntityType.DASHBOARD,
        data_consumer=DataConsumer(
            inputs=[
                dataset.get_oddrn(oddrn_generator)
                for dataset in datasets
                if dataset.id in dashboard.datasets_ids
            ]
        ),
    )
