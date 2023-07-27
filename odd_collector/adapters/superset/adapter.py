from typing import Type
from urllib.parse import urlparse

from funcy import partial, walk_values
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import Generator
from oddrn_generator.generators import SupersetGenerator

from odd_collector.domain.plugin import SupersetPlugin

from .client import SupersetClient
from .domain.dataset import create_dataset_oddrn
from .mappers.chart import map_chart
from .mappers.dashboard import add_to_group, map_dashboard


class Adapter(BaseAdapter):
    config: SupersetPlugin
    generator: SupersetGenerator

    def __init__(
        self, config: SupersetPlugin, client: Type[SupersetClient] = None
    ) -> None:
        client = client or SupersetClient
        self.client = client(config)
        super().__init__(config)

    def create_generator(self) -> Generator:
        host = urlparse(self.config.server).netloc
        return SupersetGenerator(host_settings=host)

    async def get_data_entity_list(self) -> DataEntityList:
        async with self.client.connect() as client:
            datasets = await client.fetch_datasets()
            dashboards = await client.fetch_dashboards()
            charts = await client.fetch_charts()

            dashboards_entities = walk_values(
                partial(map_dashboard, self.generator), dashboards
            )
            charts_entities = walk_values(partial(map_chart, self.generator), charts)

            for chart_id, chart in charts.items():
                chart_entity = charts_entities[chart_id]

                # Add chart to dashboards group
                for dashboard_id in chart.dashboard_ids:
                    if dashboard_entity := dashboards_entities.get(dashboard_id):
                        add_to_group(dashboard_entity, chart_entity)

                # Generate dataset datasets oddrn and add it to chart inputs
                dataset = datasets[chart.dataset_id]
                if dataset_oddrn := create_dataset_oddrn(dataset):
                    chart_entity.data_consumer.inputs.append(dataset_oddrn)

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[*charts_entities.values(), *dashboards_entities.values()],
            )
