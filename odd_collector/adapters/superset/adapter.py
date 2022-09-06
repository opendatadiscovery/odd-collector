from typing import Dict, Type
from .client import SupersetClient
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import SupersetGenerator
from .domain.dataset import Dataset
from odd_collector.domain.plugin import SupersetPlugin
from .mappers.datasets import map_table
from .mappers.databases import map_database
from .mappers.dashboards import map_dashboard


class Adapter(AbstractAdapter):
    def __init__(
        self, config: SupersetPlugin, client: Type[SupersetClient] = None
    ) -> None:
        client = client or SupersetClient
        self.client = client(config)

        self.__oddrn_generator = SupersetGenerator(
            host_settings=self.client.get_server_host()
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def _get_datasets(self) -> Dict[str, Dataset]:
        dsets = await self.client.get_datasets()
        datasets_by_id: Dict[str, Dataset] = {dataset.id: dataset for dataset in dsets}
        dsets_ids = [dset.id for dset in dsets]
        datasets_columns = await self.client.get_datasets_columns(dsets_ids)

        for dataset_id, columns in datasets_columns.items():
            datasets_by_id[dataset_id].columns = columns

        return datasets_by_id

    async def get_data_entity_list(self) -> DataEntityList:
        dashboards = await self.client.get_dashboards()
        datasets = await self._get_datasets()

        datasets_data_entities_by_id: Dict[str, DataEntity] = {
            dataset_id: map_table(self.__oddrn_generator, dataset)
            for dataset_id, dataset in datasets.items()
        }
        datasets_values = list(datasets.values())

        datasets_data_entities = datasets_data_entities_by_id.values()
        databases_ids_names: Dict[int, str] = {
            dataset.database_id: dataset.database_name for dataset in datasets_values
        }

        databases_entities = [
            map_database(
                self.__oddrn_generator,
                datasets_values,
                database_id,
                database_name,
            )
            for database_id, database_name in databases_ids_names.items()
        ]

        dashboards_entities = [
            map_dashboard(self.__oddrn_generator, datasets_values, dashboard)
            for dashboard in dashboards
        ]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*datasets_data_entities, *databases_entities, *dashboards_entities],
        )
