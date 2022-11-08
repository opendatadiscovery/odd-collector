from typing import Dict, List, Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import SupersetGenerator
from oddrn_generator.utils.external_generators import (
    ExternalGeneratorMappingError,
    ExternalSnowflakeGenerator,
)

from odd_collector.domain.plugin import SupersetPlugin

from .client import SupersetClient
from .domain.database import Database
from .domain.dataset import Dataset
from .mappers.backends import backends_factory
from .mappers.dashboards import map_dashboard
from .mappers.datasets import map_table


class Adapter(AbstractAdapter):
    def __init__(
        self, config: SupersetPlugin, client: Type[SupersetClient] = None
    ) -> None:
        client = client or SupersetClient
        self.client = client(config)

        self._oddrn_generator = SupersetGenerator(
            host_settings=self.client.get_server_host()
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    async def _get_datasets(self) -> List[Dataset]:
        dsets = await self.client.get_datasets()
        datasets_by_id: Dict[str, Dataset] = {dataset.id: dataset for dataset in dsets}
        dsets_ids = [dset.id for dset in dsets]
        datasets_columns = await self.client.get_datasets_columns(dsets_ids)

        for dataset_id, columns in datasets_columns.items():
            datasets_by_id[dataset_id].columns = columns

        return list(datasets_by_id.values())

    async def _get_databases_dict(self) -> Dict[str, Database]:
        databases = await self.client.get_databases()
        return {database.id: database for database in databases}

    def _split_views_and_tables(
        self, datasets: List[Dataset], databases: Dict[str, Database]
    ) -> (Dict[int, DataEntity], Dict[int, str]):
        views_entities_dict: Dict[int, DataEntity] = {}
        datasets_oddrns_dict: Dict[int, str] = {}
        for dataset in datasets:
            database_id = dataset.database_id
            database = databases.get(database_id)
            backend_name = database.backend
            backend_cls = backends_factory.get(backend_name)
            if backend_cls is None:
                raise ExternalGeneratorMappingError(backend_name)
            backend = backend_cls(database).get_external_generator()
            if isinstance(backend, ExternalSnowflakeGenerator):
                dataset.name = dataset.name.upper()
                dataset.schema = dataset.schema.upper()
            if dataset.kind == "virtual":
                view_entity = map_table(
                    self._oddrn_generator, dataset, external_backend=backend
                )
                views_entities_dict.update({dataset.id: view_entity})
            else:
                gen = backend.get_generator_for_schema_lvl(dataset.schema)
                gen.get_oddrn_by_path(backend.table_path_name, dataset.name)
                oddrn = gen.get_oddrn_by_path(backend.table_path_name)
                datasets_oddrns_dict.update({dataset.id: oddrn})
        return views_entities_dict, datasets_oddrns_dict

    async def get_data_entity_list(self) -> DataEntityList:

        datasets = await self._get_datasets()
        databases_dict = await self._get_databases_dict()
        dashboards = await self.client.get_dashboards()

        views_entities_dict, datasets_oddrns_dict = self._split_views_and_tables(
            datasets, databases_dict
        )
        for dataset_id, dataset in views_entities_dict.items():
            datasets_oddrns_dict.update({dataset_id: dataset.oddrn})

        dashboards_entities = [
            map_dashboard(self._oddrn_generator, datasets_oddrns_dict, dashboard)
            for dashboard in dashboards
        ]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*list(views_entities_dict.values()), *dashboards_entities],
        )
