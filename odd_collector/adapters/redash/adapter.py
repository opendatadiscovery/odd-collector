from typing import Dict, List, Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedashGenerator
from oddrn_generator.utils.external_generators import ExternalGeneratorMappingError

from odd_collector.domain.plugin import RedashPlugin

from .client import RedashClient
from .domain.datasource import DataSource
from .domain.query import Query
from .mappers.dashboards import map_dashboard
from .mappers.datasources import ds_types_factory
from .mappers.queries import map_query


class Adapter(AbstractAdapter):
    def __init__(self, config: RedashPlugin, client: Type[RedashClient] = None) -> None:
        client = client or RedashClient
        self.client = client(config)

        self._oddrn_generator = RedashGenerator(
            host_settings=self.client.get_server_host()
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def __get_views_entities_dict(
        self, datasources: List[DataSource], queries: List[Query]
    ) -> Dict[int, DataEntity]:
        datasources_ids_dict: Dict[str, DataSource] = {
            datasource.id: datasource for datasource in datasources
        }
        views_entities_dict: Dict[int, DataEntity] = {}
        for query in queries:
            datasource_id = query.data_source_id
            datasource = datasources_ids_dict.get(datasource_id)
            datasource_type_name = datasource.type
            ds_type_cls = ds_types_factory.get(datasource_type_name)
            if ds_type_cls is None:
                raise ExternalGeneratorMappingError(datasource_type_name)
            ds_type = ds_type_cls(datasource).get_external_generator()

            view_entity = map_query(
                self._oddrn_generator, query, external_ds_type=ds_type
            )
            views_entities_dict.update({query.id: view_entity})
        return views_entities_dict

    async def get_data_entity_list(self) -> DataEntityList:
        dashboards = await self.client.get_dashboards()
        datasources = await self.client.get_data_sources()
        queries = await self.client.get_queries()
        views_entities_dict = self.__get_views_entities_dict(datasources, queries)
        dashboards_entities = [
            map_dashboard(self._oddrn_generator, views_entities_dict, dashboard)
            for dashboard in dashboards
        ]

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*list(views_entities_dict.values()), *dashboards_entities],
        )
