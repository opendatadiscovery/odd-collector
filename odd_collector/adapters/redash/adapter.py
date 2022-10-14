from typing import Dict, Type
from odd_models.models import DataEntity
from odd_models.models import DataEntityList
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import RedashPlugin
from .client import RedashClient
from oddrn_generator import RedashGenerator
from .domain.datasource import DataSource
from .mappers.datasources import ds_types_factory
from .mappers.queries import map_query


class Adapter(AbstractAdapter):
    def __init__(
            self, config: RedashPlugin, client: Type[RedashClient] = None
    ) -> None:
        client = client or RedashClient
        self.client = client(config)

        self._oddrn_generator = RedashGenerator(
            host_settings=self.client.get_server_host()
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        datasources = await self.client.get_data_sources()
        datasources_ids_dict: Dict[str, DataSource] = {datasource.id: datasource for datasource in datasources}
        queries = await self.client.get_queries()
        views_entities_dict: Dict[int, DataEntity] = {}
        for query in queries:
            datasource_id = query.data_source_id
            datasource = datasources_ids_dict.get(datasource_id)
            ds_type_cls = ds_types_factory.get(datasource.type)
            ds_type = ds_type_cls(datasource)

            view_entity = map_query(
                self._oddrn_generator, query, external_ds_type=ds_type
            )
            views_entities_dict.update({query.id: view_entity})

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*list(views_entities_dict.values())],
        )
