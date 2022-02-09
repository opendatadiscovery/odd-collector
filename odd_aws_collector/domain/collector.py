import tzlocal

from aiohttp import ClientSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
from typing import List
from odd_aws_collector.domain.register_datasource_request import (
    RegisterDataSourceRequest,
    RegisterDataSourceRequests,
)
from odd_aws_collector.services.datasource_api import DataSourceApi

from odd_aws_collector.services.http_client import HttpClient

from .adapters_folder_meta import AdapterFolderMetadata
from .adapters_initializer import AdaptersInitializer
from .collector_config_loader import CollectorConfigLoader
from .collector_config import CollectorConfig


class Collector:
    def __init__(
        self,
        config_path: str,
        adapters_folder_meta: AdapterFolderMetadata,
    ) -> None:
        load_config = CollectorConfigLoader()
        self.config: CollectorConfig = load_config(config_path)

        adapter_initizlizator = AdaptersInitializer(
            adapters_folder_meta, self.config.plugins
        )
        self.adapters_with_plugins = adapter_initizlizator.init_adapters()
        self.__api = DataSourceApi(
            http_client=HttpClient(), platform_url=self.config.platform_host_url
        )

    def start_polling(self):
        scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))
        scheduler.add_job(
            self.__ingest_data,
            "interval",
            minutes=self.config.default_pulling_interval,
            next_run_time=datetime.now(),
        )
        scheduler.start()

    async def register_data_sources(self):
        requests: List[RegisterDataSourceRequest] = [
            RegisterDataSourceRequest(
                name=plugin.name,
                oddrn=adapter.get_data_source_oddrn(),
                description=plugin.description,
                namespace=plugin.namespace,
            )
            for adapter, plugin in self.adapters_with_plugins
        ]

        requests_model = RegisterDataSourceRequests(__root__=requests)

        async with ClientSession() as session:
            resp = await self.__api.register_datasource(requests_model, session)

            return resp

    async def __ingest_data(self):
        async with ClientSession() as session:
            for adapter, _ in self.adapters_with_plugins:
                await self.__api.ingest_data(adapter.get_data_entity_list(), session)
