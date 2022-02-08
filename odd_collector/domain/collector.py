import tzlocal

from aiohttp import ClientSession
from datetime import datetime
from typing import List
from apscheduler.schedulers.asyncio import AsyncIOScheduler


from .adapter import AbstractAdapter
from .adapters_folder_meta import AdapterFolderMetadata
from .collector_config_loader import CollectorConfigLoader
from .collector_config import CollectorConfig
from .adapters_initializer import AdaptersInitializer


class Collector:
    def __init__(
        self,
        config_path: str,
        adapters_folder_meta: AdapterFolderMetadata,
        platform_url: str,
    ) -> None:
        load_config = CollectorConfigLoader()
        self.config: CollectorConfig = load_config(config_path)

        init_adapters = AdaptersInitializer(adapters_folder_meta, self.config.plugins)
        self.adapters: List[AbstractAdapter] = init_adapters()
        self.platform_url = platform_url

    def start_polling(self):
        scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))
        scheduler.add_job(
            self._ingest_data,
            "interval",
            minutes=self.config.default_pulling_interval,
            next_run_time=datetime.now(),
        )
        scheduler.start()


    async def _ingest_data(self):
        headers = {"content-type": "application/json"}
        async with ClientSession() as session:
            for adapter in self.adapters:
                print("send request")
                async with session.post(
                    url=f"{self.platform_url}/ingestion/entities",
                    data=adapter.get_data_entity_list().json(),
                    headers=headers,
                ) as response:
                    print(response)
