import asyncio
import functools
import logging
import tzlocal

from typing import List, Tuple
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from odd_collector.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import Plugin
from odd_collector.module_importer import get_config
from odd_collector.module_importer import load_plugins_packages
import aiohttp
from .config import config

from os import path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)


async def get_entities(adapters: List[Tuple[Plugin, AbstractAdapter]]):
    for plugin, adapter in adapters:
        res = adapter.get_data_entity_list()
        print(res)

    # async with aiohttp.ClientSession() as session:
    #     for plugin, adapter in adapters:
    #         await session.post(
    #             config.platform_host_url, adapter.get_data_entity_list().json()
    #         )


if __name__ == "__main__":
    try:
        cur_dirname = path.dirname(path.realpath(__file__))
        collector_config = get_config(
            path.join(cur_dirname, "../collector_config.yaml")
        )

        loaded_packages = load_plugins_packages(collector_config)
        adapters = [
            (plugin_config, package.adapter.Adapter(plugin_config))
            for package, plugin_config in loaded_packages
        ]

        scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))
        scheduler.add_job(
            functools.partial(get_entities, adapters),
            "interval",
            minutes=collector_config.default_pulling_interval,
            next_run_time=datetime.now(),
        )
        scheduler.start()

        asyncio.get_event_loop().run_forever()
    except Exception as e:
        logging.error(e.message)
        asyncio.get_event_loop().stop()
