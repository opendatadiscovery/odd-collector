import asyncio
import functools
import tzlocal

from typing import List, Tuple
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from lark import logger
from odd_collector.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import Plugin
from odd_collector.module_importer import get_config
from odd_collector.module_importer import load_plugins_packages


from os import path
from datetime import datetime


async def get_entities(adapters: List[Tuple[Plugin, AbstractAdapter]]):
    for plugin, adapter in adapters:
        print(f"{plugin.type}")
        print(list(adapter.get_data_entities()))


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
        logger.error(e.message)
        asyncio.get_event_loop().stop()
