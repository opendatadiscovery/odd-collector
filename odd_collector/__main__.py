import asyncio
import logging
from os import path

from odd_collector_sdk.collector import Collector

from .plugin import AvailablePlugin

logging.basicConfig(
    level=logging.DEBUG, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)


try:
    loop = asyncio.get_event_loop()

    cur_dirname = path.dirname(path.realpath(__file__))
    config_path = path.join(cur_dirname, "../collector_config.yaml")
    adapters_path = path.join(cur_dirname, "adapters")

    collector = Collector(config_path=config_path,
                          root_package="odd_collector.adapters",
                          plugins_union_type=AvailablePlugin)

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()

    asyncio.get_event_loop().run_forever()

except Exception as e:
    logging.error(e, exc_info=True)
    asyncio.get_event_loop().stop()
