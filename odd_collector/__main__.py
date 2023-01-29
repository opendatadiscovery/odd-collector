import asyncio
import logging
import os

from odd_collector_sdk.collector import Collector
from odd_collector.domain.plugin import PLUGIN_FACTORY

logging.basicConfig(
    level=os.getenv("LOGLEVEL", "INFO"),
    format="[%(asctime)s] %(levelname)s in %(name)s: %(message)s",
)
logger = logging.getLogger("odd-collector")

try:
    loop = asyncio.get_event_loop()

    cur_dirname = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(cur_dirname, "../collector_config.yaml")
    adapters_path = os.path.join(cur_dirname, "adapters")

    collector = Collector(
        config_path=config_path,
        root_package="odd_collector.adapters",
        plugin_factory=PLUGIN_FACTORY,
    )

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()

    asyncio.get_event_loop().run_forever()
except Exception as e:
    logger.error(e, exc_info=True)
    asyncio.get_event_loop().stop()
