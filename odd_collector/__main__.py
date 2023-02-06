import asyncio
import signal

from odd_collector_sdk.collector import Collector
from odd_collector_sdk.logger import logger
from odd_collector_sdk.shutdown import shutdown

from odd_collector.domain.plugin import PLUGIN_FACTORY
from pathlib import Path


try:
    loop = asyncio.get_event_loop()

    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    cur_dirname = Path(__file__).parent
    config_path = cur_dirname / "../collector_config.yaml"
    adapters_path = cur_dirname / "adapters"

    collector = Collector(
        config_path=str(config_path),
        root_package="odd_collector.adapters",
        plugin_factory=PLUGIN_FACTORY,
    )

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()

    asyncio.get_event_loop().run_forever()
except Exception as e:
    logger.exception(e)
    asyncio.get_event_loop().stop()
