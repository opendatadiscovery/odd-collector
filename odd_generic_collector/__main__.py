import asyncio
import logging

from .domain.adapters_folder_meta import AdapterFolderMetadata
from .domain.collector import Collector

from os import path

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)

try:
    loop = asyncio.get_event_loop()

    cur_dirname = path.dirname(path.realpath(__file__))
    config_path = path.join(cur_dirname, "../collector_config.yaml")
    adapters_path = path.join(cur_dirname, "adapters")
    adapters_folder_metadata: AdapterFolderMetadata = AdapterFolderMetadata(
        adapters_path, "odd_generic_collector.adapters"
    )

    collector = Collector(config_path, adapters_folder_metadata)

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()

    asyncio.get_event_loop().run_forever()
except Exception as e:
    logging.error(e, exc_info=True)
    asyncio.get_event_loop().stop()
