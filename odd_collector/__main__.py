import traceback
from pathlib import Path

from odd_collector_sdk.collector import Collector
from odd_collector_sdk.logger import logger

from odd_collector.domain.plugin import PLUGIN_FACTORY

from .version import print_version

if __name__ == "__main__":
    print_version()

    try:
        collector = Collector(
            config_path=Path().cwd() / "collector_config.yaml",
            root_package="odd_collector.adapters",
            plugin_factory=PLUGIN_FACTORY,
        )
        collector.run()
    except Exception as e:
        logger.debug(traceback.format_exc())
        logger.error(e)
