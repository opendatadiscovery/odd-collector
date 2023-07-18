from pathlib import Path

import odd_collector_sdk as sdk
from odd_collector_sdk.collector import Collector

from odd_collector import get_version
from odd_collector.domain.plugin import PLUGIN_FACTORY
from odd_collector.logger import logger

COLLECTOR_PACKAGE = __package__
CONFIG_PATH = Path().cwd() / "collector_config.yaml"


logger.info(f"Collector version: {get_version()}")
logger.info(f"SDK: {sdk.get_version()}")


if __name__ == "__main__":
    collector = Collector(
        config_path=CONFIG_PATH,
        root_package=COLLECTOR_PACKAGE,
        plugin_factory=PLUGIN_FACTORY,
    )
    collector.run()
