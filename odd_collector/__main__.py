import traceback
from pathlib import Path

from odd_collector_sdk.collector import Collector
from odd_collector_sdk.logger import logger

from odd_collector.domain.plugin import PLUGIN_FACTORY

COLLECTOR_PACKAGE = __package__
CONFIG_PATH = Path().cwd() / "collector_config.yaml"

try:
    collector = Collector(
        config_path=CONFIG_PATH,
        root_package=COLLECTOR_PACKAGE,
        plugin_factory=PLUGIN_FACTORY,
    )
    collector.run()
except Exception as e:
    logger.debug(traceback.format_exc())
    logger.error(e)
