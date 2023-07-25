# from pathlib import Path

# from odd_collector_sdk.collector import Collector

# from odd_collector.domain.plugin import PLUGIN_FACTORY

# COLLECTOR_PACKAGE = __package__
# CONFIG_PATH = Path().cwd() / "collector_config.yaml"

# if __name__ == "__main__":
#     collector = Collector(
#         config_path=CONFIG_PATH,
#         root_package=COLLECTOR_PACKAGE,
#         plugin_factory=PLUGIN_FACTORY,
#     )
#     collector.run()


from odd_collector.adapters.superset.adapter import Adapter
from odd_collector.domain.plugin import SupersetPlugin

config = SupersetPlugin(
    name="superset_adapter",
    type="superset",
    server="http://localhost:8088",
    username="admin",
    password="admin",
)

adapter = Adapter(config)


async def main():
    await adapter.get_data_entity_list()


import asyncio

asyncio.run(main())
