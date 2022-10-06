"""
Special adapter which retrieves information from other adapter
implements https://github.com/opendatadiscovery/opendatadiscovery-specification/blob/main/specification/odd_adapter.yaml
"""


from typing import Any

from odd_collector_sdk.domain.adapter import AbstractAdapter

from odd_collector.adapters.odd_adapter.client import BaseClient, Client


class Adapter(AbstractAdapter):
    def __init__(self, config: Any, client: BaseClient = None) -> None:
        self.__client = client or Client(config=config)
        self.__config = config

    def get_data_source_oddrn(self) -> str:
        return self.__config.data_source_oddrn

    async def get_data_entity_list(self):
        return await self.__client.get_data_entities()
