from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import AirbytePlugin
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import AirbyteGenerator
from .api import AirbyteApi, OddPlatformApi
from .mappers.connections import map_connection


class Adapter(AbstractAdapter):
    def __init__(self, config: AirbytePlugin) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__airbyte_api = AirbyteApi(self.__host, self.__port)
        self.__odd_api = OddPlatformApi(self.__host, "8080")
        self.__oddrn_generator = AirbyteGenerator(host_settings=config.host)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.__get_connections(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def __get_connections(self) -> list[DataEntity]:
        all_workspaces = self.__airbyte_api.get_all_workspaces()
        all_connections = self.__airbyte_api.get_all_connections(all_workspaces)
        return [
            map_connection(
                connection, self.__oddrn_generator, self.__airbyte_api, self.__odd_api
            )
            for connection in all_connections
        ]
