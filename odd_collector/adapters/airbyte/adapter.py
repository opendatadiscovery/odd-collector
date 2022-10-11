from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import AirbytePlugin
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import AirbyteGenerator
from .api import AirbyteApi
from .mappers.connections import map_connection


class Adapter(AbstractAdapter):
    def __init__(self, config: AirbytePlugin) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__api = AirbyteApi(self.__host, self.__port)
        self.__oddrn_generator = AirbyteGenerator(host_settings=config.host)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.__get_connections(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def __get_connections(self) -> list[DataEntity]:
        all_workspaces = self.__api.get_all_workspaces()
        all_connections = self.__api.get_all_connections(all_workspaces)
        return [
            map_connection(connection, self.__oddrn_generator, self.__api)
            for connection in all_connections
        ]
