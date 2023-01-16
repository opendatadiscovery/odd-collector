from typing import Optional
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import AirbytePlugin
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import AirbyteGenerator
from .api import AirbyteApi, OddPlatformApi
from .logger import logger
from .mappers.connections import map_connection
from .mappers.datasets import filter_dataset_oddrn, generate_dataset_oddrn


class Adapter(AbstractAdapter):
    def __init__(self, config: AirbytePlugin) -> None:
        self.__airbyte_api = AirbyteApi(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
        )
        self.__odd_api = OddPlatformApi(host_url=config.platform_host_url)
        self.__oddrn_generator = AirbyteGenerator(host_settings=config.host)

    async def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=await self.__get_connections(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def __get_connections(self) -> list[DataEntity]:
        workspaces = await self.__airbyte_api.get_workspaces()
        connections = await self.__airbyte_api.get_connections(workspaces)
        connections_data: list[tuple] = []

        for connection in connections:
            source_id = connection.get("sourceId")
            destination_id = connection.get("destinationId")

            input_oddrns = await self.__get_dataset_oddrns(
                is_source=True, dataset_id=source_id, connection=connection
            )
            output_oddrns = await self.__get_dataset_oddrns(
                is_source=False, dataset_id=destination_id, connection=connection
            )

            connections_data.append((connection, input_oddrns, output_oddrns))

        return [
            map_connection(data, self.__oddrn_generator) for data in connections_data
        ]

    async def __get_dataset_oddrns(
        self, is_source: bool, dataset_id: str, connection: dict
    ) -> list[Optional[str]]:
        dataset_meta = await self.__airbyte_api.get_dataset_definition(
            is_source, dataset_id
        )
        deg_oddrn = generate_dataset_oddrn(is_source, dataset_meta)
        dataset_oddrns = await self.__odd_api.get_data_entities_oddrns(deg_oddrn)
        logger.info(f"DATASET_ODDRNS: {dataset_oddrns}")
        return filter_dataset_oddrn(connection, dataset_oddrns)
