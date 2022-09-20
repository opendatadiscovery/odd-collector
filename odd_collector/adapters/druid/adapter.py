from typing import Type, List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList, DataEntity, DataEntityType

from odd_collector.adapters.druid.client import DruidBaseClient, DruidClient
from odd_collector.adapters.druid.generator import DruidGenerator
from odd_collector.domain.plugin import DruidPlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: DruidPlugin, client: Type[DruidBaseClient] = None) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__oddrn_generator = DruidGenerator(
            host_settings=f"{self.__host}"
        )
        self.client = client or DruidClient

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(),
        )

    def get_data_entities(self) -> List[DataEntity]:
        # Prepare
        data_entities = List[DataEntity] = []

        # Fetch
        tables = self.client.get_tables()

        # Transform
        for table in tables:
            DataEntity(
                oddrn=self.__oddrn_generator.get_oddrn_by_path("tables", table.name),
                name=table.name
            )

        # Return
        return data_entities

