from typing import List, Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList, DataEntity

from odd_collector.adapters.druid.client import DruidBaseClient, DruidClient
from odd_collector.adapters.druid.domain.column import Column
from odd_collector.adapters.druid.domain.table import Table
from odd_collector.adapters.druid.generator import DruidGenerator
from odd_collector.adapters.druid.mappers.tables import table_to_data_entity
from odd_collector.domain.plugin import DruidPlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: DruidPlugin, client: Type[DruidBaseClient] = None) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__oddrn_generator = DruidGenerator(
            host_settings=f"{self.__host}"
        )
        client = client or DruidClient
        self.client = client(config)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(await self.get_data_entities()),
        )

    async def get_data_entities(self) -> List[DataEntity]:
        # Fetch
        tables: List[Table] = await self.client.get_tables()
        columns: List[Column] = await self.client.get_columns()
        tables_nr_of_rows: dict = await self.client.get_tables_nr_of_rows()

        # Transform
        data_entities = [
            table_to_data_entity(
                self.__oddrn_generator,
                table,
                list(filter(lambda column: column.table == table.name, columns)),
                tables_nr_of_rows.get(table.name, 0)
            ) for table in tables
        ]

        # Return
        return data_entities
