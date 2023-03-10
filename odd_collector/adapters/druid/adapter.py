from typing import List, Type

from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList

from odd_collector.adapters.druid.client import DruidBaseClient, DruidClient
from odd_collector.adapters.druid.generator import DruidGenerator
from odd_collector.adapters.druid.mappers.database import map_database
from odd_collector.adapters.druid.mappers.tables import table_to_data_entity
from odd_collector.domain.plugin import DruidPlugin


class Adapter(AbstractAdapter):
    def __init__(
            self, config: DruidPlugin, client: Type[DruidBaseClient] = None
    ) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__oddrn_generator = DruidGenerator(host_settings=f"{self.__host}")
        client = client or DruidClient
        self.client = client(config)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        # Get data entities
        tables_entities = await self.get_data_entities()

        # Create entity group
        oddrns = lpluck_attr("oddrn", tables_entities)
        database_entity = map_database(self.__oddrn_generator, "druid", oddrns)

        # Return
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=tables_entities + [database_entity],
        )

    async def get_data_entities(self) -> List[DataEntity]:
        # Fetch
        tables, columns, tables_nr_of_rows = await self.client.get_resources()
        tables_columns_stats = await self.client.get_column_stats(
            [table.name for table in tables]
        )

        # Transform
        data_entities = [
            table_to_data_entity(
                self.__oddrn_generator,
                table,
                list(filter(lambda column: column.table == table.name, columns)),
                tables_columns_stats[table.name],
                tables_nr_of_rows.get(table.name, 0),
            )
            for table in tables
        ]

        # Return
        return data_entities
