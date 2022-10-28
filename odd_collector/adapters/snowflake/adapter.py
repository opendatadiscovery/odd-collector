import logging
from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import SnowflakeGenerator

from odd_collector.adapters.snowflake.repository import SnowflakeRepository
from odd_collector.adapters.snowflake.mappers.tables import map_table


class Adapter(AbstractAdapter):
    def __init__(self, config):
        self.__database = config.database
        self.__repository = SnowflakeRepository(config)
        self.__warehouse = config.warehouse
        self.__oddrn_generator = SnowflakeGenerator(
            host_settings=f"{config.account}.snowflakecomputing.com",
            warehouses=self.__warehouse,
            databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:

            tables = self.__repository.get_tables()
            columns = self.__repository.get_columns()

            return map_table(
                self.__oddrn_generator, tables, columns, self.__database
            )
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )
