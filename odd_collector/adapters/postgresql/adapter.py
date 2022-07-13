
import logging
from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import PostgresqlGenerator

from .mappers.tables import map_table
from .repository import PostgreSQLRepository


class Adapter(AbstractAdapter):

    def __init__(self, config) -> None:
        self.__database = config.database
        self.__posergresql_repository = PostgreSQLRepository(config)
        self.__oddrn_generator = PostgresqlGenerator(
            host_settings=f"{config.host}", databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            tables = self.__posergresql_repository.get_tables()
            columns = self.__posergresql_repository.get_columns()

            return map_table(self.__oddrn_generator, tables, columns, self.__database)
        except Exception:
            logging.error("Failed to load metadata for tables", exc_info=True)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )


