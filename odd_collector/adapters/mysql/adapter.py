import logging

from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import MysqlGenerator

from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from .mappers.tables import map_tables
from .mysql_repository import MysqlRepository


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__config = config
        self.__mysql_repository = MysqlRepository(config)
        self.__oddrn_generator = MysqlGenerator(
            host_settings=f"{self.__config.host}", databases=self.__config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            tables = self.__mysql_repository.get_tables()
            columns = self.__mysql_repository.get_columns()
            logging.info(f"Load {len(tables)} Datasets DataEntities from database")
            return map_tables(self.__oddrn_generator, tables, columns, self.__config.database)
        except Exception:
            logging.error("Failed to load metadata for tables")
            logging.exception(Exception)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
