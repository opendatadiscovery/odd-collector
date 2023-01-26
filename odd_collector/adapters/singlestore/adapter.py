from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import SingleStoreGenerator

from .mappers.tables import map_tables
from .singlestore_repository import SingleStoreRepository
from .logger import logger


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__config = config
        self.__singlestore_repository = SingleStoreRepository(config)
        self.__oddrn_generator = SingleStoreGenerator(
            host_settings=f"{self.__config.host}", databases=self.__config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            tables = self.__singlestore_repository.get_tables()
            columns = self.__singlestore_repository.get_columns()
            logger.info(f"Load {len(tables)} Datasets DataEntities from database")
            return map_tables(
                self.__oddrn_generator, tables, columns, self.__config.database
            )
        except Exception:
            logger.error("Failed to load metadata for tables")
            logger.exception(Exception)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
