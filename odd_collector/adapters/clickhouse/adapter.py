from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector_sdk.errors import DataSourceError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ClickHouseGenerator

from ...domain.plugin import ClickhousePlugin
from .logger import logger
from .mappers.tables import map_table
from .repository import ClickHouseRepository


class Adapter(AbstractAdapter):
    def __init__(self, config: ClickhousePlugin) -> None:
        self.__db = config.database
        self.clickhouse_repository = ClickHouseRepository(config)
        self.__oddrn_generator = ClickHouseGenerator(
            host_settings=f"{config.host}", databases=config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            records = self.clickhouse_repository.get_records()
            return map_table(
                self.__oddrn_generator,
                records.tables,
                records.columns,
                records.integration_engines,
                self.__db,
            )
        except DataSourceError:
            logger.error(f"Failed to load metadata", exc_info=True)
            return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
