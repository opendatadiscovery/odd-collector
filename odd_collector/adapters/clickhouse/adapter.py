import logging
from typing import List

from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ClickHouseGenerator


from odd_collector_sdk.domain.adapter import AbstractAdapter
from .clickhouse_repository import ClickHouseRepository
from .mappers.tables import map_table
from exception import ClickHouseException


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__config = config
        self.clickhouse_repository = ClickHouseRepository(config)
        self.__oddrn_generator = ClickHouseGenerator(
            host_settings=f"{self.__config.host}", databases=self.__config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            records = self.clickhouse_repository.execute()
            return map_table(
                self.__oddrn_generator,
                records["tables"],
                records["columns"],
                records["_integration_engines"],
                self.__config.database,
            )
        except ClickHouseException as exc:
            logging.error(f"Failed to load metadata: {exc}")
            logging.exception(exc)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
