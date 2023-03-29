from typing import List

from mappers.tables import map_table
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import PostgresqlGenerator
from repository import CockroachDbSQLRepository

from odd_collector.domain.plugin import CockroachDBPlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: CockroachDBPlugin) -> None:
        self._database = config.database
        self._repository = CockroachDbSQLRepository(config)
        self._generator = PostgresqlGenerator(
            host_settings=f"{config.host}", databases=self._database
        )

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        tables = self._repository.get_tables()
        columns = self._repository.get_columns()
        primary_keys = self._repository.get_primary_keys()

        return map_table(self._generator, tables, columns, primary_keys, self._database)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )
