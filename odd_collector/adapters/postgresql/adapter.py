from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import PostgresqlGenerator

from .mappers.tables import map_table
from .repository import PostgreSQLRepository


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self._database = config.database
        self._repository = PostgreSQLRepository(config)
        self._generator = PostgresqlGenerator(
            host_settings=f"{config.host}", databases=self._database
        )

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        tables, columns, primary_keys, enum_types = self._repository.get_metadata()

        return map_table(
            oddrn_generator=self._generator,
            tables=tables,
            columns=columns,
            primary_keys=primary_keys,
            enum_type_labels=enum_types,
            database=self._database,
        )

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )
