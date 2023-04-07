from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import PostgresqlGenerator

from odd_collector.domain.plugin import PostgreSQLPlugin

from .mappers.database import map_database
from .mappers.tables import map_table
from .repository import PostgreSQLRepository


class Adapter(BaseAdapter):
    config: PostgreSQLPlugin
    generator: PostgresqlGenerator

    def __init__(self, config: PostgreSQLPlugin) -> None:
        super().__init__(config)
        self._repository = PostgreSQLRepository(config)

    def create_generator(self) -> PostgresqlGenerator:
        return PostgresqlGenerator(
            host_settings=self.config.host, databases=self.config.database
        )

    def get_data_entity_list(self) -> DataEntityList:
        tables = self._repository.get_tables()

        table_entities = map_table(generator=self.generator, tables=tables)

        database_entity = map_database(
            self.generator, self.config.database, lpluck_attr("oddrn", table_entities)
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*table_entities, database_entity],
        )
