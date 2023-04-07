from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import Generator, MysqlGenerator

from odd_collector.domain.plugin import MySQLPlugin

from .mappers.database import map_database
from .mappers.tables import map_tables
from .repository import Repository


class Adapter(BaseAdapter):
    config: MySQLPlugin
    generator: MysqlGenerator

    def create_generator(self) -> Generator:
        return MysqlGenerator(
            host_settings=self.config.host, databases=self.config.database
        )

    def __init__(self, config: MySQLPlugin) -> None:
        super().__init__(config)
        self.repository = Repository(config)

    def get_data_entity_list(self) -> DataEntityList:
        tables = self.repository.get_tables()

        tables_entities = map_tables(self.generator, tables)
        database_entity = map_database(
            self.generator, self.config.database, lpluck_attr("oddrn", tables_entities)
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_entities, database_entity],
        )
