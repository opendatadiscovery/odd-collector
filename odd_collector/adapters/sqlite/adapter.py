from funcy import concat, lpluck_attr
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import SQLiteGenerator

from odd_collector.domain.plugin import SQLitePlugin
from .mappers.database import map_database
from .mappers.table import map_table
from .mappers.view import map_view
from .repository.sqlalchemy_repository import SqlAlchemyRepository


class Adapter(BaseAdapter):
    config: SQLitePlugin
    generator: SQLiteGenerator

    def __init__(self, config: SQLitePlugin) -> None:
        super().__init__(config)

    def create_generator(self) -> SQLiteGenerator:
        return SQLiteGenerator(path=self.config.data_source)

    def get_data_entity_list(self) -> DataEntityList:
        repo = SqlAlchemyRepository(self.config)
        tables_entities = [
            map_table(self.generator, table) for table in repo.get_tables()
        ]

        views_entities = [map_view(self.generator, view) for view in repo.get_views()]

        oddrns = lpluck_attr("oddrn", concat(tables_entities, views_entities))
        database_entity = map_database(self.generator, self.config.data_source, oddrns)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=tables_entities + views_entities + [database_entity],
        )
