import logging
from typing import List

from funcy import concat, lpluck_attr
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ScyllaDBGenerator

from .mappers.keyspaces import map_keyspace
from .mappers.tables import map_tables
from .mappers.views import map_views
from .repository import ScyllaDBRepository
from ...domain.plugin import ScyllaDBPlugin


class Adapter(BaseAdapter):
    config: ScyllaDBPlugin
    generator: ScyllaDBGenerator

    def __init__(self, config: ScyllaDBPlugin):
        super().__init__(config)

    def create_generator(self) -> ScyllaDBGenerator:
        return ScyllaDBGenerator(
            host_settings=f"{self.config.host}", keyspaces=self.config.database
        )

    def get_data_entities(self) -> List[DataEntity]:
        """
        A method to get the data entities list in the Scylla database. It connects to the database and executes a
        query to gather information about the tables in the keyspace, and the keyspace itself. Finally, it arranges them
        in a list of data entities and returns that list.
        :return: list of data entities describing the keyspace and all its tables.
        """
        try:
            repo = ScyllaDBRepository(self.config)
            with repo.connection():
                tables = repo.get_tables()
                columns = repo.get_columns()
                views = repo.get_views()

            tables_entities = map_tables(
                self.generator, tables, columns, self.config.database
            )
            views_entities = map_views(self.generator, views, columns)

            oddrns = lpluck_attr("oddrn", concat(tables_entities, views_entities))
            keyspace_entity = map_keyspace(self.generator, self.config.database, oddrns)
            return tables_entities + views_entities + [keyspace_entity]

        except Exception as e:
            logging.error("Failed to load Scylla data entities")
            logging.exception(e)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
