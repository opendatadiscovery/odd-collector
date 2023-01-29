import logging
from typing import List

from funcy import concat, lpluck_attr
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import CassandraGenerator

from .mappers.keyspaces import map_keyspace
from .mappers.tables import map_tables
from .mappers.views import map_views
from .repository import CassandraRepository


class Adapter(AbstractAdapter):
    __cluster = None
    __session = None

    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__keyspace = config.database
        self.__username = config.user
        self.__password = config.password
        self.__contact_points = config.contact_points or [config.host]
        # self.__execution_profile = config['execution_profile'] TODO To be added.
        self.__repo = CassandraRepository(config)
        self.__oddrn_generator = CassandraGenerator(
            host_settings=f"{self.__host}", keyspaces=self.__keyspace
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        """
        A method to get the data entities list in the Cassandra database. It connects to the database and executes a
        query to gather information about the tables in the keyspace, and the keyspace itself. Finally, it arranges them
        in a list of data entities and returns that list.
        :return: list of data entities describing the keyspace and all its tables.
        """
        try:
            with self.__repo.connection():
                tables = self.__repo.get_tables()
                columns = self.__repo.get_columns()
                views = self.__repo.get_views()

            tables_entities = map_tables(
                self.__oddrn_generator, tables, columns, self.__keyspace
            )
            views_entities = map_views(self.__oddrn_generator, views, columns)

            oddrns = lpluck_attr("oddrn", concat(tables_entities, views_entities))
            keyspace_entity = map_keyspace(
                self.__oddrn_generator, self.__keyspace, oddrns
            )
            return tables_entities + views_entities + [keyspace_entity]

        except Exception as e:
            logging.error("Failed to load Cassandra data entities")
            logging.exception(e)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
