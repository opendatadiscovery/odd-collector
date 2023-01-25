import logging
from collections import namedtuple
from funcy import concat, lpluck_attr
from typing import List

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.query import tuple_factory
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from odd_collector.adapters.cassandra.generator import CassandraGenerator
from .mappers.keyspaces import map_keyspace
from .mappers.models import TableMetadata, ColumnMetadata, ViewMetadata

from .mappers.tables import map_tables, filter_data
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

            tables = [TableMetadata(*filter_data(table)) for table in tables]
            columns = [ColumnMetadata(*filter_data(column)) for column in columns]
            views = [ViewMetadata(*filter_data(view)) for view in views]

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

    def __execute(self, query: str, params: dict = None) -> List[namedtuple]:
        """
        A method to execute a query in the Cassandra database.
        :param query: the CQL (Cassandra Query Language) instruction.
        :param params: the parameters necessary for the query to execute.
        :return: the results of executing the query.
        """
        return self.__session.execute(query, params or dict())

    def __connect(self):
        """
        A method responsible for connecting to the Cassandra database.
        """
        try:
            profile = ExecutionProfile(row_factory=tuple_factory)
            auth_provider = PlainTextAuthProvider(
                username=self.__username, password=self.__password
            )
            self.__cluster = Cluster(
                contact_points=self.__contact_points,
                port=self.__port,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                auth_provider=auth_provider,
            )
            self.__session = self.__cluster.connect(self.__keyspace)

        except cassandra.DriverException as err:
            logging.error(err)
            raise DBException("Can't connect to Cassandra database!")


class DBException(Exception):
    pass
