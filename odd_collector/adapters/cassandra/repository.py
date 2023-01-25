import contextlib
import logging
from abc import ABC, abstractmethod
from typing import List

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.query import tuple_factory


TABLE_METADATA_QUERY: str = """
    SELECT * FROM system_schema.tables 
    WHERE keyspace_name = %(keyspace)s;
"""

COLUMNS_METADATA_QUERY: str = """
    SELECT * FROM system_schema.columns 
    WHERE keyspace_name = %(keyspace)s;
"""

VIEWS_METADATA_QUERY: str = """
    SELECT * FROM system_schema.views 
    WHERE keyspace_name = %(keyspace)s;
"""


class AbstractRepository(ABC):
    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_columns(self):
        pass

    @abstractmethod
    def get_views(self):
        pass


class CassandraRepository(AbstractRepository):
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

    @contextlib.contextmanager
    def connection(self):
        self.__connect()
        yield
        self.__disconnect()

    def __connect(self):
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

    def __disconnect(self):
        try:
            self.__cluster.shutdown()
        except cassandra.DriverException as err:
            logging.error(err)
            raise DBException("Error while disconnecting from Cassandra database!")

    def get_columns(self) -> List[tuple]:
        return self.__session.execute(
            COLUMNS_METADATA_QUERY, {"keyspace": self.__keyspace}
        )

    def get_tables(self) -> List[tuple]:
        return self.__session.execute(
            TABLE_METADATA_QUERY, {"keyspace": self.__keyspace}
        )

    def get_views(self) -> List[tuple]:
        views = self.__session.execute(
            VIEWS_METADATA_QUERY, {"keyspace": self.__keyspace}
        )
        res = []
        metadata = self.__cluster.metadata.keyspaces[self.__keyspace]
        for view in views:
            res.append((*view, metadata.views[view[1]].as_cql_query()))

        return res


class DBException(Exception):
    pass
