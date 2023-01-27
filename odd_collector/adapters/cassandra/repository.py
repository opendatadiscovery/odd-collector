import contextlib
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple, Any

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.query import tuple_factory
from cassandra.util import OrderedMapSerializedKey, SortedSet
from odd_collector_sdk.errors import DataSourceConnectionError

from odd_collector.adapters.cassandra.mappers.models import (
    ColumnMetadata,
    ViewMetadata,
    TableMetadata,
)


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
    def get_tables(self) -> List[TableMetadata]:
        pass

    @abstractmethod
    def get_columns(self) -> List[ColumnMetadata]:
        pass

    @abstractmethod
    def get_views(self) -> List[ViewMetadata]:
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
            raise DataSourceConnectionError("Can't connect to Cassandra database!")

    def __disconnect(self):
        try:
            self.__cluster.shutdown()
        except cassandra.DriverException as err:
            logging.error(err)
            raise DataSourceConnectionError(
                "Error while disconnecting from Cassandra database!"
            )

    def get_columns(self) -> List[ColumnMetadata]:
        columns = self.__session.execute(
            COLUMNS_METADATA_QUERY, {"keyspace": self.__keyspace}
        )
        return [ColumnMetadata(*self.__filter_data(column)) for column in columns]

    def get_tables(self) -> List[TableMetadata]:
        tables = self.__session.execute(
            TABLE_METADATA_QUERY, {"keyspace": self.__keyspace}
        )
        return [TableMetadata(*self.__filter_data(table)) for table in tables]

    def get_views(self) -> List[ViewMetadata]:
        views = self.__session.execute(
            VIEWS_METADATA_QUERY, {"keyspace": self.__keyspace}
        )
        res = []
        metadata = self.__cluster.metadata.keyspaces[self.__keyspace]
        for view in views:
            view = (*view, metadata.views[view[1]].as_cql_query())
            res.append(ViewMetadata(*self.__filter_data(view)))
        return res

    def __filter_data(self, data: Tuple[Any, Any]) -> Tuple[Any]:
        """
        A method to filter the data obtained from the Cassandra database. It converts the Cassandra types
        OrderedMapSerializedKey, SortedSet to usual Python dictionary and list, respectively
        :param data: the data obtained from the Cassandra database.
        :return: the same data after filtering the types.
        """
        filtered = []
        for value in data:
            if type(value) is OrderedMapSerializedKey:
                filtered.append(dict(value))
            elif type(value) is SortedSet:
                filtered.append(list(value))
            else:
                filtered.append(value)
        return tuple(filtered)
