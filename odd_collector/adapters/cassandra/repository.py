import contextlib
import logging
from abc import ABC, abstractmethod
from typing import Any, List, Tuple, Union

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.query import tuple_factory
from cassandra.util import OrderedMapSerializedKey, SortedSet
from odd_collector_sdk.errors import DataSourceConnectionError

from odd_collector.adapters.cassandra.mappers.models import (
    ColumnMetadata,
    TableMetadata,
    ViewMetadata,
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
    _cluster = None
    _session = None

    def __init__(self, config):
        self._host = config.host
        self._port = config.port
        self._keyspace = config.database
        self._username = config.user
        self._password = config.password
        self._contact_points = config.contact_points or [config.host]
        # self._execution_profile = config['execution_profile'] TODO To be added.

    @contextlib.contextmanager
    def connection(self):
        self._connect()
        yield
        self._disconnect()

    def _connect(self):
        try:
            profile = ExecutionProfile(row_factory=tuple_factory)
            auth_provider = PlainTextAuthProvider(
                username=self._username, password=self._password
            )
            self._cluster = Cluster(
                contact_points=self._contact_points,
                port=self._port,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                auth_provider=auth_provider,
            )
            self._session = self._cluster.connect(self._keyspace)

        except cassandra.DriverException as err:
            logging.error(err)
            raise DataSourceConnectionError("Can't connect to Cassandra database!")

    def _disconnect(self):
        try:
            self._cluster.shutdown()
        except cassandra.DriverException as err:
            logging.error(err)
            raise DataSourceConnectionError(
                "Error while disconnecting from Cassandra database!"
            )

    def get_columns(self) -> List[ColumnMetadata]:
        columns = self._session.execute(
            COLUMNS_METADATA_QUERY, {"keyspace": self._keyspace}
        )
        return [ColumnMetadata(*self._filter_data(column)) for column in columns]

    def get_tables(self) -> List[TableMetadata]:
        tables = self._session.execute(
            TABLE_METADATA_QUERY, {"keyspace": self._keyspace}
        )
        return [TableMetadata(*self._filter_data(table)) for table in tables]

    def get_views(self) -> List[ViewMetadata]:
        views = self._session.execute(
            VIEWS_METADATA_QUERY, {"keyspace": self._keyspace}
        )
        res = []
        metadata = self._cluster.metadata.keyspaces[self._keyspace]
        for view in views:
            view = (*view, metadata.views[view[1]].as_cql_query())
            res.append(ViewMetadata(*self._filter_data(view)))
        return res

    def _filter_data(
        self, data: Tuple[Any, Any]
    ) -> tuple[Union[Union[dict, list], Any], ...]:
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
