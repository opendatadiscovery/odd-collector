import logging
from typing import List

import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.query import tuple_factory
from odd_collector_sdk.errors import DataSourceConnectionError

from odd_collector.adapters.cassandra.repository import CassandraRepository, TABLE_METADATA_QUERY, VIEWS_METADATA_QUERY
from odd_collector.adapters.scylladb.mappers.models import (
    TableMetadata,
    ViewMetadata,
)


class ScyllaDBRepository(CassandraRepository):
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
            raise DataSourceConnectionError("Can't connect to Scylla database!")

    def _disconnect(self):
        try:
            self._cluster.shutdown()
        except cassandra.DriverException as err:
            logging.error(err)
            raise DataSourceConnectionError(
                "Error while disconnecting from Scylla database!"
            )

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
