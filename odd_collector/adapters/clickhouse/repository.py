from abc import ABC, abstractmethod
from typing import Iterable, Type, Union

from clickhouse_connect import get_client
from clickhouse_connect.driver import Client

from ...domain.plugin import ClickhousePlugin
from .domain import Column, IntegrationEngine, Records, Table
from .logger import logger

# integration_engines = ('PostgreSQL', 'RabbitMQ', 'Kafka', 'MySQL',
#                        'HDFS', 'S3', 'EmbeddedRocksDB', 'JDBC', 'MongoDB', 'ODBC')
INTEGRATION_ENGINES = ("Kafka",)

TABLE_SELECT = f"""
select t.name,
    t.database,
    t.engine,
    t.uuid,
    t.total_rows,
    t.total_bytes,
    t.metadata_path,
    t.data_paths,
    t.is_temporary,
    t.create_table_query,
    t.metadata_modification_time
from system.tables t
where t.database = %(database)s 
and t.engine not in {INTEGRATION_ENGINES}
"""

COLUMN_SELECT = f"""
select 
    c.database, 
    c.table, 
    c.name, 
    c.type, 
    c.position, 
    c.default_kind, 
    c.default_expression, 
    c.data_compressed_bytes, 
    c.data_uncompressed_bytes,
    c.marks_bytes,
    c.comment,
    c.is_in_partition_key,
    c.is_in_sorting_key,
    c.is_in_primary_key,
    c.is_in_sampling_key,
    c.compression_codec
from system.columns c
join system.tables t on (t.database = c.database and t.name = c.table )
where c.database = %(database)s
and t.engine not in {INTEGRATION_ENGINES}
"""

INTEGRATION_ENGINES_SELECT = f"""
select
    t.name,
    t.engine,
    t.engine_full
from system.tables t
where t.database = %(database)s
and t.engine in {INTEGRATION_ENGINES}
"""


class ClickHouseRepositoryBase(ABC):
    @abstractmethod
    def get_records(self) -> Records:
        raise NotImplementedError


class ConnectionParams:
    def __init__(self, clickhouse_plugin: ClickhousePlugin):
        self.host = clickhouse_plugin.host
        self.port = clickhouse_plugin.port
        self.database = clickhouse_plugin.database
        self.user = clickhouse_plugin.user
        self.password = clickhouse_plugin.password.get_secret_value()
        self.secure = clickhouse_plugin.secure
        self.verify = clickhouse_plugin.verify
        self.query_limit = clickhouse_plugin.query_limit


class ClickHouseRepository(ClickHouseRepositoryBase):
    def __init__(self, config: ClickhousePlugin):
        self._config = config

    def get_records(self) -> Records:
        params = ConnectionParams(self._config)
        query_params = {"database": self._config.database}
        logger.debug("Connecting to ClickHouse")

        with get_client(**params.__dict__) as client:
            logger.debug("Retrieving tables")
            tables = self._execute(client, TABLE_SELECT, query_params, Table)
            logger.debug("Retrieving columns")
            columns = self._execute(client, COLUMN_SELECT, query_params, Column)
            logger.debug("Retrieving integration_engines")
            integration_engines = self._execute(
                client, INTEGRATION_ENGINES_SELECT, query_params, IntegrationEngine
            )

            return Records(
                tables=list(tables),
                columns=list(columns),
                integration_engines=list(integration_engines),
            )

    def _execute(
        self,
        client: Client,
        query: str,
        query_params: dict,
        map_to: Type[Union[Column, Table, IntegrationEngine]],
    ) -> Iterable:
        with client.query_row_block_stream(query, query_params) as stream:
            for block in stream:
                for row in block:
                    yield map_to(*row)
