import logging
from abc import ABC, abstractmethod
from typing import Dict

from clickhouse_driver import connect
from odd_collector_sdk.errors import DataSourceError

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


class ClickHouseRepository(ClickHouseRepositoryBase):
    def __init__(self, config: ClickhousePlugin):
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__secure = config.secure
        self.__verify = config.verify
        self.__server_hostname = config.server_hostname

    def get_records(self) -> Records:
        clickhouse_conn_params = {
            "host": self.__host,
            "port": self.__port,
            "database": self.__database,
            "user": self.__user,
            "password": self.__password,
            "secure ": self.__secure,
            "verify": self.__verify,
            "server_hostname": self.__server_hostname,
        }
        with ClickHouseManagerConnection(clickhouse_conn_params) as cursor:
            query_params = {"database": clickhouse_conn_params["database"]}

            logger.debug("Get tables")
            cursor.execute(TABLE_SELECT, query_params)
            tables = [Table(*row) for row in cursor.fetchall()]

            logger.debug("Get columns")
            cursor.execute(COLUMN_SELECT, query_params)
            columns = [Column(*row) for row in cursor.fetchall()]

            logging.debug("Get integration engines")
            cursor.execute(INTEGRATION_ENGINES_SELECT, query_params)
            integration_engines = [IntegrationEngine(*res) for res in cursor.fetchall()]

            return Records(
                tables=tables, columns=columns, integration_engines=integration_engines
            )


class ClickHouseManagerConnection:
    def __init__(self, conn_params: Dict[str, str]):
        self.__conn_params = conn_params

    def __enter__(self):
        self.__clickhouse_conn = connect(**self.__conn_params)
        self.clickhouse_cursor = self.__clickhouse_conn.cursor()
        return self.clickhouse_cursor

    def __exit__(self, *args):
        logger.debug("Try to close cursor")
        try:
            if self.clickhouse_cursor:
                self.clickhouse_cursor.close()
        except Exception as exc:
            raise DataSourceError("Could not close clickhouse cursor") from exc

        logger.debug("Try to close connection")
        try:
            if self.__clickhouse_conn:
                self.__clickhouse_conn.close()
        except Exception as exc:
            raise DataSourceError("Could not close clickhouse connection") from exc

        logging.debug("ClickHouse resource has been released")
