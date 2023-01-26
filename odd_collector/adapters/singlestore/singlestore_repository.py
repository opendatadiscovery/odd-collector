from .logger import logger
import mysql.connector
from mysql.connector import errorcode

from .mappers.models import ColumnMetadata
from .singlestore_repository_base import SingleStoreRepositoryBase
from odd_collector_sdk.errors import DataSourceError


class SingleStoreRepository(SingleStoreRepositoryBase):
    _column_table: str = (
        "information_schema.columns "
        "where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    )
    _column_order_by: str = "table_catalog, table_schema, table_name, ordinal_position"

    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__ssl_disabled = config.ssl_disabled

    def get_tables(self):
        tables = self.__execute(self.__generate_table_metadata_query())
        return tables

    def get_views(self):
        views = self.__execute(self.__generate_view_metadata_query())
        return views

    def get_columns(self):
        columns = self.__query(
            ColumnMetadata.get_str_fields(), self._column_table, self._column_order_by
        )
        return columns

    def __query(self, columns: str, table: str, order_by: str) -> list[tuple]:
        return self.__execute(f"select {columns} from {table} order by {order_by}")

    def __execute(self, query: str) -> list[tuple]:
        try:
            singlestore_conn_params = {
                "host": self.__host,
                "port": self.__port,
                "database": self.__database,
                "user": self.__user,
                "password": self.__password,
                "ssl_disabled": self.__ssl_disabled,
            }
            with mysql.connector.connect(**singlestore_conn_params) as singlestore_conn:
                with singlestore_conn.cursor() as singlestore_cur:
                    singlestore_cur.execute(query)
                    records = singlestore_cur.fetchall()
                    return records
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error("Database does not exist")
            else:
                logger.error(err)
            raise DataSourceError("Database error") from err

    def __generate_table_metadata_query(self):
        return f"""
        with row_counts as(
            select table_name, sum(rows) as table_rows
            from information_schema.table_statistics 
            where DATABASE_NAME = '{self.__database}' and PARTITION_TYPE = 'Master'
            group by table_name
        )
        select t.table_catalog,
               t.table_schema,
               t.table_name,
               t.table_type,
               t.engine,
               t.version,
               t.row_format,
               IFNULL(rc.table_rows, t.table_rows) as table_rows,
               t.avg_row_length,
               t.data_length,
               t.max_data_length,
               t.index_length,
               t.data_free,
               t.auto_increment,
               t.create_time,
               t.update_time,
               t.check_time,
               t.table_collation,
               t.checksum,
               t.create_options,
               t.table_comment,
               t.distributed,
               t.storage_type,
               t.alter_time,
               t.create_user,
               t.alter_user,
               t.flags,
               v.view_definition
        from information_schema.tables t
                left join information_schema.views v
                    on t.TABLE_CATALOG = v.TABLE_CATALOG and
                       t.TABLE_SCHEMA = v.TABLE_SCHEMA and
                       t.TABLE_NAME = v.TABLE_NAME
                left join row_counts rc
                    on t.TABLE_NAME = rc.TABLE_NAME
        where t.table_schema = '{self.__database}' and t.table_type = 'BASE TABLE'
        order by t.table_catalog, t.table_schema, t.table_name
        """

    def __generate_view_metadata_query(self):
        return f"""
        select t.table_catalog,
               t.table_schema,
               t.table_name,
               t.table_type,
               t.engine,
               t.version,
               t.row_format,
               t.table_rows,
               t.avg_row_length,
               t.data_length,
               t.max_data_length,
               t.index_length,
               t.data_free,
               t.auto_increment,
               t.create_time,
               t.update_time,
               t.check_time,
               t.table_collation,
               t.checksum,
               t.create_options,
               t.table_comment,
               t.distributed,
               t.storage_type,
               t.alter_time,
               t.create_user,
               t.alter_user,
               t.flags,
               v.view_definition
        from information_schema.tables t
                left join information_schema.views v
                    on t.TABLE_CATALOG = v.TABLE_CATALOG and
                       t.TABLE_SCHEMA = v.TABLE_SCHEMA and
                       t.TABLE_NAME = v.TABLE_NAME
        where t.table_schema = '{self.__database}' and t.table_type = 'VIEW'
        order by t.table_catalog, t.table_schema, t.table_name
        """
