import logging
from typing import List

import mysql.connector
from mysql.connector import errorcode

from .mappers import _column_order_by, _column_table
from .mappers.models import ColumnMetadata
from .mysql_repository_base import MysqlRepositoryBase


class MysqlRepository(MysqlRepositoryBase):
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

    def get_columns(self):
        columns = self.__query(
            ColumnMetadata.get_str_fields(), _column_table, _column_order_by
        )
        return columns

    def __query(self, columns: str, table: str, order_by: str) -> List[tuple]:
        return self.__execute(f"select {columns} from {table} order by {order_by}")

    def __execute(self, query: str) -> List[tuple]:
        try:
            mysql_conn_params = {
                "host": self.__host,
                "port": self.__port,
                "database": self.__database,
                "user": self.__user,
                "password": self.__password,
                "ssl_disabled": self.__ssl_disabled,
            }
            with mysql.connector.connect(**mysql_conn_params) as mysql_conn:
                with mysql_conn.cursor() as mysql_cur:
                    mysql_cur.execute(query)
                    records = mysql_cur.fetchall()
                    return records
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error("Database does not exist")
            else:
                logging.error(err)
            raise DBException("Database error")

    def __generate_table_metadata_query(self):
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
               v.view_definition
        from information_schema.tables t
                 left join information_schema.views v
                           on t.TABLE_CATALOG = v.TABLE_CATALOG and
                              t.TABLE_SCHEMA = v.TABLE_SCHEMA and
                              t.TABLE_NAME = v.TABLE_NAME
        where t.table_schema = '{self.__database}'
        order by t.table_catalog, t.table_schema, t.table_name
        """


class DBException(Exception):
    pass
