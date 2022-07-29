import logging

from odd_collector.adapters.odbc.connection import connect_odbc
from odd_collector.adapters.odbc.odbc_repository_base import OdbcRepositoryBase
from pyodbc import Cursor


class OdbcRepository(OdbcRepositoryBase):
    def __init__(self, config):
        self.__tables = None
        self.__config = config
        self.__data_source: str = f"DRIVER={self.__config.driver};SERVER={self.__config.host},{self.__config.port};" \
                                  f"DATABASE={self.__config.database};" \
                                  f"UID={self.__config.user};PWD={self.__config.password}"
        logging.debug(f"__data_source: {self.__data_source}")

    def get_tables(self):
        if self.__tables is not None:  # Caching tables
            return self.__tables
        with connect_odbc(self.__data_source) as cursor:
            tables_cursor: Cursor = cursor.tables(catalog=self.__config.database)
            # excluding system tables
            # TODO check if ms sql do have these names
            self.__tables = list(filter(lambda t: t.table_schem not in ['INFORMATION_SCHEMA', 'sys', ], tables_cursor))
            self.__tables.sort(key=lambda row: "[{}].[{}].[{}]".format(row[0], row[1], row[2]))

            return self.__tables

    def get_columns(self):
        with connect_odbc(self.__data_source) as cursor:
            columns: list = []
            for table in self.get_tables():
                columns_cursor: Cursor = cursor.columns(catalog=self.__config.database, table=table[2])
                columns.extend(columns_cursor.fetchall())

            columns.sort(key=lambda row: "[{}].[{}].[{}].[{:>9}]".format(row[0], row[1], row[2], row[16]))

            return columns
