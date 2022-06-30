import logging

import pyodbc
from odd_models.models import DataEntity
from oddrn_generator import OdbcGenerator
from pyodbc import Connection, Cursor

from .mappers.tables import map_table
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList


class Adapter(AbstractAdapter):
    __connection: Connection = None
    __cursor: Cursor = None

    # replace
    def __init__(self, config) -> None:
        # https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
        # https://github.com/mkleehammer/pyodbc/wiki/Install
        # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Linux
        # cat /etc/odbcinst.ini
        self.__conf = config
        self.__data_source: str = f"DRIVER={self.__conf.driver};SERVER={self.__conf.host},{self.__conf.port};" \
                                  f"DATABASE={self.__conf.database};" \
                                  f"UID={self.__conf.user};PWD={self.__conf.password}"
        print(f"__data_source: {self.__data_source}")
        self.__oddrn_generator = OdbcGenerator(host_settings=f"{self.__conf.host}", databases=self.__conf.database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return self.get_data_entities()

    def get_data_entities(self) -> DataEntityList:
        try:
            self.__connect()

            tables_cursor: Cursor = self.__cursor.tables(catalog=self.__conf.database)

            # excluding system tables
            tables = list(filter(lambda t: t.table_schem not in ['INFORMATION_SCHEMA', 'sys', ], tables_cursor))

            columns: list = []
            for table in tables:
                columns_cursor: Cursor = self.__cursor.columns(catalog=self.__conf.database, table=table[2])
                columns.extend(columns_cursor.fetchall())

            tables.sort(key=lambda row: "[{}].[{}].[{}]".format(row[0], row[1], row[2]))
            columns.sort(key=lambda row: "[{}].[{}].[{}].[{:>9}]".format(row[0], row[1], row[2], row[16]))

            entities = map_table(self.__oddrn_generator, tables, columns)
            print(f"Entities: {entities}")
            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=entities,
            )
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        finally:
            self.__disconnect()
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[],
        )

    # replace
    def __connect(self):
        print(f"Connecting __data_source: {self.__data_source}")
        # raise Exception(f"Connecting __data_source: {self.__data_source} ...")
        try:
            self.__connection = pyodbc.connect(self.__data_source)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            logging.error(e)
            raise DBException(f"Database error ({self.__data_source})")

    # replace
    def __disconnect(self):
        try:
            if self.__cursor:
                self.__cursor.close()
        except Exception:
            pass
        try:
            if self.__connection:
                self.__connection.close()
        except Exception:
            pass
        return


class DBException(Exception):
    pass
