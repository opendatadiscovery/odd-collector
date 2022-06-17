import logging
import pyodbc

from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import MssqlGenerator
from odd_collector_sdk.domain.adapter import AbstractAdapter

from .mappers import table_query, column_query
from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
        # https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
        # https://github.com/mkleehammer/pyodbc/wiki/Install
        # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Linux
        # cat /etc/odbcinst.ini
        self.__data_source: str = (
            f"DRIVER={config.driver};SERVER={config.host};DATABASE={config.database};"
            f"UID={config.user};PWD={config.password}"
        )
        self.__oddrn_generator = MssqlGenerator(
            host_settings=f"{config.host}", databases=config.database
        )

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            self.__connect()

            tables = self.__execute(table_query)
            columns = self.__execute(column_query)

            return map_table(self.__oddrn_generator, tables, columns)
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        finally:
            self.__disconnect()
        return []

    def __execute(self, query: str) -> list[tuple]:
        self.__cursor.execute(query)
        records = self.__cursor.fetchall()
        return records

    def __connect(self):
        try:
            self.__connection = pyodbc.connect(self.__data_source)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            logging.error(e)
            raise DBException("Database error")
        return

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
