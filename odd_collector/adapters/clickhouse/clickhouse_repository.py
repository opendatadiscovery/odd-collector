import logging
from abc import ABC
from typing import Dict

import clickhouse_driver

from exception import ClickHouseException
from .mappers import _table_select, _column_select, _integration_engines_select


class ClickHouseRepositoryBase(ABC):
    pass


class ClickHouseRepository(ClickHouseRepositoryBase):
    def __init__(self, config):
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password

    def execute(self) -> Dict:
        clickhouse_conn_params = {
            "host": self.__host,
            "port": self.__port,
            "database": self.__database,
            "user": self.__user,
            "password": self.__password,
        }
        with ClickHouseManagerConnection(clickhouse_conn_params) as cursor:
            query_params = {"database": clickhouse_conn_params["database"]}

            logging.info("Get tables")

            cursor.execute(_table_select, query_params)
            tables = cursor.fetchall()

            logging.info("Get columns")
            cursor.execute(_column_select, query_params)
            columns = cursor.fetchall()

            logging.info("Get integration engines")
            cursor.execute(_integration_engines_select, query_params)
            integration_engines = cursor.fetchall()
            return {
                "tables": tables,
                "columns": columns,
                "integration_engines": integration_engines
            }


class ClickHouseManagerConnection:
    def __init__(self, conn_params: Dict[str, str]):
        self.__conn_params = conn_params

    def __enter__(self):
        self.__clickhouse_conn = clickhouse_driver.connect(**self.__conn_params)
        self.clickhouse_cursor = self.__clickhouse_conn.cursor()
        return self.clickhouse_cursor

    def __exit__(self, *args):
        logging.info("Try to close cursor")
        try:
            if self.clickhouse_cursor:
                self.clickhouse_cursor.close()
        except Exception as exc:
            logging.error(f"Cannot close cursor: {exc}")
            raise ClickHouseException("Cannot close clickhouse cursor")

        logging.info("Try to close conneciton")
        try:
            if self.__clickhouse_conn:
                self.__clickhouse_conn.close()
        except Exception as exc:
            logging.error(f"Cannot close connection: {exc}")
            raise ClickHouseException("Cannot close clickhouse conneciton")

        logging.info("ClickHouse resource has been released")
