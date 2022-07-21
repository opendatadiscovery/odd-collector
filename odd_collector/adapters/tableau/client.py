from typing import Any
from urllib.parse import urlparse

import tableauserverclient as TSC
from odd_collector.adapters.tableau.logger import logger
from odd_collector.domain.plugin import TableauPlugin

from .query import SHEET_QUERY, TABLE_QUERY


class TableauClient:
    def __init__(self, config: TableauPlugin) -> None:
        self.__config = config

        self.__auth = self.__get_auth(config)
        self.__server = TSC.Server(config.server, use_server_version=True)

    def get_server_host(self):
        return urlparse(self.__config.server).netloc

    def get_tables(self) -> Any:
        return self.__query(TABLE_QUERY)["databaseTables"]

    def get_sheets(self) -> Any:
        return self.__query(SHEET_QUERY)["sheets"]

    def __query(self, query: str) -> any:
        with self.__server.auth.sign_in(self.__auth):
            try:
                response = self.__server.metadata.query(query)
                return response["data"]
            except Exception as e:
                logger.exception(f"Error during query, {e}")
                return []

    @staticmethod
    def __get_auth(config: TableauPlugin) -> None:
        if config.token_value and config.token_name:
            return TSC.PersonalAccessTokenAuth(
                config.token_name,
                config.token_value.get_secret_value(),
                config.site,
            )
        else:
            return TSC.TableauAuth(
                config.user,
                config.password.get_secret_value(),
                config.site,
            )
