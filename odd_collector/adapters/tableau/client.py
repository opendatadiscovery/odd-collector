import contextlib
from pprint import pprint
from typing import Any
from urllib.parse import urlparse
from odd_collector.domain.plugin import TableauPlugin

import tableauserverclient
import logging

from .query import TABLE_QUERY, SHEET_QUERY


class TableauClient:
    __connection = None

    def __init__(self, config: TableauPlugin) -> None:
        self.__server = config.server
        self.__site = config.site
        self.__user = config.user
        self.__password = config.password

    def get_server_host(self):
        return urlparse(self.__server).netloc

    def get_tables(self) -> Any:
        return self.__query(TABLE_QUERY)["databaseTables"]

    def get_sheets(self) -> Any:
        return self.__query(SHEET_QUERY)["sheets"]

    def __query(self, query: str) -> any:
        try:
            self.__connect()
            response = self.__connection.metadata.query(query)
            return response["data"]
        except Exception as e:
            logging.error("Failed to load metadata")
            logging.exception(e)
            return []
        finally:
            self.__disconnect()

    # ContextManager?
    def __connect(self):
        try:
            self.__connection = tableauserverclient.Server(self.__server)
            self.__connection.version = "3.15"
            tableau_auth = tableauserverclient.TableauAuth(
                self.__user, self.__password, self.__site
            )
            self.__connection.auth.sign_in(tableau_auth)
        except Exception as e:
            logging.error("Database error")
            logging.exception(e)
            raise DBException("Database error") from e
        return

    def __disconnect(self):
        with contextlib.suppress(Exception):
            self.__connection.auth.sign_out()
        return


class DBException(Exception):
    pass
