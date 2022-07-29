from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import tableauserverclient as TSC
from odd_collector.adapters.tableau.logger import logger
from odd_collector.domain.plugin import TableauPlugin

from .query import SHEET_QUERY

import json

class TableauClient:
    def __init__(self, config: TableauPlugin) -> None:
        self.__config = config

        self.__auth = self.__get_auth(config)
        self.__server = TSC.Server(config.server, use_server_version=True)
        self.__database_cache = {}

    def get_server_host(self):
        return urlparse(self.__config.server).netloc

    def get_sheets(self) -> List[Any]:
        try:
            return self.__query(SHEET_QUERY)["sheets"]
        except Exception as e:
            logger.error(e)
            return []
    
    def get_database_by_luid(self, luid) -> Dict[str, str]:
        if luid in self.__database_cache:
            return self.__database_cache[luid]

        with self.__server.auth.sign_in(self.__auth):
            database: TSC.DatabaseItem = self.__server.databases.get_by_id(luid)

        dikt = {
            'name': database.name,
            'connection_type': database.connection_type,
            'host_name': database.host_name
        }

        self.__database_cache[luid] = dikt
        return dikt

    def get_databases_by_luid(self, luids) -> List[Dict[str, str]]:
        res = []
        with self.__server.auth.sign_in(self.__auth):
            for luid in luids:
                if luid in self.__database_cache:
                    res.append(self.__database_cache[luid])

                database: TSC.DatabaseItem = self.__server.databases.get_by_id(luid)

                dikt = {
                    'luid': luid,
                    'name': database.name,
                    'connection_type': database.connection_type,
                    'host_name': database.host_name
                }

                self.__database_cache[luid] = dikt
                res.append(dikt)
        return res

    def __query(self, query: str) -> Optional[object]:
        with self.__server.auth.sign_in(self.__auth):
            response = self.__server.metadata.query(query, abort_on_error=True)
            return response["data"]

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
