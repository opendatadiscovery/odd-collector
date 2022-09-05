from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union
from urllib.parse import urlparse

import tableauserverclient as TSC
from funcy import lmap
from odd_collector_sdk.errors import DataSourceAuthorizationError, DataSourceError
from tableauserverclient import PersonalAccessTokenAuth, TableauAuth

from odd_collector.domain.plugin import TableauPlugin

from .domain.column import Column
from .domain.sheet import Sheet
from .domain.table import Table, databases_to_tables

sheets_query = """
    {
        sheets {
            id
            name
            createdAt
            updatedAt
            workbook {
                name
                owner {
                    name
                }
            }
            upstreamFields {
                id
                name
                upstreamTables {
                    id
                    name
                }
            }
        }
    }
"""
databases_query = """
{
databases {
    id
    name
    connectionType
    downstreamOwners {
        name
    }
    tables {
        id
        schema
        name
        description
    }
  }
}
"""
tables_columns_query = """
query GetTablesColumns($ids: [ID]){
    tables(filter: {idWithin: $ids}) {
        id
        columns {
            id
            name
            remoteType
            isNullable
            description
        }
    }
}
"""


class TableauBaseClient(ABC):
    @abstractmethod
    def get_server_host(self):
        raise NotImplementedError

    @abstractmethod
    def get_sheets(self) -> List[Sheet]:
        raise NotImplementedError

    @abstractmethod
    def get_tables(self) -> List[Table]:
        raise NotImplementedError

    @abstractmethod
    def get_tables_columns(self, tables_ids: List[str]) -> Dict[str, Table]:
        raise NotImplementedError


class TableauClient(TableauBaseClient):
    def __init__(self, config: TableauPlugin) -> None:
        self.__config = config
        self.__auth = self.__get_auth(config)
        self.server = TSC.Server(config.server, use_server_version=True)

    def get_server_host(self):
        return urlparse(self.__config.server).netloc

    def get_sheets(self) -> List[Sheet]:
        sheets_response = self.__query(query=sheets_query, root_key="sheets")

        return [Sheet.from_response(response) for response in sheets_response]

    def get_tables(self) -> List[Table]:
        databases_response = self.__query(query=databases_query, root_key="databases")
        return databases_to_tables(databases_response)

    def get_tables_columns(self, table_ids: List[str]) -> Dict[str, List[Column]]:
        response: List = self.__query(
            query=tables_columns_query,
            variables={"ids": table_ids},
            root_key="tables",
        )

        return {
            table.get("id"): lmap(Column.from_response, table.get("columns"))
            for table in response
        }

    def __query(
        self, query: str, variables: object = None, root_key: str = None
    ) -> Any:
        with self.server.auth.sign_in(self.__auth):
            try:
                response = self.server.metadata.query(
                    query, variables, abort_on_error=True
                )
                if root_key:
                    return response["data"][root_key]
                return response["data"]
            except Exception as e:
                raise DataSourceError(
                    f"Couldn't get data for: {root_key} with vars: {variables}"
                ) from e

    @staticmethod
    def __get_auth(
        config: TableauPlugin,
    ) -> Union[PersonalAccessTokenAuth, TableauAuth]:
        try:
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
        except Exception as e:
            raise DataSourceAuthorizationError("Couldn't connect to Tablue") from e
