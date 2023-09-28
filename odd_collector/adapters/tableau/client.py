from abc import ABC, abstractmethod
from typing import Any, Dict, Union
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
query GetSheets($count: Int, $after: String) {
    sheetsConnection(first: $count, after: $after, orderBy: {field: NAME, direction: ASC}) {
        nodes {
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
        pageInfo {
          hasNextPage
          endCursor
        }
    }
}
"""
databases_query = """
query GetDatabases($count: Int, $after: String) {
    databasesConnection(first: $count, after: $after, orderBy: {field: NAME, direction: ASC}) {
        nodes {
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
        pageInfo {
          hasNextPage
          endCursor
        }
    }
}
"""
tables_columns_query = """
query GetTablesColumns($ids: [ID], $count: Int, $after: String){
    tablesConnection(filter: {idWithin: $ids}, first: $count, after: $after, orderBy: {field: NAME, direction: ASC}) {
        nodes {
            id
            columns {
                id
                name
                remoteType
                isNullable
                description
            }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
    }
}
"""


class TableauBaseClient(ABC):
    def __init__(self, config: TableauPlugin) -> None:
        self.config = config

    @abstractmethod
    def get_server_host(self):
        raise NotImplementedError

    @abstractmethod
    def get_sheets(self) -> list[Sheet]:
        raise NotImplementedError

    @abstractmethod
    def get_tables(self) -> list[Table]:
        raise NotImplementedError

    @abstractmethod
    def get_tables_columns(self, tables_ids: list[str]) -> dict[str, list[Column]]:
        raise NotImplementedError


class TableauClient(TableauBaseClient):
    def __init__(self, config: TableauPlugin) -> None:
        super().__init__(config)
        self.__auth = self._get_auth(config)
        self.server = TSC.Server(config.server, use_server_version=True)

    def get_server_host(self):
        return urlparse(self.config.server).netloc

    def get_sheets(self) -> list[Sheet]:
        sheets_response = self._query(query=sheets_query, root_key="sheetsConnection")

        return [Sheet.from_response(response) for response in sheets_response]

    def get_tables(self) -> list[Table]:
        databases_response = self._query(
            query=databases_query, root_key="databasesConnection"
        )
        return databases_to_tables(databases_response)

    def get_tables_columns(self, table_ids: list[str]) -> Dict[str, list[Column]]:
        response: list = self._query(
            query=tables_columns_query,
            variables={"ids": table_ids},
            root_key="tablesConnection",
        )

        return {
            table.get("id"): lmap(Column.from_response, table.get("columns"))
            for table in response
        }

    def _query(
        self,
        query: str,
        root_key: str,
        variables: object = None,
    ) -> Any:
        if variables is None:
            variables = {"count": self.config.pagination_size}

        with self.server.auth.sign_in(self.__auth):
            try:
                has_next_page = True
                end_cursor = None
                while has_next_page:
                    if end_cursor:
                        variables.update({"after": end_cursor or "null"})

                    response = self.server.metadata.query(
                        query, variables, abort_on_error=True
                    )

                    data = response["data"][root_key]

                    page_info = data["pageInfo"]
                    has_next_page = page_info["hasNextPage"]
                    end_cursor = page_info["endCursor"]

                    yield from data["nodes"]
            except Exception as e:
                raise DataSourceError(
                    f"Couldn't get data for: {root_key} with vars: {variables}"
                ) from e

    @staticmethod
    def _get_auth(
        config: TableauPlugin,
    ) -> Union[PersonalAccessTokenAuth, TableauAuth]:
        try:
            if config.token_value and config.token_name:
                return PersonalAccessTokenAuth(
                    config.token_name,
                    config.token_value.get_secret_value(),
                    config.site,
                )
            else:
                return TableauAuth(
                    config.user,
                    config.password.get_secret_value(),
                    config.site,
                )
        except Exception as e:
            raise DataSourceAuthorizationError("Couldn't connect to Tableau") from e
