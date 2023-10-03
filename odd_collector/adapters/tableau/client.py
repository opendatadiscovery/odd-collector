from typing import Any, Union

import tableauserverclient as TSC
from odd_collector_sdk.errors import DataSourceAuthorizationError, DataSourceError
from tableauserverclient import PersonalAccessTokenAuth, TableauAuth

from odd_collector.adapters.tableau.domain.table import Table
from odd_collector.domain.plugin import TableauPlugin

from .domain.database import ConnectionParams, EmbeddedDatabase, ExternalDatabase
from .domain.sheet import Sheet
from .logger import logger

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
            isEmbedded
            connectionType
            downstreamOwners {
                name
            }
            tables {
                id
                schema
                name
                description
                columns {
                    id
                    name
                    remoteType
                    isNullable
                    description
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

database_servers_query = """
query DatabaseServersConnection($count: Int, $after: String) {
    databaseServersConnection(first: $count, after: $after, orderBy: {field: NAME, direction: ASC}) {
        nodes {
            id
            name
            isEmbedded
            connectionType
            hostName
            port
            service
        }
        pageInfo {
          hasNextPage
          endCursor
        }
    }
}
"""


class TableauClient:
    def __init__(self, config: TableauPlugin) -> None:
        self.config = config
        self.__auth = self._get_auth(config)
        self.server = TSC.Server(config.server, use_server_version=True)

    def get_sheets(self) -> list[Sheet]:
        sheets_response = self._query(query=sheets_query, root_key="sheetsConnection")

        return [Sheet.from_response(response) for response in sheets_response]

    def get_databases(self) -> dict[str, Union[EmbeddedDatabase, ExternalDatabase]]:
        logger.debug("Getting databases")
        databases = self._query(query=databases_query, root_key="databasesConnection")

        connection_params = self.get_servers()

        result = {}
        for db in databases:
            if db.get("isEmbedded"):
                result[db.get("id")] = EmbeddedDatabase.from_dict(**db)
            else:
                try:
                    database = ExternalDatabase(
                        id=db.get("id"),
                        name=db.get("name"),
                        connection_type=db.get("connectionType"),
                        connection_params=connection_params[db.get("id")],
                        tables=db.get("tables"),
                    )
                    result[database.id] = database
                except Exception as e:
                    logger.warning(f"Couldn't get database: {db.get('name')} {e}")
                    continue

        logger.debug(f"Got {len(result)} databases")
        return result

    def get_tables(self) -> dict[str, Table]:
        databases = self.get_databases()

        return {
            table.id: table
            for database in databases.values()
            for table in database.tables
        }

    def get_servers(self) -> dict[str, ConnectionParams]:
        servers = self._query(
            query=database_servers_query, root_key="databaseServersConnection"
        )
        return {
            server.get("id"): ConnectionParams.from_dict(**server) for server in servers
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
