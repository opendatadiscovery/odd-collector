import aiohttp
from databricks_cli.sdk.api_client import ApiClient
from odd_collector_sdk.errors import DataSourceError
from odd_collector.domain.plugin import DatabricksPlugin


class DatabricksRestClient:
    def __init__(self, config: DatabricksPlugin):
        self.__host = f"https://{config.workspace}"
        self.api_client = ApiClient(host=self.__host, token=config.token)
        self.__headers = {"Authorization": f"Bearer {config.token}"}
        self.__requests = {
            "catalogs": "/api/2.1/unity-catalog/catalogs",
            "schemas": "/api/2.1/unity-catalog/schemas",
            "tables": "/api/2.1/unity-catalog/tables",
        }

    async def _get_request(self, url: str, params: dict = None) -> dict:
        async with aiohttp.ClientSession(
            self.__host, headers=self.__headers
        ) as session:
            try:
                async with session.get(url, params=params) as resp:
                    result = await resp.json()
                return result
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data from workspace {self.__host}"
                ) from e

    async def get_catalogs(self) -> list[str]:
        resp = await self._get_request(self.__requests["catalogs"])
        catalogs = [catalog["name"] for catalog in resp["catalogs"]]
        return catalogs

    async def get_schemas(self, catalog: str) -> list[str]:
        params = {"catalog_name": catalog}
        resp = await self._get_request(self.__requests["schemas"], params)
        schemas = [schema["name"] for schema in resp["schemas"]]
        return schemas

    async def get_tables(self, catalog: str, schema: str) -> list[dict]:
        params = {"catalog_name": catalog, "schema_name": schema}
        resp = await self._get_request(self.__requests["tables"], params)
        if resp:
            tables = [table for table in resp["tables"]]
            return tables
        return []
