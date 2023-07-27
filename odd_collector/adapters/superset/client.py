import contextlib
import json
from typing import Any, Dict, Optional

import aiohttp
from funcy import lmap

from odd_collector.domain.plugin import SupersetPlugin
from odd_collector.domain.rest_client.client import RequestArgs, RestClient

from .domain.chart import Chart
from .domain.dashboard import Dashboard
from .domain.database import Database, DatabaseSchema, Table
from .domain.dataset import Dataset
from .logger import logger

DEFAULT_PAGE_SIZE = 100
ORDER_DIRECTION = "asc"


class SupersetClient(RestClient):
    _session: aiohttp.ClientSession
    _headers: dict[str, str]

    def __init__(self, config: SupersetPlugin):
        self.config = config
        self.base_url = f"{config.server}/api/v1"

    @contextlib.asynccontextmanager
    async def connect(self):
        logger.info("Connecting to Superset")

        async with aiohttp.ClientSession() as session:
            self._session = session
            self._headers = await self._build_headers(session)
            yield self

        self._session = None
        self._headers = None

    async def fetch_datasets(self) -> dict[int, Dataset]:
        dataset_nodes = await self._fetch_all("dataset")
        databases = await self.fetch_databases()

        result = {}
        for node in dataset_nodes:
            database = node.pop("database")
            node["database"] = databases[database["id"]]

            dataset = Dataset.from_dict(node)
            result[dataset.id] = dataset

        return result

    async def fetch_dashboards(self) -> dict[int, Dashboard]:
        columns = ["id", "dashboard_title"]
        response = await self._fetch_all("dashboard", columns)
        dashboards = lmap(Dashboard.from_dict, response)
        return {dashboard.id: dashboard for dashboard in dashboards}

    async def fetch_charts(self) -> dict[int, Chart]:
        columns = ["id", "datasource_id", "slice_name", "dashboards.id"]
        nodes = await self._fetch_all("chart", columns)
        charts = lmap(Chart.from_dict, nodes)
        return {chart.id: chart for chart in charts}

    async def fetch_database_schema_tables(
        self, database_id: int, schema_name: str
    ) -> list[Table]:
        url = f"{self.base_url}/database/{database_id}/tables/?q=(schema_name:{schema_name})"
        request = RequestArgs("GET", url, headers=self._headers)
        response = await self.fetch(self._session, request)

        return lmap(Table.from_dict, response["result"])

    async def fetch_database_schemas(self, database_id: int) -> list[DatabaseSchema]:
        url = f"{self.base_url}/database/{database_id}/schemas"
        request = RequestArgs("GET", url, headers=self._headers)
        response = await self.fetch(self._session, request)

        schemas = []

        for schema_name in response["result"]:
            tables = await self.fetch_database_schema_tables(database_id, schema_name)
            tables = {table.name: table for table in tables}
            schemas.append(DatabaseSchema(schema_name=schema_name, tables=tables))

        return schemas

    async def fetch_database(self, database_id: int) -> Database:
        url = f"{self.base_url}/database/{database_id}/connection"
        request = RequestArgs("GET", url, headers=self._headers)
        response = await self.fetch(self._session, request)
        raw = response["result"]

        schemas = await self.fetch_database_schemas(database_id)
        raw["schemas"] = {schema.schema_name: schema for schema in schemas}

        return Database.from_dict(response["result"])

    async def fetch_databases(self) -> dict[int, Database]:
        nodes = await self._fetch_all("database")

        databases = []
        for database_node in nodes:
            database = await self.fetch_database(database_node["id"])
            databases.append(database)

        return {database.id: database for database in databases}

    async def _get_token(self, session) -> str:
        payload = {
            "username": self.config.username,
            "password": self.config.password.get_secret_value(),
            "provider": "db",
        }
        logger.info("Getting access token")
        response = await self.fetch(
            session,
            RequestArgs(
                method="POST", url=f"{self.base_url}/security/login", payload=payload
            ),
        )
        if response and response.get("access_token"):
            logger.info("Access token received")
            return response["access_token"]

        raise Exception("Failed to get access token")

    async def _build_headers(self, session) -> Dict[str, str]:
        token = await self._get_token(session)
        return {"Authorization": f"Bearer {token}"}

    async def _fetch_all(
        self, endpoint: str, columns: Optional[list[str]] = None
    ) -> list[Any]:
        async def fetch_page(
            page: int, session: aiohttp.ClientSession
        ) -> dict[str, Any]:
            url = f"{self.base_url}/{endpoint}"
            base_q = {
                "page_size": DEFAULT_PAGE_SIZE,
                "order_direction": ORDER_DIRECTION,
                "page": page,
            }

            if columns:
                base_q["columns"] = columns

            params = {"q": json.dumps(base_q)}

            return await self.fetch(
                session, RequestArgs("GET", url, params, self._headers)
            )

        results = []
        page = 0

        while True:
            response = await fetch_page(page, self._session)
            count = response["count"]
            results.extend(response["result"])

            if count <= len(results):
                return results

            page += 1
