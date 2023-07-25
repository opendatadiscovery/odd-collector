import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
from funcy import lmap

from odd_collector.domain.plugin import SupersetPlugin
from odd_collector.domain.rest_client.client import RequestArgs, RestClient

from . import (
    _METADATA_SCHEMA_URL_PREFIX,
    _keys_to_include_dashboard,
    _keys_to_include_dataset,
)
from .domain.chart import Chart
from .domain.column import Column
from .domain.dashboard import Dashboard
from .domain.database import Database
from .domain.dataset import Dataset
from .domain.metadata import add_owner, create_metadata_extension_list

DEFAULT_PAGE_SIZE = 100
ORDER_DIRECTION = "asc"


class DbUriParser:
    def __init__(self, alchemy_uri: str):
        self.parsed_uri = urlparse(alchemy_uri)

    @property
    def host(self) -> str:
        return self.parsed_uri.hostname

    @property
    def port(self) -> int:
        return self.parsed_uri.port

    @property
    def db_name(self) -> str:
        return self.parsed_uri.path.split("/")[1]


def create_dataset(dataset: Any) -> Dataset:
    return Dataset(
        id=dataset.get("id"),
        metadata=create_metadata_extension_list(
            _METADATA_SCHEMA_URL_PREFIX, dataset, _keys_to_include_dataset
        ),
        description=dataset.get("description"),
        name=dataset.get("table_name"),
        db_id=dataset.get("database").get("id"),
        db_name=dataset.get("database").get("database_name"),
        kind=dataset.get("kind"),
        schema=dataset.get("schema"),
    )


class SupersetClient(RestClient):
    def __init__(self, config: SupersetPlugin):
        self.config = config
        self.base_url = f"{config.server}/api/v1"

    async def get_datasets(self) -> list[Dataset]:
        dataset_nodes = await self._paginate_nodes("dataset")
        return lmap(create_dataset, dataset_nodes)

    async def get_dashboards(self) -> list[Dashboard]:
        charts = await self.__get_charts()
        dashboards_without_metadata = self.extract_dashboards_from_charts(charts)
        nodes_with_metadata = await self._get_dashboards_nodes_with_metadata()

        return self.populate_dashboards_with_metadata(
            dashboards_without_metadata, nodes_with_metadata
        )

    async def get_databases(self) -> List[Database]:
        nodes_ids = await self._paginate_nodes("database")
        headers = await self._build_headers()
        urls = [f"{self.base_url}/database/{database['id']}" for database in nodes_ids]
        databases_nodes = await self.fetch_all_async_responses(
            [RequestArgs("GET", url, None, headers) for url in urls]
        )

        databases: List[Database] = []
        for node in databases_nodes:
            db_params_parser = DbUriParser(node["result"]["sqlalchemy_uri"])
            databases.append(
                Database(
                    id=node["id"],
                    database_name=db_params_parser.db_name,
                    backend=node["result"]["backend"],
                    host=db_params_parser.host,
                    port=db_params_parser.port,
                )
            )
        return databases

    async def get_datasets_columns(
        self, datasets_ids: List[int]
    ) -> Dict[int, List[Column]]:
        datasets_columns: Dict[int, List[Column]] = {}
        nodes = await self._get_datasets_columns_nodes(datasets_ids)
        for dataset_node in nodes:
            result = dataset_node["result"]
            datasets_columns[result["id"]] = [
                Column(
                    id=column.get("id"),
                    name=column.get("column_name"),
                    remote_type=column.get("type"),
                )
                for column in result["columns"]
            ]
        return datasets_columns

    async def _get_access_token(self) -> str:
        payload = {
            "username": self.config.username,
            "password": self.config.password.get_secret_value(),
            "provider": "db",
        }
        async with aiohttp.ClientSession() as session:
            response = await self.fetch_async_response(
                session,
                RequestArgs(
                    method="POST",
                    url=f"{self.base_url}/security/login",
                    payload=payload,
                ),
            )
            return response.get("access_token")

    async def _build_headers(self) -> Dict[str, str]:
        access = await self._get_access_token()
        return {"Authorization": f"Bearer {access}"}

    async def _get_dashboard_nodes_by_chart_ids(self, chart_ids: list[int]) -> tuple:
        headers = await self._build_headers()
        urls = [f"{self.base_url}/chart/{chart_id}" for chart_id in chart_ids]
        dashboard_nodes = self.fetch_all_async_responses(
            [RequestArgs("GET", url, None, headers) for url in urls]
        )
        return await dashboard_nodes

    async def _paginate_nodes(
        self, endpoint: str, columns: Optional[list[str]] = None
    ) -> list[Any]:
        async def get_result_for_a_page(page: int):
            base_q = {
                "page_size": DEFAULT_PAGE_SIZE,
                "order_direction": "asc",
                "page": page,
            }

            if columns is not None:
                base_q["columns"] = columns

            async with aiohttp.ClientSession() as session:
                response = await self.fetch_async_response(
                    session,
                    RequestArgs(
                        method="GET",
                        url=f"{self.base_url}/{endpoint}",
                        params={"q": json.dumps(base_q)},
                        headers=await self._build_headers(),
                    ),
                )
            return response.get("result")

        return await self.collect_nodes_with_pagination(
            DEFAULT_PAGE_SIZE, get_result_for_a_page, 0
        )

    async def __get_charts(self) -> list[Chart]:
        chart_nodes = await self._paginate_nodes("chart", ["datasource_id", "id"])
        chart_ids = [chart_node.get("id") for chart_node in chart_nodes]
        chart_nodes_with_dashboards = await self._get_dashboard_nodes_by_chart_ids(
            chart_ids
        )
        nodes_with_chart_ids: Dict[int, Dict[int, str]] = {
            chart_node.get("id"): {
                dashboard_node["id"]: dashboard_node["dashboard_title"]
                for dashboard_node in chart_node["result"]["dashboards"]
            }
            for chart_node in chart_nodes_with_dashboards
        }
        return [
            Chart(
                id=chart_node.get("id"),
                dataset_id=chart_node.get("datasource_id"),
                dashboards_ids_names=nodes_with_chart_ids.get(chart_node.get("id")),
            )
            for chart_node in chart_nodes
        ]

    async def _get_dashboards_nodes_with_metadata(self) -> dict[int, dict[Any, Any]]:
        nodes_with_metadata = await self._paginate_nodes("dashboard")
        return {node.get("id"): node for node in nodes_with_metadata}

    @staticmethod
    def populate_dashboards_with_metadata(
        dashboards_without_metadata: list[Dashboard],
        nodes_with_metadata: dict[int, dict[Any, Any]],
    ):
        dashboards_with_metadata: list[Dashboard] = []
        for dashboard in dashboards_without_metadata:
            metanode = nodes_with_metadata.get(dashboard.id)
            dashboard.metadata = create_metadata_extension_list(
                _METADATA_SCHEMA_URL_PREFIX, metanode, _keys_to_include_dashboard
            )
            dashboard = add_owner(metanode, dashboard)

            dashboards_with_metadata.append(dashboard)
        return dashboards_with_metadata

    @staticmethod
    def extract_dashboards_from_charts(charts: list[Chart]) -> list[Dashboard]:
        unique_dashboard_ids_names: Dict[int, str] = {}
        for chart in charts:
            unique_dashboard_ids_names |= chart.dashboards_ids_names

        dashboards: List[Dashboard] = []
        for dashboard_id, dashboard_name in unique_dashboard_ids_names.items():
            dashboard = Dashboard(
                id=dashboard_id,
                name=dashboard_name,
                datasets_ids={
                    chart.dataset_id
                    for chart in charts
                    if dashboard_id in chart.dashboards_ids_names.keys()
                },
            )
            dashboards.append(dashboard)

        return dashboards

    async def _get_datasets_columns_nodes(self, datasets_ids: List[int]) -> Tuple:
        headers = await self._build_headers()
        urls = [f"{self.base_url}/dataset/{dataset_id}" for dataset_id in datasets_ids]
        datasets_columns_nodes = self.fetch_all_async_responses(
            [RequestArgs("GET", url, None, headers) for url in urls]
        )
        return await datasets_columns_nodes
