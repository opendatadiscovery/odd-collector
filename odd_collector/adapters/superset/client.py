from typing import List, Dict, Any, NamedTuple, Optional, Tuple
from urllib.parse import urlparse
from .domain.column import Column
from .domain.chart import Chart
from .domain.dashboard import Dashboard
from odd_collector.domain.plugin import SupersetPlugin
from .domain.dataset import Dataset
from .domain.database import Database
from .domain.metadata import create_metadata_extension_list, add_owner
from . import (
    _METADATA_SCHEMA_URL_PREFIX,
    _keys_to_include_dashboard,
    _keys_to_include_dataset,
)
from json import dumps

import asyncio
import aiohttp


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


class RequestArgs(NamedTuple):
    method: str
    url: str
    params: Optional[Dict[Any, Any]] = None
    headers: Optional[Dict[Any, Any]] = None
    payload: Optional[Dict[Any, Any]] = None


class SupersetClient:
    def __init__(self, config: SupersetPlugin):
        self.__config = config
        self.__base_url = config.server + "/api/v1/"

    async def __get_access_token(self) -> str:
        payload = {
            "username": self.__config.username,
            "password": self.__config.password,
            "provider": "db",
        }
        async with aiohttp.ClientSession() as session:
            response = await self.__fetch_async_response(
                session,
                RequestArgs(
                    method="POST",
                    url=self.__base_url + "security/login",
                    payload=payload,
                ),
            )
            return response.get("access_token")

    @staticmethod
    async def __fetch_async_response(
        session, request_args: RequestArgs
    ) -> Dict[Any, Any]:
        async with session.request(
            request_args.method,
            url=request_args.url,
            params=request_args.params,
            headers=request_args.headers,
            json=request_args.payload,
        ) as response:
            return await response.json()

    async def __fetch_all_async_responses(
        self, request_args_list: List[RequestArgs]
    ) -> Tuple:
        async with aiohttp.ClientSession() as session:
            return await asyncio.gather(
                *[
                    self.__fetch_async_response(session, request_args=request_args)
                    for request_args in request_args_list
                ],
                return_exceptions=True,
            )

    async def __build_headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer " + await self.__get_access_token()}

    def get_server_host(self) -> str:
        return urlparse(self.__config.server).netloc

    async def get_datasets(self) -> List[Dataset]:
        dataset_nodes = await self.__get_nodes_list_with_pagination("dataset")
        return [
            Dataset(
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
            for dataset in dataset_nodes
        ]

    async def __get_dashboard_nodes_by_chart_ids(self, chart_ids: List[int]) -> Tuple:
        headers = await self.__build_headers()
        urls = [self.__base_url + f"chart/{chart_id}" for chart_id in chart_ids]
        dashboard_nodes = self.__fetch_all_async_responses(
            [RequestArgs("GET", url, None, headers) for url in urls]
        )
        return await dashboard_nodes

    async def __get_nodes_list_with_pagination(
        self, endpoint: str, columns: List[str] = None
    ) -> List[Any]:
        default_page_size = 100

        async def get_result_for_a_page(page: int):
            base_q = {
                "page_size": default_page_size,
                "order_direction": "asc",
                "page": page,
            }
            if columns is not None:
                base_q.update({"columns": columns})
            async with aiohttp.ClientSession() as session:
                response = await self.__fetch_async_response(
                    session,
                    RequestArgs(
                        method="GET",
                        url=self.__base_url + endpoint,
                        params={"q": dumps(base_q)},
                        headers=await self.__build_headers(),
                    ),
                )
            return response.get("result")

        nodes_list = []
        pg = 0
        results_len = default_page_size
        while results_len == default_page_size:
            result = await get_result_for_a_page(pg)
            nodes_list += result
            results_len = len(result)
            pg += 1
        return nodes_list

    async def __get_charts(self) -> List[Chart]:
        chart_nodes = await self.__get_nodes_list_with_pagination(
            "chart", ["datasource_id", "id"]
        )
        chart_ids = [chart_node.get("id") for chart_node in chart_nodes]
        chart_nodes_with_dashboards = await self.__get_dashboard_nodes_by_chart_ids(
            chart_ids
        )
        nodes_with_chart_ids: Dict[int, Dict[int, str]] = {}
        for chart_node in chart_nodes_with_dashboards:
            nodes_with_chart_ids.update(
                {
                    chart_node.get("id"): {
                        dashboard_node["id"]: dashboard_node["dashboard_title"]
                        for dashboard_node in chart_node["result"]["dashboards"]
                    }
                }
            )

        return [
            Chart(
                id=chart_node.get("id"),
                dataset_id=chart_node.get("datasource_id"),
                dashboards_ids_names=nodes_with_chart_ids.get(chart_node.get("id")),
            )
            for chart_node in chart_nodes
        ]

    async def get_dashboards(self) -> List[Dashboard]:
        dashboards_without_metadata = self.extract_dashboards_from_charts(
            await self.__get_charts()
        )
        nodes_with_metadata = await self.__get_dashboards_nodes_with_metadata()
        return self.populate_dashboards_with_metadata(
            dashboards_without_metadata, nodes_with_metadata
        )

    async def __get_dashboards_nodes_with_metadata(self) -> Dict[int, Dict[Any, Any]]:
        nodes_with_metadata = await self.__get_nodes_list_with_pagination("dashboard")
        return {node.get("id"): node for node in nodes_with_metadata}

    @staticmethod
    def populate_dashboards_with_metadata(
        dashboards_without_metadata: List[Dashboard],
        nodes_with_metadata: Dict[int, Dict[Any, Any]],
    ):
        dashboards_with_metadata: List[Dashboard] = []
        for dashboard in dashboards_without_metadata:
            metanode = nodes_with_metadata.get(dashboard.id)
            dashboard.metadata = create_metadata_extension_list(
                _METADATA_SCHEMA_URL_PREFIX, metanode, _keys_to_include_dashboard
            )
            dashboard = add_owner(metanode, dashboard)

            dashboards_with_metadata.append(dashboard)
        return dashboards_with_metadata

    @staticmethod
    def extract_dashboards_from_charts(charts: List[Chart]) -> List[Dashboard]:
        unique_dashboard_ids_names: Dict[int, str] = {}
        for chart in charts:
            unique_dashboard_ids_names.update(chart.dashboards_ids_names)
        dashboards: List[Dashboard] = []
        for dashboard_id, dashboard_name in unique_dashboard_ids_names.items():
            dashboard = Dashboard(
                id=dashboard_id,
                name=dashboard_name,
                datasets_ids=set(
                    [
                        chart.dataset_id
                        for chart in charts
                        if dashboard_id in chart.dashboards_ids_names.keys()
                    ]
                ),
            )
            dashboards.append(dashboard)

        return dashboards

    async def __get_datasets_columns_nodes(self, datasets_ids: List[int]) -> Tuple:
        headers = await self.__build_headers()
        urls = [
            self.__base_url + f"dataset/{dataset_id}" for dataset_id in datasets_ids
        ]
        datasets_columns_nodes = self.__fetch_all_async_responses(
            [RequestArgs("GET", url, None, headers) for url in urls]
        )
        return await datasets_columns_nodes

    async def get_databases(self) -> List[Database]:
        nodes_ids = await self.__get_nodes_list_with_pagination("database")
        headers = await self.__build_headers()
        urls = [
            self.__base_url + f"database/{database['id']}" for database in nodes_ids
        ]
        databases_nodes = await self.__fetch_all_async_responses(
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
        nodes = await self.__get_datasets_columns_nodes(datasets_ids)
        for dataset_node in nodes:
            result = dataset_node["result"]
            datasets_columns.update(
                {
                    result["id"]: [
                        Column(
                            id=column.get("id"),
                            name=column.get("column_name"),
                            remote_type=column.get("type"),
                        )
                        for column in result["columns"]
                    ]
                }
            )
        return datasets_columns
