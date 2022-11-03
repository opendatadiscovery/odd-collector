from aiohttp import ClientSession
from urllib.parse import urlparse
from typing import Dict, Any, List
from odd_collector.domain.plugin import RedashPlugin
from .domain.query import Query
from .domain.datasource import DataSource
from .domain.dashboard import Dashboard
from odd_collector.domain.rest_client.client import RestClient, RequestArgs


class RedashClient(RestClient):
    def __init__(self, config: RedashPlugin):
        self.__config = config
        self.__base_url = config.server + "/api/"

    @property
    def __headers(self) -> Dict[str, str]:
        return {"Authorization": f"Key {self.__config.api_key}"}

    def get_server_host(self) -> str:
        return urlparse(self.__config.server).netloc

    async def __get_nodes_list_with_pagination(self, endpoint: str) -> List[Any]:
        default_page_size = 25

        async def get_result_for_a_page(page: int):
            base_params = {
                "page_size": default_page_size,
                "page": page,
            }
            async with ClientSession() as session:
                response = await self.fetch_async_response(
                    session,
                    RequestArgs(
                        method="GET",
                        url=self.__base_url + endpoint,
                        params=base_params,
                        headers=self.__headers,
                    ),
                )
            return response.get("results")

        return await self.collect_nodes_with_pagination(
            default_page_size, get_result_for_a_page, 1
        )

    async def get_queries(self) -> List[Query]:
        nodes = await self.__get_nodes_list_with_pagination("queries")
        return [Query.from_response(node) for node in nodes]

    async def __get_data_sources_nodes(self) -> Dict[str, Any]:
        async with ClientSession() as session:
            return await self.fetch_async_response(
                session,
                RequestArgs(
                    method="GET",
                    url=self.__base_url + "data_sources",
                    headers=self.__headers,
                ),
            )

    async def get_dashboards(self) -> List[Dashboard]:
        common_nodes = await self.__get_nodes_list_with_pagination("dashboards")
        urls = [
            self.__base_url + f"dashboards/{common_node['slug']}"
            for common_node in common_nodes
        ]
        nodes = await self.fetch_all_async_responses(
            [RequestArgs("GET", url, None, self.__headers) for url in urls]
        )
        return [Dashboard.from_response(node) for node in nodes]

    async def get_data_sources(self) -> List[DataSource]:
        common_nodes = await self.__get_data_sources_nodes()
        urls = [
            self.__base_url + f"data_sources/{datasource_common_node['id']}"
            for datasource_common_node in common_nodes
        ]
        nodes = await self.fetch_all_async_responses(
            [RequestArgs("GET", url, None, self.__headers) for url in urls]
        )
        return [DataSource.from_response(node) for node in nodes]
