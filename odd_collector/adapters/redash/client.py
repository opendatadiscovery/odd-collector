from aiohttp import ClientSession
from asyncio import gather
from urllib.parse import urlparse
from typing import NamedTuple, Tuple, Optional, Dict, Any, List
from odd_collector.domain.plugin import RedashPlugin
from .domain.query import Query
from .domain.datasource import DataSource
from .domain.dashboard import Dashboard


class RequestArgs(NamedTuple):
    method: str
    url: str
    params: Optional[Dict[Any, Any]] = None
    headers: Optional[Dict[Any, Any]] = None
    payload: Optional[Dict[Any, Any]] = None


class RedashClient:
    def __init__(self, config: RedashPlugin):
        self.__config = config
        self.__base_url = config.server + "/api/"

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
        async with ClientSession() as session:
            return await gather(
                *[
                    self.__fetch_async_response(session, request_args=request_args)
                    for request_args in request_args_list
                ],
                return_exceptions=True,
            )

    @property
    def __headers(self) -> Dict[str, str]:
        return {"Authorization": f"Key {self.__config.api_key}"}

    def get_server_host(self) -> str:
        return urlparse(self.__config.server).netloc

    async def __get_nodes_list_with_pagination(
            self, endpoint: str
    ) -> List[Any]:
        default_page_size = 25

        async def get_result_for_a_page(page: int):
            base_params = {
                "page_size": default_page_size,
                "page": page,
            }
            async with ClientSession() as session:
                response = await self.__fetch_async_response(
                    session,
                    RequestArgs(
                        method="GET",
                        url=self.__base_url + endpoint,
                        params=base_params,
                        headers=self.__headers,
                    ),
                )
            return response.get("results")

        nodes_list = []
        pg = 1
        results_len = default_page_size
        while results_len == default_page_size:
            result = await get_result_for_a_page(pg)
            nodes_list += result
            results_len = len(result)
            pg += 1
        return nodes_list

    async def get_queries(self) -> List[Query]:
        nodes = await self.__get_nodes_list_with_pagination(
            "queries"
        )
        return [Query.from_response(node) for node in nodes]

    async def __get_data_sources_nodes(self) -> Dict[str, Any]:
        async with ClientSession() as session:
            return await self.__fetch_async_response(
                session,
                RequestArgs(
                    method="GET",
                    url=self.__base_url + 'data_sources',
                    headers=self.__headers
                ),
            )

    async def get_dashboards(self) -> List[Dashboard]:
        common_nodes = await self.__get_nodes_list_with_pagination("dashboards")
        urls = [
            self.__base_url + f"dashboards/{common_node['slug']}" for common_node in common_nodes
        ]
        nodes = await self.__fetch_all_async_responses(
            [RequestArgs("GET", url, None, self.__headers) for url in urls]
        )
        return [Dashboard.from_response(node) for node in nodes]

    async def get_data_sources(self) -> List[DataSource]:
        common_nodes = await self.__get_data_sources_nodes()
        urls = [
            self.__base_url + f"data_sources/{datasource_common_node['id']}" for datasource_common_node in common_nodes
        ]
        nodes = await self.__fetch_all_async_responses(
            [RequestArgs("GET", url, None, self.__headers) for url in urls]
        )
        return [DataSource.from_response(node) for node in nodes]
