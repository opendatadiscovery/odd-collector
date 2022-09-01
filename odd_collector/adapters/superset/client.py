from typing import List, Dict, Union, Any, NamedTuple, Optional
from urllib.parse import urlparse
from .domain.column import Column
from .domain.chart import Chart
from .domain.dashboard import Dashboard
from odd_collector.domain.plugin import SupersetPlugin
from .domain.dataset import Dataset
import json

import requests
import asyncio
import aiohttp


class RequestArgs(NamedTuple):
    url: str
    params: Optional[Dict[Any, Any]]
    headers: Optional[Dict[Any, Any]]


class SupersetClient:
    def __init__(self, config: SupersetPlugin):
        self.__config = config
        self.__base_url = config.server + '/api/v1/'

    def __get_access_token(self) -> str:
        payload = {
            'username': self.__config.username,
            'password': self.__config.password,
            'provider': 'db'
        }
        r = self.__query('POST', 'security/login', payload)
        return json.loads(r.content)['access_token']

    @staticmethod
    async def __fetch_async_response(session, request_args: RequestArgs):
        async with session.get(url=request_args.url, params=request_args.params, headers=request_args.headers) \
                as response:
            return await response.json()

    async def __fetch_all_async_responses(self, request_args_list: List[RequestArgs]) -> tuple:
        async with aiohttp.ClientSession() as session:
            return await asyncio.gather(
                *[self.__fetch_async_response(session, request_args=request_args)
                  for request_args in request_args_list],
                return_exceptions=True)

    def __build_headers(self) -> Dict[str, str]:
        return {'Authorization': 'Bearer ' + self.__get_access_token()}

    def __query(self, method: str, endpoint: str, payload: Dict[str, Union[str, int]] = None,
                headers: Dict[str, str] = None, params: Dict[str, Union[str, int]] = None) \
            -> requests.Response:
        return requests.request(method,
                                self.__base_url + endpoint,
                                json=payload, headers=headers,
                                params=params
                                )

    def get_server_host(self) -> str:
        return urlparse(self.__config.server).netloc

    def get_datasets(self) -> List[Dataset]:
        dataset_nodes = self._get_nodes_list_with_pagination('dataset')
        return [Dataset(id=dataset.get('id'),
                        name=dataset.get('table_name'),
                        db_id=dataset.get('database').get('id'),
                        db_name=dataset.get('database').get('database_name'),
                        kind=dataset.get('kind')
                        ) for dataset in dataset_nodes]

    async def __get_dashboard_nodes_by_chart_ids(self, chart_ids: List[int]):
        headers = self.__build_headers()
        urls = [self.__base_url + f'chart/{chart_id}' for chart_id in chart_ids]
        dashboard_nodes = self.__fetch_all_async_responses(
            [RequestArgs(url, None, headers) for url in urls])
        return await dashboard_nodes

    def _get_nodes_list_with_pagination(self, endpoint: str, columns: List[str] = None) -> List[Any]:
        default_page_size = 100

        def get_result_for_a_page(page: int):
            base_q = {"page_size": default_page_size, "order_direction": "asc", "page": page}
            if columns is not None:
                base_q.update({"columns": columns})
            decoded = json.loads(self.__query('GET', endpoint, None, self.__build_headers(),
                                              {'q': json.dumps(base_q)}).content)
            return decoded['result']

        nodes_list = []
        pg = 0
        results_len = default_page_size
        while results_len == default_page_size:
            result = get_result_for_a_page(pg)
            nodes_list += result
            results_len = len(result)
            pg += 1
        return nodes_list

    async def _get_charts(self) -> List[Chart]:
        chart_nodes = self._get_nodes_list_with_pagination('chart', ["datasource_id", "id"])
        chart_ids = [chart_node.get('id') for chart_node in chart_nodes]
        chart_nodes_with_dashboards = await self.__get_dashboard_nodes_by_chart_ids(chart_ids)
        nodes_with_chart_ids: Dict[int, Dict[int, str]] = {}
        for chart_node in chart_nodes_with_dashboards:
            nodes_with_chart_ids.update({chart_node.get("id"):
                                             {dashboard_node['id']: dashboard_node['dashboard_title']
                                              for dashboard_node in chart_node['result']['dashboards']}})

        return [Chart(id=chart_node.get('id'),
                      dataset_id=chart_node.get('datasource_id'),
                      dashboards_ids_names=nodes_with_chart_ids.get(chart_node.get('id'))

                      ) for chart_node in chart_nodes]

    async def get_dashboards(self) -> List[Dashboard]:
        return self._extract_dashboards_from_charts(await self._get_charts())

    @staticmethod
    def _extract_dashboards_from_charts(charts: List[Chart]) -> List[Dashboard]:
        unique_dashboard_ids_names: Dict[int, str] = {}
        for chart in charts:
            unique_dashboard_ids_names.update(chart.dashboards_ids_names)
        dashboards: List[Dashboard] = []
        for dashboard_id, dashboard_name in unique_dashboard_ids_names.items():
            dashboard = Dashboard(id=dashboard_id,
                                  name=dashboard_name,
                                  datasets_ids=set([chart.dataset_id for chart in charts if
                                                    dashboard_id in chart.dashboards_ids_names.keys()])
                                  )
            dashboards.append(dashboard)

        return dashboards

    async def __get_datasets_columns_nodes(self, datasets_ids: List[int]):
        headers = self.__build_headers()
        urls = [self.__base_url + f'dataset/{dataset_id}' for dataset_id in datasets_ids]
        datasets_columns_nodes = self.__fetch_all_async_responses(
            [RequestArgs(url, None, headers) for url in urls])
        return await datasets_columns_nodes

    async def get_datasets_columns(self, datasets_ids: List[int]) -> Dict[int, List[Column]]:
        datasets_columns: Dict[int, List[Column]] = {}
        nodes = await self.__get_datasets_columns_nodes(datasets_ids)
        for dataset_node in nodes:
            result = dataset_node['result']
            datasets_columns.update({result['id']: [Column(id=column.get('id'),
                                                           name=column.get('column_name'),
                                                           remote_type=column.get('type')
                                                           ) for column in result['columns']]})
        return datasets_columns
