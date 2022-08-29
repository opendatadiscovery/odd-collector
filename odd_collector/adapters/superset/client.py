from typing import List, Dict, Union, Any
from urllib.parse import urlparse
from .domain.column import Column
from .domain.chart import Chart
from .domain.dashboard import Dashboard
from odd_collector.domain.plugin import SupersetPlugin
from .domain.dataset import Dataset
import json

import requests


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

    def _get_dashboards_ids_names_by_chart_id(self, chart_id: int) -> Dict[int, str]:
        resp = self.__query('GET', f'chart/{chart_id}', headers=self.__build_headers())
        decoded = json.loads(resp.content)['result']
        dashboards_node: List[Dict[str, str]] = decoded.get('dashboards')
        return {dashboard['id']: dashboard['dashboard_title'] for dashboard in dashboards_node}

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

    def _get_charts(self) -> List[Chart]:
        chart_nodes = self._get_nodes_list_with_pagination('chart', ["datasource_id", "id"])
        return [Chart(id=chart_node.get('id'),
                      dataset_id=chart_node.get('datasource_id'),
                      dashboards_ids_names=self._get_dashboards_ids_names_by_chart_id(chart_node.get('id'))

                      ) for chart_node in chart_nodes]

    def get_dashboards(self) -> List[Dashboard]:
        charts = self._get_charts()
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

    def _get_dataset_columns(self, dataset_id: int) -> List[Column]:
        resp = self.__query('GET', f'dataset/{dataset_id}', headers=self.__build_headers())
        decoded = json.loads(resp.content)['result']['columns']
        return [Column(id=column.get('id'),
                       name=column.get('column_name'),
                       remote_type=column.get('type')
                       ) for column in decoded]

    def get_datasets_columns(self, datasets_ids: List[int]) -> Dict[int, List[Column]]:
        datasets_columns: Dict[int, List[Column]] = {}
        for dataset_id in datasets_ids:
            datasets_columns.update({dataset_id: self._get_dataset_columns(dataset_id)})
        return datasets_columns
