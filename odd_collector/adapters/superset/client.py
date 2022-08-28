from typing import List, Dict
from urllib.parse import urlparse
from .domain.column import Column
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

    def __build_headers(self):
        return {'Authorization': 'Bearer ' + self.__get_access_token()}

    def __query(self, method: str, endpoint: str, payload: Dict[str, str] = None, headers: Dict[str, str] = None):
        return requests.request(method, self.__base_url + endpoint, json=payload, headers=headers)

    def get_server_host(self):
        return urlparse(self.__config.server).netloc

    def get_datasets(self):
        resp = self.__query('GET', 'dataset', headers=self.__build_headers())
        decoded = json.loads(resp.content)['result']
        return [Dataset(id=dataset.get('id'),
                        name=dataset.get('table_name'),
                        db_id=dataset.get('database').get('id'),
                        db_name=dataset.get('database').get('database_name')
                        ) for dataset in decoded]

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

