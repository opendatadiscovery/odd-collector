from abc import ABC, abstractmethod
from typing import List, Dict, Any
from urllib.parse import urljoin

import requests

from odd_collector.adapters.fivetran.domain.table import TableMetadata
from odd_collector.adapters.fivetran.domain.column import ColumnMetadata
from requests.auth import HTTPBasicAuth


class AbstractRepository(ABC):
    @abstractmethod
    def get_tables(self, *args, **kwargs) -> List[TableMetadata]:
        pass

    @abstractmethod
    def get_columns(self, *args, **kwargs) -> List[ColumnMetadata]:
        pass


class FivetranRepository(AbstractRepository):
    def __init__(self, config):
        self.base_url = config.base_url
        self.auth = HTTPBasicAuth(config.api_key, config.api_secret.get_secret_value())
        self.connector_id = config.connector_id
        self.schema = config.schema

    def _request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = urljoin(self.base_url, endpoint)
        with requests.Session() as session:
            response = session.get(url, auth=self.auth, params=params)
            if response.status_code != 200:
                response.raise_for_status()
        return response.json()["data"]

    def get_db_name(self) -> str:
        res = self._request(f"/v1/connectors/{self.connector_id}")
        return res["config"]["database"]

    # We can only get metadata if Fivetran initial sync between connector and destination was performed.
    def get_tables(self) -> List[TableMetadata]:
        res = self._request(f"/v1/metadata/connectors/{self.connector_id}/tables")
        return [TableMetadata(**table) for table in res["items"]]

    def get_columns(self) -> List[ColumnMetadata]:
        res = self._request(f"/v1/metadata/connectors/{self.connector_id}/columns")
        return [ColumnMetadata(**column) for column in res["items"]]
