from abc import ABC, abstractmethod
from typing import List, Dict, Any
from urllib.parse import urljoin

import requests

from requests.auth import HTTPBasicAuth

from odd_collector.adapters.fivetran.domain.connector import ConnectorMetadata
from odd_collector.adapters.fivetran.domain.destination import DestinationMetadata


class AbstractRepository(ABC):
    @abstractmethod
    def get_connector_details(self, *args, **kwargs) -> ConnectorMetadata:
        pass

    @abstractmethod
    def get_destination_details(self, *args, **kwargs) -> DestinationMetadata:
        pass


class FivetranRepository:
    def __init__(self, config):
        self.base_url = config.base_url
        self.destination_id = config.destination_id
        self.connector_id = config.connector_id
        self.auth = HTTPBasicAuth(config.api_key, config.api_secret.get_secret_value())

    def _request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = urljoin(self.base_url, endpoint)
        with requests.Session() as session:
            response = session.get(url, auth=self.auth, params=params)
            if response.status_code != 200:
                response.raise_for_status()
        return response.json()["data"]

    def get_connector_details(self) -> ConnectorMetadata:
        return ConnectorMetadata(**self._request(f"/v1/connectors/{self.connector_id}"))

    def get_destination_details(self) -> DestinationMetadata:
        return DestinationMetadata(**self._request(f"/v1/destinations/{self.destination_id}"))
