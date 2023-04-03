from abc import ABC, abstractmethod
from typing import Dict, Any
from urllib.parse import urljoin

import aiohttp

from odd_collector.adapters.fivetran.domain.connector import ConnectorMetadata
from odd_collector.adapters.fivetran.domain.destination import DestinationMetadata


class AbstractRepository(ABC):
    @abstractmethod
    async def get_connector_details(self, *args, **kwargs) -> ConnectorMetadata:
        pass

    @abstractmethod
    async def get_destination_details(self, *args, **kwargs) -> DestinationMetadata:
        pass


class FivetranRepository:
    def __init__(self, config):
        self.base_url = config.base_url
        self.destination_id = config.destination_id
        self.connector_id = config.connector_id
        self.auth = aiohttp.BasicAuth(
            login=config.api_key, password=config.api_secret.get_secret_value()
        )

    async def _request(
        self, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        async with aiohttp.ClientSession(
            auth=self.auth, raise_for_status=True
        ) as session:
            url = urljoin(self.base_url, endpoint)
            async with session.get(url, params=params) as response:
                response_json = await response.json()
                return response_json["data"]

    async def get_connector_details(self) -> ConnectorMetadata:
        data = await self._request(f"/v1/connectors/{self.connector_id}")
        return ConnectorMetadata(**data)

    async def get_destination_details(self) -> DestinationMetadata:
        data = await self._request(f"/v1/destinations/{self.destination_id}")
        return DestinationMetadata(**data)
