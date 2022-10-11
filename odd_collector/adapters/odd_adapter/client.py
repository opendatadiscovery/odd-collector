from abc import ABC, abstractmethod
from datetime import datetime

import aiohttp
from odd_collector_sdk.errors import DataSourceError
from odd_models.models import DataEntityList


class BaseClient(ABC):
    """Client for getting dataentities from external adapter"""

    @abstractmethod
    async def get_data_entities(self):
        raise NotImplementedError


class Client(BaseClient):
    """Implemented client for getting dataentities from external adapter"""

    def __init__(self, config) -> None:
        self.__host = config.host

    async def get_data_entities(self):
        async with aiohttp.ClientSession(self.__host) as session:
            try:
                async with session.get(
                    "/entities", params={"change_since": datetime.now().isoformat()}
                ) as resp:
                    result = await resp.json()
                    return DataEntityList.parse_obj(result)
            except Exception as e:
                raise DataSourceError(
                    f"Error during getting data entities from {self.__host}"
                ) from e
