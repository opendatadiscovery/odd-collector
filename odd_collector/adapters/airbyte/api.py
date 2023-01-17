import aiohttp
from typing import List, Optional
from .logger import logger
from odd_models.models import DataEntityType


class AirbyteApi:
    """
    Class intended for retrieving data from Airbyte API
    """

    def __init__(self, host: str, port: str, user: str, password: str) -> None:
        self.__base_url = f"http://{host}:{port}"
        self.__auth = aiohttp.BasicAuth(login=user, password=password)

    async def get_workspaces(self) -> List[str]:
        url = "/api/v1/workspaces/list"
        async with aiohttp.ClientSession(self.__base_url, auth=self.__auth) as session:
            try:
                async with session.post(url) as resp:
                    result = await resp.json()
                    workspaces = result["workspaces"]
                return [workspace["workspaceId"] for workspace in workspaces]
            except TypeError:
                logger.warning("Workspaces endpoint response is not returned")
                return []

    async def get_connections(self, workspace_ids: List[str]) -> List[dict]:
        url = "/api/v1/connections/list"
        connections = []
        async with aiohttp.ClientSession(self.__base_url, auth=self.__auth) as session:
            try:
                for workspace_id in workspace_ids:
                    workspace_dict = {"workspaceId": workspace_id}
                    async with session.post(url, json=workspace_dict) as resp:
                        result = await resp.json()
                        connections.extend(result["connections"])
                logger.info(f"Connections found: {connections}")
                return connections
            except TypeError:
                logger.warning("Connections endpoint response is not returned")
                return []

    async def get_dataset_definition(self, is_source: bool, dataset_id: str) -> dict:
        url = "/api/v1/sources/get" if is_source else "/api/v1/destinations/get"
        field_name = "sourceId" if is_source else "destinationId"
        body = {field_name: dataset_id}
        async with aiohttp.ClientSession(self.__base_url, auth=self.__auth) as session:
            try:
                async with session.post(url, json=body) as resp:
                    result = await resp.json()
                    return result
            except TypeError as e:
                logger.warning(f"Dataset endpoint response is not returned. {e}")
                return {}


class OddPlatformApi:
    """
    Class intended to retrieve data from ODD Platform API
    """

    def __init__(self, host_url: str) -> None:
        self.__base_url = host_url

    async def get_data_entities_oddrns(self, deg_oddrn: str) -> List[Optional[str]]:
        url = "/ingestion/entities/degs/children"
        params = {"oddrn": deg_oddrn}
        entities = []
        async with aiohttp.ClientSession(self.__base_url) as session:
            try:
                async with session.get(url, params=params) as resp:
                    result = await resp.json()
                    logger.info(f"ODD_API response: {result}")
                    for item in result["items"]:
                        if item["type"] == DataEntityType.DATABASE_SERVICE:
                            oddrn = item["oddrn"]
                            return await self.get_data_entities_oddrns(oddrn)
                        else:
                            entities.append(item["oddrn"])
                    return entities
            except TypeError as e:
                logger.warning(f"Dataset endpoint response is not returned. {e}")
                return entities
