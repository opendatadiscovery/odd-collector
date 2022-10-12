import json
import aiohttp
import logging
from typing import List, Optional


class AirbyteApi:
    """
    Class intended for retrieving data from Airbyte API
    """

    def __init__(self, host: str, port: str) -> None:
        self.__base_url = f"http://{host}:{port}"
        self.__headers = {
            "Content-type": "application/json",
            "Accept": "application/json",
        }

    async def get_all_workspaces(self) -> List[str]:
        async with aiohttp.ClientSession(self.__base_url) as session:
            try:
                async with session.post("/api/v1/workspaces/list") as resp:
                    result = await resp.json()
                    workspaces = result["workspaces"]
                return [workspace["workspaceId"] for workspace in workspaces]
            except TypeError:
                logging.warning("Workspaces endpoint response is not returned")
                return []

    async def get_all_connections(self, workspace_ids: List[str]) -> List[dict]:
        connections = []
        async with aiohttp.ClientSession(self.__base_url) as session:
            try:
                for workspace_id in workspace_ids:
                    workspaces_dict = {"workspaceId": workspace_id}
                    request_body = json.dumps(workspaces_dict)
                    async with session.post(
                            "/api/v1/connections/list", data=request_body, headers=self.__headers
                    ) as resp:
                        result = await resp.json()
                        connections.extend(result["connections"])
                return connections
            except TypeError:
                logging.warning("Connections endpoint response is not returned")
                return []

    async def get_dataset_definition(self, is_source: bool, dataset_id: str) -> dict:
        field_name = "sourceId" if is_source else "destinationId"
        body = {field_name: dataset_id}
        url = "/api/v1/sources/get" if is_source else "/api/v1/destinations/get"
        async with aiohttp.ClientSession(self.__base_url) as session:
            try:
                request_body = json.dumps(body)
                async with session.post(
                        url, data=request_body, headers=self.__headers
                ) as resp:
                    result = await resp.json()
                    return result
            except TypeError:
                logging.warning("Dataset endpoint response is not returned")
                return {}


class OddPlatformApi:
    """
    Class intended to retrieve data from ODD API
    """

    def __init__(self, host_url: str) -> None:
        self.__base_url = host_url

    async def get_data_entities_oddrns(self, deg_oddrn: str) -> List[Optional[str]]:
        params = {"deg_oddrn": deg_oddrn}
        entities = []
        async with aiohttp.ClientSession(self.__base_url) as session:
            try:
                async with session.get(
                        "/ingestion/dataentities", params=params
                ) as resp:
                    result = await resp.json()
                    for item in result["items"]:
                        entities.append(item["oddrn"])
                    return entities
            except TypeError:
                logging.warning("Dataset endpoint response is not returned")
                return entities
