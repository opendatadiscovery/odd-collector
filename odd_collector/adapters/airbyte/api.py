import json
import requests
import logging
from typing import List


class ApiGetter:
    """Class intended for retrieving data from Airbyte API"""

    __WORKSPACE_IDS = []
    __CONNECTIONS = []

    def __init__(self, host: str, port: int) -> None:
        self.__base_url = f"http://{host}:{port}/api/v1"
        self.__workspaces_request = f"{self.__base_url}/workspaces/list"
        self.__connections_request = f"{self.__base_url}/connections/list"
        self.__source_definition_request = f"{self.__base_url}/sources/get"
        self.__destination_definition_request = f"{self.__base_url}/destinations/get"
        self.__headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }

    def get_all_workspaces(self) -> List[str]:
        try:
            response = requests.post(self.__workspaces_request).json()
            workspaces = response['workspaces']
            for workspace in workspaces:
                self.__WORKSPACE_IDS.append(workspace['workspaceId'])
            return self.__WORKSPACE_IDS

        except TypeError:
            logging.warning("Workspaces endpoint response is not returned")
            return []

    def get_all_connections(self, workspace_ids: List[str]) -> List[dict]:
        workspaces_dict = {}
        try:
            for workspace_id in workspace_ids:
                workspaces_dict["workspaceId"] = workspace_id
                request_body = json.dumps(workspaces_dict)
                response = requests.post(url=self.__connections_request, data=request_body,
                                         headers=self.__headers).json()
                self.__CONNECTIONS.extend(response["connections"])
            return self.__CONNECTIONS
        except TypeError:
            logging.warning("Connections endpoint response is not returned")
            return []

    def get_dataset_definition(self, is_source: bool, dataset_id: str) -> dict:
        field_name = 'sourceId' if is_source else 'destinationId'
        body = {field_name: dataset_id}
        url = self.__source_definition_request if is_source else self.__destination_definition_request
        try:
            request_body = json.dumps(body)
            response = requests.post(url=url, data=request_body,
                                     headers=self.__headers).json()
            return response
        except TypeError:
            logging.warning("Dataset endpoint response is not returned")
            return {}
