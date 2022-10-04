import json
import requests
import logging
from typing import List, Iterable

MAX_PAGE_SIZE = 10


class ApiGetter:
    __WORKSPACE_IDS = []
    __CONNECTIONS = []

    def __init__(self, host: str) -> None:
        self.__workspaces_request = f"http://{host}/api/v1/workspaces/list"
        self.__connections_request = f"http://{host}/api/v1/connections/list"
        self.__source_definition_request = f"http://{host}/api/v1/sources/get"
        self.__destination_definition_request = f"http://{host}/api/v1/destinations/get"
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
            logging.warning("Pipelines endpoint response is not returned")
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
            logging.warning("Runs endpoint response is not returned")
            return []

    # def get_dataset_definition(self, is_source: bool, connections: List[dict]) -> dict:
    #     body_dict = {}
    #     url = self.__source_definition_request if is_source else self.__destination_definition_request
    #     try:
    #         body_dict['sourceId'] = connections[0]['sourceId']
    #         request_body = json.dumps(body_dict)
    #         response = requests.post(url=url, data=request_body,
    #                                  headers=self.__headers).json()
    #         return response
    #     except TypeError:
    #         logging.warning("Runs endpoint response is not returned")
    #         return {}
