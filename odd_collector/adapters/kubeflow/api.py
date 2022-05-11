import kfp
import logging
from typing import List, Iterable

MAX_PAGE_SIZE = 10


class ApiGetter:
    __PIPELINES = []
    __RUNS = []

    def __init__(self, host: str, namespace: str, cookies: str = None) -> None:
        try:
            self.__namespace = namespace
            self.__cookies = cookies
            if self.__cookies:
                self.__host = f"https://{host}/pipeline"
                self.__client = kfp.Client(host=self.__host, cookies=self.__cookies)
            self.__host = f"{host}/pipeline"
            self.__client = kfp.Client(host=self.__host, namespace=self.__namespace)
        except Exception:
            logging.error("Failed to connect to Kubeflow")
            logging.exception(Exception)

    def get_all_pipelines(self, page_token: str = None) -> List[Iterable]:
        try:
            thread = self.__client.list_pipelines(page_token, page_size=MAX_PAGE_SIZE)
            thread_to_dict = thread.to_dict()
            result = thread_to_dict["pipelines"]
            token = thread_to_dict["next_page_token"]
            self.__PIPELINES.extend(result)
            if token is not None:
                self.get_all_pipelines(token)
            return self.__PIPELINES
        except TypeError:
            logging.warning("Pipelines endpoint response is not returned")
            return []

    def get_all_runs(self, page_token: str = None) -> List[Iterable]:
        try:
            thread = self.__client.list_runs(
                page_token, namespace=self.__namespace, page_size=MAX_PAGE_SIZE
            )
            thread_to_dict = thread.to_dict()
            result = thread_to_dict["runs"]
            token = thread_to_dict["next_page_token"]
            self.__RUNS.extend(result)
            if token is not None:
                self.get_all_runs(token)
            return self.__RUNS
        except TypeError:
            logging.warning("Runs endpoint response is not returned")
            return []
