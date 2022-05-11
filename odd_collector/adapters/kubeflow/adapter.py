from typing import List
from itertools import chain

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity
from oddrn_generator import KubeflowGenerator

from odd_collector.domain.plugin import KubeflowPlugin
from api import ApiGetter
from .mappers.pipelines import map_pipelines
from .mappers.runs import map_runs


class KubeflowAdapter(AbstractAdapter):
    def __init__(self, config: KubeflowPlugin) -> None:
        self.__host = config.host
        self.__namespace = config.namespace
        if (c0 := config.session_cookie0) or (c1 := config.session_cookie1):
            self.__cookies = f"{c0};{c1}"
            self.__api = ApiGetter(self.__host, self.__namespace, self.__cookies)
        self.__api = ApiGetter(self.__host, self.__namespace)

        self.__oddrn_generator = KubeflowGenerator(host_settings=self.__host)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> List[DataEntity]:
        return list(chain(self.__get_pipelines(), self.__get_runs()))

    def __get_pipelines(self) -> List[DataEntity]:
        all_pipelines = self.__api.get_all_pipelines()
        return [
            map_pipelines(pipeline, self.__oddrn_generator)
            for pipeline in all_pipelines
        ]

    def __get_runs(self) -> List[DataEntity]:
        all_runs = self.__api.get_all_runs()
        return [map_runs(run, self.__oddrn_generator) for run in all_runs]
