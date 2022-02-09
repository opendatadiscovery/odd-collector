import logging
from dataclasses import dataclass
from typing import Dict, Callable, Union, Any, Iterable, Optional, List

from odd_aws_collector.domain.plugin import QuicksightPlugin
from odd_aws_collector.domain.paginator_config import PaginatorConfig

import boto3
from more_itertools import flatten
from odd_models.models import DataEntity
from odd_models.models import DataEntityList

from oddrn_generator import QuicksightGenerator

from .mappers.datasets import map_quicksight_dataset
from .mappers.dashboards import map_quicksight_dashboard
from .mappers.analysis import map_quicksight_analysis
from .mappers.data_sources import map_quicksight_data_sources

SDK_DATASET_MAX_RESULTS = 1000
SDK_DATASET_COL_STATS_MAX_RESULTS = 100
SDK_DATA_TRANSFORMERS_MAX_RESULTS = 100


class Adapter:
    def __init__(self, config: QuicksightPlugin) -> None:
        self._account_id = boto3.client("sts",
                                        aws_access_key_id=config.aws_access_key_id,
                                        aws_secret_access_key=config.aws_secret_access_key).get_caller_identity()[
            "Account"]
        self._region_name = config.aws_region
        self._quicksight_client = boto3.client(
            "quicksight",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self._oddrn_generator = QuicksightGenerator(
            cloud_settings={"region": self._region_name , "account": self._account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()
    """
    def get_datasets(self) -> Iterable[DataEntity]:
        result = []
        for i in self.__get_dataset():
            try:
                result.append(self.__describe_data_set(i))
            except self._quicksight_client.exceptions.InvalidParameterValueException:
                logging.warning(f'Could not process dataset: {i}')
        return flatten(result)
    """
    def get_data_entities(self) -> Iterable[DataEntity]:
        result = []
        for i in self.__get_dataset():
            try:
                result.append(self.__describe_data_set(i))
            except self._quicksight_client.exceptions.InvalidParameterValueException:
                logging.warning(f'Could not process dataset: {i}')
        return flatten(result)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(self.get_data_entities()),
        )

    def get_dashboard(self) -> Iterable[DataEntity]:
        return flatten([self.__describe_dashboard(dashboard_id) for dashboard_id in self.__get_dashboard()])

    def get_analysis(self) -> Iterable[DataEntity]:
        return flatten([self.__describe_analysis(analysis_id) for analysis_id in self.__get_analysis()])

    def get_transformers(self) -> List[DataEntity]:
        return []

    def get_transformers_runs(self) -> List[DataEntity]:
        return []

    def __get_dataset(self) -> Iterable[str]:
        return self.__fetch_paginator(PaginatorConfig(
            op_name='list_data_sets',
            parameters={'AwsAccountId': self._account_id},
            page_size=SDK_DATASET_MAX_RESULTS,
            list_fetch_key='DataSetSummaries',
            mapper=lambda dataset, _: dataset['DataSetId']
        ))

    def __describe_data_set(self, dataset_id: str):
        raw_datasets: Dict[str, Any] = \
            self._quicksight_client.describe_data_set(
                AwsAccountId=self._account_id,
                DataSetId=dataset_id
            )['DataSet']
        return [self.__process_dataset_raw_data(raw_datasets)]

    def __get_ingestions(self, dataset_id: str):
        return self.__fetch_paginator(PaginatorConfig(
            op_name='list_ingestions',
            parameters={
                'AwsAccountId': self._account_id,
                'DataSetId': dataset_id
            },
            page_size=SDK_DATASET_MAX_RESULTS,
            list_fetch_key='Ingestions',
        ))

    def __get_dashboard(self) -> Iterable[str]:
        return self.__fetch_paginator(PaginatorConfig(
            op_name='list_dashboards',
            parameters={'AwsAccountId': self._account_id},
            page_size=SDK_DATASET_MAX_RESULTS,
            list_fetch_key='DashboardSummaryList',
            mapper=lambda dataset, _: dataset['DashboardId']
        ))

    def __describe_dashboard(self, dashboard_id: str):
        raw_dashboards: Dict[str, Any] = \
            self._quicksight_client.describe_dashboard(
                AwsAccountId=self._account_id,
                DashboardId=dashboard_id
            )['Dashboard']
        return [self.__process_dashboard_raw_data(raw_dashboards)]

    def __get_analysis(self) -> Iterable[str]:
        return self.__fetch_paginator(PaginatorConfig(
            op_name='list_analyses',
            parameters={'AwsAccountId': self._account_id},
            page_size=SDK_DATASET_MAX_RESULTS,
            list_fetch_key='AnalysisSummaryList',
            mapper=lambda dataset, _: dataset['AnalysisId']
        ))

    def __describe_analysis(self, analysis_id: str):
        raw_analysis: Dict[str, Any] = \
            self._quicksight_client.describe_analysis(
                AwsAccountId=self._account_id,
                AnalysisId=analysis_id
            )['Analysis']
        return [self.__process_analysis_raw_data(raw_analysis)]

    def __get_data_sources(self) -> Iterable[str]:
        return self.__fetch_paginator(PaginatorConfig(
            op_name='list_data_sources',
            parameters={'AwsAccountId': self._account_id},
            page_size=SDK_DATASET_MAX_RESULTS,
            list_fetch_key='DataSources',
            mapper=lambda dataset, _: dataset['DataSourceId']
        ))

    def __fetch_paginator(self, conf: PaginatorConfig) -> Iterable:
        paginator = self._quicksight_client.get_paginator(operation_name=conf.op_name)

        token = None
        while True:
            sdk_response = paginator.paginate(
                **conf.parameters,
                PaginationConfig={
                    'MaxItems': conf.page_size,
                    'StartingToken': token
                }
            )

            for entity in sdk_response.build_full_result()[conf.list_fetch_key]:
                yield entity if conf.mapper is None else conf.mapper(entity, conf.mapper_args)

            if sdk_response.resume_token is None:
                break

            token = sdk_response.resume_token

    def __process_dataset_raw_data(self, raw_dataset_data: Dict[str, Any]) -> DataEntity:
        return map_quicksight_dataset(raw_dataset_data,
                                      self._account_id,
                                      self._region_name,
                                      self._quicksight_client)

    def __process_dashboard_raw_data(self, raw_dashboard_data: Dict[str, Any]) -> DataEntity:
        return map_quicksight_dashboard(raw_dashboard_data, self._account_id, self._region_name)

    def __process_analysis_raw_data(self, raw_analysis_data: Dict[str, Any]) -> DataEntity:
        return map_quicksight_analysis(raw_analysis_data, self._account_id, self._region_name)

    def __process_data_sources_raw_data(self, raw_data_source_data: Dict[str, Any]) -> DataEntity:
        return map_quicksight_data_sources(raw_data_source_data, self._account_id, self._region_name)
