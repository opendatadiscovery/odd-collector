from dataclasses import dataclass
from typing import Dict, Callable, Union, Any, Iterable, Optional
import logging
import boto3
from more_itertools import chunked, flatten
from odd_models.models import DataEntity
from oddrn_generator import GlueGenerator

from odd_collector.domain.plugin import GluePlugin

from .mappers.columns import map_column_stats
from .mappers.jobs import map_glue_job, map_glue_job_run
from .mappers.tables import map_glue_table

SDK_DATASET_MAX_RESULTS = 1000
SDK_DATASET_COL_STATS_MAX_RESULTS = 100
SDK_DATA_TRANSFORMERS_MAX_RESULTS = 100


@dataclass
class PaginatorConfig:
    op_name: str
    parameters: Dict[str, Union[str, int]]
    page_size: int
    list_fetch_key: str
    mapper: Optional[Callable] = None
    mapper_args: Optional[Dict[str, Any]] = None


class Adapter:
    def __init__(self, config: GluePlugin) -> None:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        self._glue_client = boto3.client(
            "glue",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self._oddrn_generator = GlueGenerator(
            cloud_settings={"region": config.aws_region, "account": account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def get_datasets(self) -> Iterable[DataEntity]:
        logging.info([self.__get_tables(dn) for dn in self.__get_database_names()])
        return flatten([self.__get_tables(dn) for dn in self.__get_database_names()])

    def get_transformers(self) -> Iterable[DataEntity]:
        return self.__fetch_paginator(
            PaginatorConfig(
                op_name="get_jobs",
                parameters={},
                page_size=SDK_DATA_TRANSFORMERS_MAX_RESULTS,
                list_fetch_key="Jobs",
                mapper=map_glue_job,
                mapper_args={"oddrn_generator": self._oddrn_generator},
            )
        )

    def get_transformers_runs(
        self, transformer: DataEntity = None
    ) -> Iterable[DataEntity]:
        if transformer is None:
            return flatten(
                [self.get_transformers_runs(t) for t in self.get_transformers()]
            )

        return self.__fetch_paginator(
            PaginatorConfig(
                op_name="get_job_runs",
                parameters={"JobName": transformer.name},
                page_size=SDK_DATASET_MAX_RESULTS,
                list_fetch_key="JobRuns",
                mapper=map_glue_job_run,
                mapper_args={
                    "oddrn_generator": self._oddrn_generator,
                    "transformer_owner": transformer.owner,
                },
            )
        )

    def __get_database_names(self) -> Iterable[str]:
        return self.__fetch_paginator(
            PaginatorConfig(
                op_name="get_databases",
                parameters={"ResourceShareType": "ALL"},
                page_size=SDK_DATASET_MAX_RESULTS,
                list_fetch_key="DatabaseList",
                mapper=lambda database, _: database["Name"],
            )
        )

    def __get_tables(self, database_name: str) -> Iterable[DataEntity]:
        raw_tables: Iterable[Dict[str, Any]] = self.__fetch_paginator(
            PaginatorConfig(
                op_name="get_tables",
                parameters={"DatabaseName": database_name},
                page_size=SDK_DATASET_MAX_RESULTS,
                list_fetch_key="TableList",
            )
        )

        return [self.__process_table_raw_data(rt) for rt in raw_tables]

    def __fetch_paginator(self, conf: PaginatorConfig) -> Iterable:
        paginator = self._glue_client.get_paginator(operation_name=conf.op_name)

        token = None
        while True:
            sdk_response = paginator.paginate(
                **conf.parameters,
                PaginationConfig={"MaxItems": conf.page_size, "StartingToken": token}
            )

            for entity in sdk_response.build_full_result()[conf.list_fetch_key]:
                yield entity if conf.mapper is None else conf.mapper(
                    entity, conf.mapper_args
                )

            if sdk_response.resume_token is None:
                break

            token = sdk_response.resume_token

    def __process_table_raw_data(self, raw_table_data: Dict[str, Any]) -> DataEntity:
        column_names = [
            c["Name"] for c in raw_table_data["StorageDescriptor"]["Columns"]
        ]

        column_stats = flatten(
            [
                map_column_stats(
                    self.__get_stats_for_columns(cns, raw_table_data)[
                        "ColumnStatisticsList"
                    ]
                )
                for cns in chunked(column_names, SDK_DATASET_COL_STATS_MAX_RESULTS)
            ]
        )

        return map_glue_table(
            raw_table_data,
            {col[0]: col[1] for col in column_stats},
            self._oddrn_generator,
        )

    def __get_stats_for_columns(
        self, column_names: Iterable[str], raw_table_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        return self._glue_client.get_column_statistics_for_table(
            DatabaseName=raw_table_data["DatabaseName"],
            TableName=raw_table_data["Name"],
            ColumnNames=column_names,
        )
