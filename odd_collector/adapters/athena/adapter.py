from dataclasses import dataclass
from typing import Dict, Callable, Union, Any, Iterable, Optional, List

import boto3
from more_itertools import flatten
from odd_models.models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import AthenaGenerator
from odd_collector.domain.adapter import AbstractAdapter

from odd_collector.domain.plugin import AthenaPlugin
from odd_collector.domain.paginator_config import PaginatorConfig

from .mappers.tables import map_athena_table

SDK_DATASET_MAX_RESULTS = 1000
SDK_DATASET_COL_STATS_MAX_RESULTS = 100
SDK_DATA_TRANSFORMERS_MAX_RESULTS = 100


class Adapter(AbstractAdapter):
    def __init__(self, config: AthenaPlugin) -> None:
        account_id = boto3.client(
            "sts",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        ).get_caller_identity()["Account"]
        self._athena_client = boto3.client(
            "athena",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self._oddrn_generator = AthenaGenerator(
            cloud_settings={"region": config.aws_region, "account": account_id}
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> Iterable[DataEntity]:
        return flatten(
            [
                self.__get_table_metadata(cn, dn)
                for cn in self.__get_catalog_names()
                for dn in self.__get_database_names(cn)
            ]
        )

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(self.get_data_entities()),
        )

    def __get_catalog_names(self) -> Iterable[str]:
        return self.__fetch_paginator(
            PaginatorConfig(
                op_name="list_data_catalogs",
                parameters={},
                page_size=SDK_DATASET_MAX_RESULTS,
                list_fetch_key="DataCatalogsSummary",
                mapper=lambda catalog, _: catalog["CatalogName"],
            )
        )

    def get_transformers(self) -> List[DataEntity]:
        return []

    def get_transformers_runs(self) -> List[DataEntity]:
        return []

    def __get_database_names(self, catalog_name: str) -> Iterable[str]:
        return self.__fetch_paginator(
            PaginatorConfig(
                op_name="list_databases",
                parameters={"CatalogName": catalog_name},
                page_size=SDK_DATASET_MAX_RESULTS,
                list_fetch_key="DatabaseList",
                mapper=lambda database, _: database["Name"],
            )
        )

    def __get_table_metadata(
            self, catalog_name: str, database_name: str
    ) -> Iterable[DataEntity]:
        raw_tables: Iterable[Dict[str, Any]] = self.__fetch_paginator(
            PaginatorConfig(
                op_name="list_table_metadata",
                parameters={"CatalogName": catalog_name, "DatabaseName": database_name},
                page_size=SDK_DATASET_MAX_RESULTS,
                list_fetch_key="TableMetadataList",
            )
        )

        return [
            self.__process_table_raw_data(rt, catalog_name, database_name)
            for rt in raw_tables
        ]

    def __fetch_paginator(self, conf: PaginatorConfig) -> Iterable:
        paginator = self._athena_client.get_paginator(operation_name=conf.op_name)

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

    def __process_table_raw_data(
            self, raw_table_data: Dict[str, Any], catalog_name: str, database_name: str
    ) -> DataEntity:
        return map_athena_table(
            raw_table_data, catalog_name, database_name, self._oddrn_generator
        )
