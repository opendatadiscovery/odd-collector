from dataclasses import dataclass, field
from typing import Dict, Union, Callable, Any, Iterable, List

import boto3
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSet,
    DataSetField,
    DataSetFieldType,
)
from oddrn_generator import DynamodbGenerator

from odd_collector.domain.plugin import DynamoDbPlugin
from odd_collector.domain.paginator_config import PaginatorConfig

from .metadata import MetadataExtractor

SDK_DATASET_MAX_RESULTS = 100

"""
@dataclass
class PaginatorConfig:
    op_name: str
    page_size: int = SDK_DATASET_MAX_RESULTS
    payload_key: str = None
    parameters: Dict[str, Union[str, int]] = field(default_factory=dict)
    mapper: Callable = None
    mapper_args: Dict[str, Any] = None
"""

class Adapter:
    __dynamo_types = {"N": "TYPE_NUMBER", "S": "TYPE_STRING", "B": "TYPE_BINARY"}

    def __init__(self, config: DynamoDbPlugin) -> None:
        self.__dynamo_client = boto3.client(
            "dynamodb",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self.__aws_account_id = boto3.client(
            "sts", aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region
        ).get_caller_identity()["Account"]
        self.__exclude_tables = config.exclude_tables
        self.__metadata_extractor = MetadataExtractor()
        self.__oddrn_generator = DynamodbGenerator(
            cloud_settings={
                "region": config.aws_region,
                "account": self.__aws_account_id,
            }
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_datasets(self) -> List[DataEntity]:
        return [
            self.__map_table_from_response(table) for table in self.__fetch_tables()
        ]

    def get_data_transformers(self) -> List[DataEntity]:
        return []

    def get_transformers(self) -> List[DataEntity]:
        return []

    def get_data_transformer_runs(self) -> List[DataEntity]:
        return []

    def get_transformers_runs(self) -> List[DataEntity]:
        return []

    def __fetch_tables(self) -> Iterable[Dict[str, Any]]:
        return [
            self.__dynamo_client.describe_table(TableName=tn)
            for tn in self.__fetch_tables_names()
            if tn not in self.__exclude_tables
        ]

    def __fetch_tables_names(self) -> Iterable:
        return self.__fetch_paginator(
            PaginatorConfig(
                op_name="list_tables",
                payload_key="TableNames",
                page_size = SDK_DATASET_MAX_RESULTS
            )
        )

    def __fetch_paginator(self, conf: PaginatorConfig) -> Iterable:
        paginator = self.__dynamo_client.get_paginator(operation_name=conf.op_name)

        token = None
        while True:
            sdk_response = paginator.paginate(
                **conf.parameters,
                PaginationConfig={"MaxItems": conf.page_size, "StartingToken": token}
            )

            sdk_response_payload = (
                sdk_response.build_full_result()[conf.payload_key]
                if conf.payload_key
                else sdk_response.build_full_result()
            )

            for entity in sdk_response_payload:
                yield entity if conf.mapper is None else conf.mapper(
                    entity, conf.mapper_args
                )

            if sdk_response.resume_token is None:
                break

            token = sdk_response.resume_token

    def __map_table_from_response(self, raw_response: Dict[str, Any]) -> DataEntity:
        raw_table_data = raw_response["Table"]

        return DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path(
                "tables", raw_table_data["TableName"]
            ),
            name=raw_table_data["TableName"],
            type=DataEntityType.TABLE,
            metadata=[
                self.__metadata_extractor.extract_dataset_metadata(raw_table_data)
            ],
            created_at=raw_table_data[
                "CreationDateTime"
            ].isoformat(),  # todo: smthng with dates??
            updated_at=raw_table_data[
                "CreationDateTime"
            ].isoformat(),  # todo: smthng with dates??
            dataset=DataSet(
                rows_number=raw_table_data["ItemCount"],
                field_list=self.__map_fields_from_attributes(
                    raw_table_data["AttributeDefinitions"]
                ),
            ),
        )

    def __map_fields_from_attributes(
        self, raw_attributes: List[Dict[str, Any]]
    ) -> Iterable[DataSetField]:
        return [self.__map_field_from_attribute(a) for a in raw_attributes]

    def __map_field_from_attribute(self, raw_attribute: Dict[str, Any]) -> DataSetField:
        return DataSetField(
            oddrn=self.__oddrn_generator.get_oddrn_by_path(
                "columns", raw_attribute["AttributeName"]
            ),
            name=raw_attribute["AttributeName"],
            type=DataSetFieldType(
                type=self.__dynamo_types[raw_attribute["AttributeType"]],
                logical_type=raw_attribute["AttributeType"],
                is_nullable=False,
            ),
        )
