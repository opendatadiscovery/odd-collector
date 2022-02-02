import logging
from dataclasses import dataclass
from typing import List, Dict, Union, Iterable, Any

import boto3
from odd_models.models import DataEntity
from oddrn_generator.generators import S3Generator

from odd_collector.domain.plugin import S3Plugin

from odd_s3_adapter.mapper.dataset import map_dataset
from odd_s3_adapter.schema.s3_parquet_schema_retriever import S3ParquetSchemaRetriever

SDK_LIST_OBJECTS_MAX_RESULTS = 1000
DATA_EXTENSIONS = ['.csv', '.parquet']


@dataclass
class PaginatorConfig:
    op_name: str
    parameters: Dict[str, Union[str, int]]
    page_size: int
    list_fetch_key: str


def is_data_file(filepath: str) -> bool:
    for ext in DATA_EXTENSIONS:
        if filepath.endswith(ext):
            return True

    return False


class Adapter:
    def __init__(self, config: S3Plugin) -> None:
        self.__s3_client  = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )
        self.__buckets = config.buckets
        account_id = boto3.client("sts",
                                  aws_access_key_id=config.aws_access_key_id,
                                  aws_secret_access_key=config.aws_secret_access_key).get_caller_identity()["Account"]
        self.__oddrn_generator = S3Generator(
            cloud_settings={"region": config.aws_region, "account": account_id}
        )
        self.__schema_retriever = S3ParquetSchemaRetriever(
                    aws_access_key_id=config.aws_access_key_id,
                    aws_secret_access_key=config.aws_secret_access_key,
                    aws_region=config.aws_region
                )


    def get_datasets(self) -> Iterable[DataEntity]:
        bucket_response = self.__s3_client.list_buckets()
        logging.info(bucket_response)
        logging.info(self.__buckets)
        files_dict: Dict[str, List[Dict[str, Any]]] = {
            bucket['Name']: self.__list_dataset_files(bucket['Name'])
            for bucket in bucket_response['Buckets']
            if bucket['Name'] in self.__buckets
        }

        for bucket, files in files_dict.items():
            self.__oddrn_generator.set_oddrn_paths(buckets=bucket)

            for file in files:
                if not file["Key"].endswith('.parquet'):
                    continue

                yield map_dataset(
                    file=file,
                    schema=self.__schema_retriever.get_schema(f'{bucket}/{file["Key"]}'),
                    oddrn_gen=self.__oddrn_generator
                )
    """           
    def get_entities(self) -> Iterable[DataEntity]:
        bucket_response = self.__s3_client.list_buckets()
        logging.info(bucket_response)
        files_dict: Dict[str, List[Dict[str, Any]]] = {
            bucket['Name']: self.__list_dataset_files(bucket['Name'])
            for bucket in bucket_response['Buckets']
            if bucket['Name'] in self.__buckets
        }

        for bucket, files in files_dict.items():
            self.__oddrn_generator.set_oddrn_paths(buckets=bucket)

            for file in files:
                if not file["Key"].endswith('.parquet'):
                    continue

                yield map_dataset(
                    file=file,
                    schema=self.__schema_retriever.get_schema(f'{bucket}/{file["Key"]}'),
                    oddrn_gen=self.__oddrn_generator
                )
    """
    def get_data_transformers(self) -> List[DataEntity]:
        return []

    def get_transformers(self) -> List[DataEntity]:
        return []

    def get_data_transformer_runs(self) -> List[DataEntity]:
        return []

    def get_transformers_runs(self) -> List[DataEntity]:
        return []

    def __retrieve_parquet_paths(self, bucket_names: List[Dict[str, Any]]) -> List[str]:
        files_dict: Dict[str, List[Dict[str, Any]]] = {
            bucket['Name']: self.__list_dataset_files(bucket['Name'])
            for bucket in bucket_names
        }

        parquet_s3_paths = []
        for bucket_name, files in files_dict.items():
            for file in files:
                if file['Key'].endswith('.parquet'):
                    parquet_s3_paths.append(f"{bucket_name}/{file['Key']}")

        return parquet_s3_paths

    def __list_dataset_files(self, bucket_name: str) -> List[Dict[str, Any]]:
        files_gen = self.__fetch_paginator(
            PaginatorConfig(
                op_name='list_objects_v2',
                parameters={'Bucket': bucket_name, 'FetchOwner': True},
                page_size=SDK_LIST_OBJECTS_MAX_RESULTS,
                list_fetch_key='Contents'
            )
        )

        files = [file for file in files_gen if is_data_file(file['Key'])]

        logging.debug(f"Got {len(files)} dataset files for {bucket_name}")

        return files

    def __fetch_paginator(self, conf: PaginatorConfig) -> Iterable[Dict[str, Any]]:
        paginator = self.__s3_client.get_paginator(operation_name=conf.op_name)

        token = None
        while True:
            sdk_response = paginator.paginate(
                **conf.parameters,
                PaginationConfig={
                    'MaxItems': conf.page_size,
                    'StartingToken': token
                }
            )

            for entity in sdk_response.build_full_result().get(conf.list_fetch_key, []):
                yield entity

            if sdk_response.resume_token is None:
                break

            token = sdk_response.resume_token
