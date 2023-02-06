import logging
from abc import abstractmethod
from copy import deepcopy
from typing import List

import sqlparse
from odd_models.models import DataEntity, DataEntityType, DataTransformer
from oddrn_generator import SnowflakeGenerator
from oddrn_generator.generators import S3Generator

from odd_collector.adapters.snowflake.domain import Pipe

from .view import _map_connection

logger = logging.getLogger("Snowpipe")


def map_pipe(pipe: Pipe, generator: SnowflakeGenerator) -> DataEntity:
    generator = deepcopy(generator)
    generator.set_oddrn_paths(pipes=pipe.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("pipes"),
        name=pipe.name,
        type=DataEntityType.JOB,
        data_transformer=DataTransformer(
            sql=sqlparse.format(pipe.definition),
            inputs=find_input(pipe),
            outputs=_map_connection(pipe.downstream, generator),
        ),
    )


class PipeInputStrategy:
    def __init__(self, pipe_url: str):
        self.url = self.__remove_slash_if_exists(pipe_url)

    @abstractmethod
    def get_input(self) -> List[str]:
        pass

    @staticmethod
    def __remove_slash_if_exists(line: str) -> str:
        if line[-1] == "/":
            new_line = line[:-1]
        else:
            new_line = line
        return new_line


class AwsStrategy(PipeInputStrategy):
    def get_input(self):
        gen = S3Generator().from_s3_url(self.url)
        return [gen.get_oddrn_by_path("keys")]


class GcpStrategy(PipeInputStrategy):
    def get_input(self):
        logger.warning("GCP is Not implemented yet")
        return [self.url]


class AzureStrategy(PipeInputStrategy):
    def get_input(self):
        logger.warning("Azure is Not implemented yet")
        return [self.url]


def find_input(pipe: Pipe) -> List[str]:
    if "internal" in pipe.stage_type.lower():
        return ["INTERNAL_SNOWFLAKE_STORAGE"]
    else:
        url = pipe.stage_url
        if ("s3gov://" in url) or ("s3://" in url):
            strategy = AwsStrategy
        elif "gcs://" in url:
            strategy = GcpStrategy
        elif "azure://" in url:
            strategy = AzureStrategy

        else:
            logger.warning("Cloud provider wasn't detected")
            return [url]

    return strategy(url).get_input()
