from copy import deepcopy
from typing import List
import sqlparse
from oddrn_generator.generators import S3Generator
from odd_models.models import DataEntity, DataEntityType, DataTransformer
from oddrn_generator import SnowflakeGenerator

from odd_collector.adapters.snowflake.domain import Pipe

from .view import _map_connection


def map_pipe(pipe: Pipe, generator: SnowflakeGenerator) -> DataEntity:
    generator = deepcopy(generator)
    generator.set_oddrn_paths(pipes=pipe.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("pipes"),
        name=pipe.name,
        type=DataEntityType.JOB,
        data_transformer=DataTransformer(
            sql=sqlparse.format(pipe.definition),
            # TODO add cloud part to SQL
            inputs=find_input(pipe),
            outputs=_map_connection(pipe.downstream, generator),
        ),
    )


def find_input(pipe: Pipe) -> List[str]:
    if 'internal' in pipe.stage_type.lower():
        return ['INTERNAL_SNOWFLAKE_STORAGE']
    else:
        url = pipe.stage_url
        if ('s3gov://' in url) or ('s3://' in url):
            pass


