from copy import deepcopy

import sqlparse
from odd_models.models import DataEntity, DataEntityType, DataTransformer

from odd_collector.adapters.snowflake.domain import Pipe

from ..generator import SnowflakeGenerator
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
            inputs=["s3"],
            outputs=_map_connection(pipe.downstream, generator),
        ),
    )
