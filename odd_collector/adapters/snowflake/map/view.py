from copy import deepcopy
from typing import List

import sqlparse
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import SnowflakeGenerator

from odd_collector.adapters.snowflake.domain import View
from odd_collector.adapters.snowflake.logger import logger

from ..domain.entity import Connection
from ..helpers import transform_datetime
from .column import map_columns
from .entity_type_path_key import EntityTypePathKey
from .metadata import map_metadata


def map_view(view: View, generator: SnowflakeGenerator) -> DataEntity:
    generator = deepcopy(generator)
    generator.set_oddrn_paths(schemas=view.table_schema, views=view.table_name)

    sql = None
    try:
        sql = sqlparse.format(view.view_definition)
    except Exception:
        logger.warning(f"Couldn't parse view definition {view.view_definition}")

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.table_name,
        type=DataEntityType.VIEW,
        owner=view.table_owner,
        description=view.view_comment,
        metadata=[map_metadata(view)],
        updated_at=transform_datetime(view.last_altered),
        created_at=transform_datetime(view.created),
        dataset=DataSet(
            field_list=map_columns(view.columns, EntityTypePathKey.VIEW, generator)
        ),
        data_transformer=DataTransformer(
            sql=sql,
            inputs=_map_connection(view.upstream, generator),
            outputs=_map_connection(view.downstream, generator),
        ),
    )


def _map_connection(
    connections: List[Connection], generator: SnowflakeGenerator
) -> List[str]:
    """
    Map list of connections (upstream, downstream) to list of oddrns
    """
    generator = deepcopy(generator)

    result: List[str] = []

    for connection in connections:
        path_key: str = (
            EntityTypePathKey.TABLE
            if connection.domain == "TABLE"
            else EntityTypePathKey.VIEW
        ).value

        generator_params = {
            "databases": connection.table_catalog,
            "schemas": connection.table_schema,
            path_key: connection.table_name,
        }

        generator.set_oddrn_paths(**generator_params)

        result.append(generator.get_oddrn_by_path(path_key))

    return result
