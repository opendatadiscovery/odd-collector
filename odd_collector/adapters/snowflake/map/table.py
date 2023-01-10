from copy import deepcopy

from odd_models.models import DataEntity, DataEntityType, DataSet

from odd_collector.adapters.snowflake.domain import Table

from ..generator import SnowflakeGenerator
from ..helpers import transform_datetime
from .column import map_columns
from .entity_type_path_key import EntityTypePathKey
from .metadata import map_metadata


def map_table(table: Table, generator: SnowflakeGenerator) -> DataEntity:
    generator = deepcopy(generator)
    generator.set_oddrn_paths(schemas=table.table_schema, tables=table.table_name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        type=DataEntityType.TABLE,
        owner=table.table_owner,
        description=table.table_comment,
        metadata=[map_metadata(table)],
        updated_at=transform_datetime(table.last_altered),
        created_at=transform_datetime(table.created),
        dataset=DataSet(
            rows_number=table.row_count,
            field_list=map_columns(table.columns, EntityTypePathKey.TABLE, generator),
        ),
    )
