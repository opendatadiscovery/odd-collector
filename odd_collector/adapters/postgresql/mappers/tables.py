from typing import List

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import PostgresqlGenerator

from odd_collector.logger import logger

from ..models import Table
from .columns import map_column
from .views import map_view


def _map_table(generator: PostgresqlGenerator, table: Table):
    data_entity_type = DataEntityType.TABLE
    generator.set_oddrn_paths(
        **{"schemas": table.table_schema, "tables": table.table_name}
    )
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        type=data_entity_type,
        owner=table.table_owner,
        description=table.description,
        metadata=[extract_metadata("postgres", table, DefinitionType.DATASET)],
        dataset=DataSet(
            rows_number=int(table.table_rows) if table.table_rows is not None else None,
            field_list=[map_column(c, generator, "tables") for c in table.columns],
        ),
    )


def map_table(
    generator: PostgresqlGenerator,
    tables: List[Table],
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []

    for table in tables:
        logger.debug(f"Map table {table.table_name} {table.table_type}")
        if table.table_type == "BASE TABLE":
            tbl = _map_table(generator, table)
        elif table.table_type == "VIEW":
            tbl = map_view(generator, table)
        else:
            logger.warning(
                f"Parsing only [BASE_TABLE, VIEW]. Got unknown {table.table_type=} {table.oid=}"
            )
            continue

        data_entities.append(tbl)

    return data_entities
