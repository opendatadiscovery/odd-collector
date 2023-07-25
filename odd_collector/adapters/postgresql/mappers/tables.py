from funcy import lmap, partial, silent
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import PostgresqlGenerator

from odd_collector.logger import logger

from ..models import Table
from .columns import map_column
from .metadata import get_table_metadata
from .views import map_view


def map_table(generator: PostgresqlGenerator, table: Table):
    generator.set_oddrn_paths(
        **{"schemas": table.table_schema, "tables": table.table_name}
    )

    map_table_column = partial(map_column, generator=generator, path="tables")
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        type=DataEntityType.TABLE,
        owner=table.table_owner,
        description=table.description,
        metadata=[get_table_metadata(entity=table)],
        dataset=DataSet(
            rows_number=silent(int)(table.table_rows),
            field_list=lmap(map_table_column, table.columns),
        ),
    )


def map_tables(
    generator: PostgresqlGenerator,
    tables: list[Table],
) -> list[DataEntity]:
    data_entities: dict[str, tuple[Table, DataEntity]] = {}

    for table in tables:
        logger.debug(f"Map table {table.table_name} {table.table_type}")

        if table.table_type == "BASE TABLE":
            entity = map_table(generator, table)
        elif table.table_type == "VIEW":
            entity = map_view(generator, table)
        else:
            logger.warning(
                f"Parsing only [BASE_TABLE, VIEW]. Got unknown {table.table_type=} {table.oid=}"
            )
            continue

        data_entities[table.as_dependency.uid] = table, entity

    for table, data_entity in data_entities.values():
        for dependency in table.dependencies:
            if dependency.uid in data_entities and data_entity.data_transformer:
                data_entity.data_transformer.inputs.append(
                    data_entities[dependency.uid][1].oddrn
                )

    return [entity for _, entity in data_entities.values()]
