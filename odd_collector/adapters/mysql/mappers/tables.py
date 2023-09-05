from copy import deepcopy

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import MysqlGenerator

from odd_collector.models import Table

from ..logger import logger
from .columns import map_column
from .views import map_view


def map_table(generator: MysqlGenerator, table: Table) -> DataEntity:
    generator = deepcopy(generator)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables", table.name),
        name=table.name,
        type=DataEntityType.TABLE,
        owner=table.schema,
        description=table.comment,
        created_at=table.create_time.datetime,
        updated_at=table.update_time.datetime,
        metadata=[extract_metadata("mysql", table, DefinitionType.DATASET)],
        dataset=DataSet(
            rows_number=table.table_rows,
            field_list=[
                map_column(generator, column, "tables") for column in table.columns
            ],
        ),
    )


def map_tables(
    generator: MysqlGenerator,
    tables: list[Table],
) -> list[DataEntity]:
    data_entities: dict[str, tuple[Table, DataEntity]] = {}

    for table in tables:
        if table.type == "VIEW":
            data_entity = map_view(generator, table)
        elif table.type == "BASE TABLE":
            data_entity = map_table(generator, table)
        else:
            logger.warning(f"Can't parse {table.type=}. Available [VIEW, BASE_TABLE]")
            continue

        data_entities[table.uid] = (table, data_entity)

    for table, data_entity in data_entities.values():
        for dependency in table.dependencies:
            if dependency.uid in data_entities and data_entity.data_transformer:
                data_entity.data_transformer.inputs.append(
                    data_entities[dependency.uid][1].oddrn
                )

    return [data_entity for _, data_entity in data_entities.values()]
