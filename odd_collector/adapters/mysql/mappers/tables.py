from copy import deepcopy

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import MysqlGenerator

from odd_collector.models import Table

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
):
    data_entities: list[DataEntity] = []

    for table in tables:
        if table.type == "VIEW":
            data_entity = map_view(generator, table)
        elif table.type == "BASE TABLE":
            data_entity = map_table(generator, table)
        else:
            continue

        data_entities.append(data_entity)

    return data_entities
