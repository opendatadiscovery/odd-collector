from typing import List

from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import TableauGenerator

from . import DATA_SET_EXCLUDED_KEYS, DATA_SET_SCHEMA
from .columns import map_column
from .metadata import extract_metadata


def map_table(
    oddrn_generator: TableauGenerator, tables: List[dict]
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []

    for table in tables:
        oddrn_generator.set_oddrn_paths(
            databases=table.get("database", dict()).get("id", ""),
            schemas=table.get("schema") or "unknown_schema",
            tables=table.get("name"),
        )

        owner = contact["name"] if (contact := table["contact"]) else None

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("tables"),
            name=table.get("name"),
            owner=owner,
            metadata=extract_metadata(DATA_SET_SCHEMA, table, DATA_SET_EXCLUDED_KEYS),
            description=table.get("description"),
            type=DataEntityType.TABLE,
            dataset=DataSet(
                parent_oddrn=oddrn_generator.get_oddrn_by_path("schemas")
                if table["schema"]
                else oddrn_generator.get_oddrn_by_path("databases"),
                description=table.get("description"),
                field_list=[
                    map_column(column, oddrn_generator, owner)
                    for column in table.get("columns", [])
                ],
            ),
        )

        data_entities.append(data_entity)

    return data_entities
