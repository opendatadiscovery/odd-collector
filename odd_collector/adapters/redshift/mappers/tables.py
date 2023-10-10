from copy import deepcopy

import pytz
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import RedshiftGenerator

from ..logger import logger
from . import _data_set_metadata_excluded_keys_info, _data_set_metadata_schema_url_info
from .columns import map_column
from .metadata import MetadataTable, _append_metadata_extension
from .views import map_view


def map_table(generator: RedshiftGenerator, table: MetadataTable) -> DataEntity:
    generator.set_oddrn_paths(
        **{"schemas": table.schema_name, "tables": table.table_name}
    )

    # DataEntity
    data_entity: DataEntity = DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        owner=table.all.table_owner,
        metadata=[],
        type=DataEntityType.TABLE,
        dataset=DataSet(
            field_list=[
                map_column(
                    column,
                    generator,
                    table.all.table_owner,
                    "tables",
                    column.column_name in table.primary_keys,
                )
                for column in table.columns
            ]
        ),
        description=table.base.remarks if table.base is not None else None,
    )

    if table.all.table_type == "TABLE":
        # data_entity.dataset.subtype == 'DATASET_TABLE'
        # it is for full tables only
        _append_metadata_extension(
            metadata_list=data_entity.metadata,
            schema_url=_data_set_metadata_schema_url_info,
            metadata_dataclass=table.info,
            excluded_keys=_data_set_metadata_excluded_keys_info,
        )

    if table.all.table_creation_time is not None:
        data_entity.updated_at = table.all.table_creation_time.replace(
            tzinfo=pytz.utc
        ).isoformat()
        data_entity.created_at = table.all.table_creation_time.replace(
            tzinfo=pytz.utc
        ).isoformat()

    if table.info is not None:
        if table.info.estimated_visible_rows is not None:
            data_entity.dataset.rows_number = int(table.info.estimated_visible_rows)
        else:
            if table.info.tbl_rows is not None:
                data_entity.dataset.rows_number = int(table.info.tbl_rows)

    return data_entity


def map_tables(
    generator: RedshiftGenerator,
    tables: list[MetadataTable],
) -> list[DataEntity]:
    data_entities: dict[str, tuple[MetadataTable, DataEntity]] = {}

    for table in tables:
        logger.debug(f"Map table {table.table_name} {table.base.table_type}")

        if table.base.table_type == "BASE TABLE":
            entity = map_table(generator, table)
        elif table.base.table_type in ("VIEW", "MATERIALIZED VIEW"):
            entity = map_view(generator, table)
        else:
            logger.warning(
                f"Parsing only [BASE_TABLE, VIEW, MATERIALIZED_VIEW]. Got unknown {table.base.table_type}"
            )
            continue

        data_entities[table.as_dependency.uid] = table, entity

    for table, data_entity in data_entities.values():
        for dependency in table.dependencies:
            if dependency.uid in data_entities and data_entity.data_transformer:
                generator = deepcopy(generator)
                generator.set_oddrn_paths(
                    **{
                        "databases": dependency.database,
                        "schemas": dependency.schema,
                        "tables": dependency.name,
                    }
                )
                data_entity.data_transformer.inputs.append(
                    generator.get_oddrn_by_path("tables")
                )

    return [entity for _, entity in data_entities.values()]
