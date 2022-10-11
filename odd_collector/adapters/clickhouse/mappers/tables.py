from typing import List

import pytz
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import ClickHouseGenerator

from ..domain import Column, IntegrationEngine, Table
from . import _data_set_metadata_excluded_keys, _data_set_metadata_schema_url
from .columns import map_column
from .metadata import extract_metadata
from .transformer import extract_transformer_data


def map_table(
    oddrn_generator: ClickHouseGenerator,
    tables: List[Table],
    columns: List[Column],
    integration_engines: List[IntegrationEngine],
    database: str,
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    for table in tables:
        data_entity_type = (
            DataEntityType.VIEW
            if table.engine in ["View", "MaterializedView"]
            else DataEntityType.TABLE
        )
        oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path, table.name),
            name=table.name,
            owner=None,
            description=None,
            metadata=[
                extract_metadata(
                    _data_set_metadata_schema_url,
                    _data_set_metadata_excluded_keys,
                    table,
                )
            ],
            type=data_entity_type,
        )
        data_entities.append(data_entity)

        if table.metadata_modification_time is not None:
            data_entity.updated_at = table.metadata_modification_time.replace(
                tzinfo=pytz.utc
            ).isoformat()

        # Dataset
        data_entity.dataset = DataSet(
            rows_number=int(table.total_rows) if table.total_rows is not None else None,
            field_list=[],
        )

        # DataTransformer
        if data_entity.type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(
                table, oddrn_generator, integration_engines
            )

        # Reduce time complexity now it is N_tables * M_columns
        for column in columns:
            if column.table == table.name:
                data_entity.dataset.field_list.append(
                    map_column(column, oddrn_generator, data_entity.owner, oddrn_path)
                )

    data_entities.append(
        DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("databases"),
            name=database,
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            data_entity_group=DataEntityGroup(
                entities_list=[de.oddrn for de in data_entities]
            ),
        )
    )

    return data_entities
