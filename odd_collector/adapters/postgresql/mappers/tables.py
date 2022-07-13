import logging

from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup
from oddrn_generator import PostgresqlGenerator


from .columns import map_column
from odd_collector.adapters.postgresql.config import TableMetadata, ColumnMetadata, _data_set_metadata_schema_url, _data_set_metadata_excluded_keys
from .metadata import append_metadata_extension
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data

from typing import List

from ..exceptions import MappingException


def map_table(
    oddrn_generator: PostgresqlGenerator,
    tables: List[tuple],
    columns: List[tuple],
    database: str,
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    column_index: int = 0

    for table in tables:
        try:
            metadata: TableMetadata = TableMetadata(*table)

            data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
                metadata.table_type, DataEntityType.UNKNOWN
            )
            oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

            table_schema: str = metadata.table_schema
            table_name: str = metadata.table_name

            oddrn_generator.set_oddrn_paths(
                **{"schemas": table_schema, oddrn_path: table_name}
            )

        # DataEntity
            data_entity: DataEntity = DataEntity(
                oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
                name=table_name,
                type=data_entity_type,
                owner=metadata.table_owner,
                description=metadata.description,
                metadata=[],
            )
            data_entities.append(data_entity)

            append_metadata_extension(
                data_entity.metadata,
                _data_set_metadata_schema_url,
                metadata,
                _data_set_metadata_excluded_keys,
            )

            data_entity.dataset = DataSet(
                rows_number=int(metadata.table_rows)
                if metadata.table_rows is not None
                else None,
                field_list=[],
            )

        # DataTransformer
            if data_entity_type == DataEntityType.VIEW:
                data_entity.data_transformer = extract_transformer_data(
                    metadata.view_definition, oddrn_generator
                )

        # DatasetField
            while column_index < len(columns):
                column: tuple = columns[column_index]
                column_metadata: ColumnMetadata = ColumnMetadata(
                    *column
                )

                if (
                    column_metadata.table_schema == table_schema
                    and column_metadata.table_name == table_name
                ):
                    data_entity.dataset.field_list.append(
                        map_column(
                            column_metadata, oddrn_generator, data_entity.owner, oddrn_path
                        )
                    )
                    column_index += 1
                else:
                    break
        except Exception as err:
            logging.error('Error in map_table', exc_info=True)
            raise MappingException(err)

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
