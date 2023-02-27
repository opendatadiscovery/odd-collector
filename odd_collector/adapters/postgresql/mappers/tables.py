import logging
from typing import List

from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import PostgresqlGenerator

from odd_collector.adapters.postgresql.config import (
    _data_set_metadata_excluded_keys,
    _data_set_metadata_schema_url,
)

from .columns import map_column
from .metadata import append_metadata_extension
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data
from ..models import TableMetadata, PrimaryKey, ColumnMetadata, EnumTypeLabel


def map_table(
    oddrn_generator: PostgresqlGenerator,
    tables: List[TableMetadata],
    columns: List[ColumnMetadata],
    primary_keys: List[PrimaryKey],
    enum_type_labels: List[EnumTypeLabel],
    database: str,
) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    column_index: int = 0

    for table in tables:
        try:
            primary_key_columns = [
                pk.column_name
                for pk in primary_keys
                if pk.table_name == table.table_name
            ]

            data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
                table.table_type, DataEntityType.UNKNOWN
            )
            oddrn_path = (
                "views" if data_entity_type == DataEntityType.VIEW else "tables"
            )

            table_schema: str = table.table_schema
            table_name: str = table.table_name

            oddrn_generator.set_oddrn_paths(
                **{"schemas": table_schema, oddrn_path: table_name}
            )

            # DataEntity
            data_entity: DataEntity = DataEntity(
                oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
                name=table_name,
                type=data_entity_type,
                owner=table.table_owner,
                description=table.description,
                metadata=[],
            )
            data_entities.append(data_entity)

            append_metadata_extension(
                data_entity.metadata,
                _data_set_metadata_schema_url,
                table,
                _data_set_metadata_excluded_keys,
            )

            data_entity.dataset = DataSet(
                rows_number=int(table.table_rows)
                if table.table_rows is not None
                else None,
                field_list=[],
            )

            # DataTransformer
            if data_entity_type == DataEntityType.VIEW:
                data_entity.data_transformer = extract_transformer_data(
                    table.view_definition, oddrn_generator
                )

            etl_grouped = {}
            for etl in enum_type_labels:
                if etl.type_oid not in etl_grouped:
                    etl_grouped[etl.type_oid] = []
                etl_grouped[etl.type_oid].append(etl)

            # DatasetField
            while column_index < len(columns):
                column_metadata: ColumnMetadata = columns[column_index]
                if (
                    column_metadata.table_schema == table_schema
                    and column_metadata.table_name == table_name
                ):
                    is_pk = column_metadata.column_name in primary_key_columns

                    enum_values = [e for e in etl_grouped[column_metadata.type_oid]] \
                        if column_metadata.type_oid in etl_grouped \
                        else None

                    data_entity.dataset.field_list.append(
                        map_column(
                            column_metadata,
                            oddrn_generator,
                            data_entity.owner,
                            oddrn_path,
                            enum_values,
                            is_pk
                        )
                    )
                    column_index += 1
                else:
                    break
        except Exception as err:
            raise MappingDataError(err)

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
