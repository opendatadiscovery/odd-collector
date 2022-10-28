from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import SnowflakeGenerator

from odd_collector.adapters.snowflake.config import _data_set_metadata_schema_url, _data_set_metadata_excluded_keys
from odd_collector.adapters.snowflake.mappers.columns import map_column
from odd_collector.adapters.snowflake.mappers.metadata import append_metadata_extension
from odd_collector.adapters.snowflake.mappers.models import TableMetadata, ColumnMetadata
from odd_collector.adapters.snowflake.mappers.types import TABLE_TYPES_SQL_TO_ODD
from odd_collector.adapters.snowflake.mappers.utils import transform_datetime
from odd_collector.adapters.snowflake.mappers.views import extract_transformer_data


def map_table(
    oddrn_generator: SnowflakeGenerator, tables: list[tuple], columns: list[tuple], database: str
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []
    column_index: int = 0

    for table in tables:
        metadata: TableMetadata = TableMetadata(*table)
        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
            metadata.table_type, DataEntityType.UNKNOWN
        )
        oddrn_path = (
            "views" if data_entity_type == DataEntityType.VIEW else "tables"
        )

        table_schema: str = metadata.table_schema
        table_name: str = metadata.table_name

        oddrn_generator.set_oddrn_paths(
            **{"schemas": table_schema, oddrn_path: table_name}
        )

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
            name=metadata.table_name,
            type=data_entity_type,
            owner=metadata.table_owner,
            description=metadata.comment,
            metadata=[],
            updated_at=transform_datetime(metadata.last_altered),
            created_at=transform_datetime(metadata.created),
        )
        data_entities.append(data_entity)

        append_metadata_extension(
            data_entity.metadata,
            _data_set_metadata_schema_url,
            metadata,
            _data_set_metadata_excluded_keys,
        )

        data_entity.dataset = DataSet(
            rows_number=int(metadata.row_count)
            if metadata.row_count is not None
            else None,
            field_list=[],
        )

        # DataTransformer
        if data_entity.type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(
                metadata.view_definition, oddrn_generator
            )

        # DatasetField
        while column_index < len(columns):
            column: tuple = columns[column_index]
            column_metadata: ColumnMetadata = ColumnMetadata(*column)

            if (
                column_metadata.table_schema == metadata.table_schema
                and column_metadata.table_name == metadata.table_name
            ):
                data_entity.dataset.field_list.append(
                    map_column(
                        column_metadata, oddrn_generator, data_entity.owner, oddrn_path
                    )
                )
                column_index += 1
            else:
                break

    return data_entities
