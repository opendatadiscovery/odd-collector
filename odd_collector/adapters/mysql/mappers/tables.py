import pytz
from odd_models.models import DataEntity, DataSet, DataTransformer, DataEntityType, DataEntityGroup
from oddrn_generator import MysqlGenerator

from . import (
    MetadataNamedtuple, ColumnMetadataNamedtuple, _data_set_metadata_schema_url, _data_set_metadata_excluded_keys
)
from .columns import map_column
from .metadata import append_metadata_extension
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data

from typing import List


def map_tables(oddrn_generator: MysqlGenerator, tables: List[tuple], columns: List[tuple], database: str) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    column_index: int = 0

    for table in tables:
        metadata: MetadataNamedtuple = MetadataNamedtuple(*table)
        table_name: str = metadata.table_name

        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(metadata.table_type, DataEntityType.UNKNOWN)
        oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path, table_name),
            name=table_name,
            type=data_entity_type,
            owner=metadata.table_schema,
            description=metadata.table_comment,
            metadata=[],
        )
        data_entities.append(data_entity)

        if metadata.table_type == 'BASE TABLE':
            # it is for full tables only
            append_metadata_extension(data_entity.metadata, _data_set_metadata_schema_url, metadata,
                                      _data_set_metadata_excluded_keys)

        if metadata.create_time is not None:
            data_entity.created_at = metadata.create_time.replace(tzinfo=pytz.utc).isoformat()
        if metadata.update_time is not None:
            data_entity.updated_at = metadata.update_time.replace(tzinfo=pytz.utc).isoformat()

        # Dataset
        data_entity.dataset = DataSet(
            rows_number=metadata.table_rows,
            field_list=[]
        )

        # DataTransformer
        if data_entity_type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(metadata.view_definition, oddrn_generator)

        # DatasetField
        while column_index < len(columns):
            column: tuple = columns[column_index]
            column_metadata: ColumnMetadataNamedtuple = ColumnMetadataNamedtuple(*column)

            if column_metadata.table_name == table_name:
                data_entity.dataset.field_list.append(map_column(column_metadata, oddrn_generator, data_entity.owner, oddrn_path))
                column_index += 1
            else:
                break
    data_entities.append(DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("databases"),
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=[de.oddrn for de in data_entities]
        ),
    ))

    return data_entities
