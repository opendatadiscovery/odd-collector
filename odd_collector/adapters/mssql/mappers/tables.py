from odd_models.models import DataEntity, DataSet, MetadataExtension, DataEntityType
from oddrn_generator import MssqlGenerator

from . import _data_set_metadata_schema_url, MetadataNamedtuple, ColumnMetadataNamedtuple
from .columns import map_column
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data


def map_table(oddrn_generator: MssqlGenerator, tables: list[tuple], columns: list[tuple]) -> list[DataEntity]:
    data_entities: list[DataEntity] = []
    column_index: int = 0

    for table in tables:
        metadata: MetadataNamedtuple = MetadataNamedtuple(*table)
        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(metadata.table_type, DataEntityType.UNKNOWN)
        oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

        table_schema: str = metadata.table_schema
        table_name: str = metadata.table_name

        oddrn_generator.set_oddrn_paths(**{'schemas': table_schema, oddrn_path: table_name})

        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
            name=table_name,
            type=data_entity_type,
            owner=oddrn_generator.get_oddrn_by_path("schemas"),
            metadata=[
                MetadataExtension(
                    schema_url=_data_set_metadata_schema_url,
                    metadata=metadata._asdict(),
                )
            ],
        )
        data_entities.append(data_entity)

        data_entity.dataset = DataSet(
            description=None,
            field_list=[]
        )

        # DataTransformer
        if data_entity_type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(metadata.view_definition, oddrn_generator)

        while column_index < len(columns):
            column: tuple = columns[column_index]
            column_metadata: ColumnMetadataNamedtuple = ColumnMetadataNamedtuple(*column)

            if column_metadata.table_schema == table_schema and column_metadata.table_name == table_name:
                data_entity.dataset.field_list.append(map_column(column_metadata, oddrn_generator, data_entity.owner, oddrn_path))
                column_index += 1
            else:
                break

    return data_entities
