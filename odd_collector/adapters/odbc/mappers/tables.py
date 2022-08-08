from odd_models.models import DataEntity, DataSet, MetadataExtension, DataEntityType
from oddrn_generator import OdbcGenerator

from . import _data_set_metadata_schema_url, Metadata, ColumnMetadata
from .columns import map_column
from .types import TABLE_TYPES_SQL_TO_ODD


def map_table(
    oddrn_generator: OdbcGenerator, tables: list[tuple], columns: list[tuple]
) -> list[DataEntity]:
    data_entities = [
        get_data_entity(table, columns, oddrn_generator) for table in tables
    ]

    return data_entities


def get_data_entity(table, columns, oddrn_generator):
    metadata: Metadata = Metadata(*table)
    data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
        metadata.table_type, DataEntityType.UNKNOWN
    )
    oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"
    table_schema: str = metadata.table_schem
    table_name: str = metadata.table_name
    oddrn_generator.set_oddrn_paths(**{"schemas": table_schema, oddrn_path: table_name})

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

    column_list = (ColumnMetadata(*column) for column in columns)
    matching_columns = (
        column_metadata
        for column_metadata in column_list
        if schema_and_table_name(column_metadata) == (table_schema, table_name)
    )
    cols = [
        map_column(column_metadata, oddrn_generator, data_entity.owner, oddrn_path)
        for column_metadata in matching_columns
    ]

    data_entity.dataset = DataSet(description=metadata.remarks, field_list=cols)
    return data_entity


def schema_and_table_name(column_metadata):
    return column_metadata.table_schem, column_metadata.table_name
