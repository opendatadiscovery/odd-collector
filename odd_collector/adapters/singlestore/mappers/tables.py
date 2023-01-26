import pytz
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import SingleStoreGenerator

from . import _data_set_metadata_excluded_keys, _data_set_metadata_schema_url
from .columns import map_column
from .metadata import append_metadata_extension
from .models import ColumnMetadata, TableMetadata
from .types import TABLE_TYPES_SQL_TO_ODD
from .views import extract_transformer_data


def map_tables(
    oddrn_generator: SingleStoreGenerator,
    tables: list[tuple],
    columns: list[tuple],
    database: str,
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []
    column_metadata = [ColumnMetadata(*c) for c in columns]

    for table in tables:
        metadata: TableMetadata = TableMetadata(*table)
        table_name: str = metadata.table_name
        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
            metadata.table_type, DataEntityType.UNKNOWN
        )
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

        if metadata.table_type == "BASE TABLE":
            # it is for full tables only
            append_metadata_extension(
                data_entity.metadata,
                _data_set_metadata_schema_url,
                metadata,
                _data_set_metadata_excluded_keys,
            )

        if metadata.create_time is not None:
            data_entity.created_at = metadata.create_time.replace(
                tzinfo=pytz.utc
            ).isoformat()
        if metadata.update_time is not None:
            data_entity.updated_at = metadata.update_time.replace(
                tzinfo=pytz.utc
            ).isoformat()

        # Dataset
        data_entity.dataset = DataSet(rows_number=metadata.table_rows, field_list=[])

        # DataTransformer
        if data_entity_type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(
                metadata.view_definition, oddrn_generator
            )

        for column in column_metadata:
            if column.table_name == table_name and column.table_schema == database:
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
