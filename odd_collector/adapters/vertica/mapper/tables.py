from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import VerticaGenerator

from ..domain.table import Table
from ..mapper.types import TABLE_TYPES_SQL_TO_ODD
from .columns import map_column
from .metadata import map_metadata
from .views import extract_transformer_data


def map_table(oddrn_generator: VerticaGenerator, table: Table) -> DataEntity:
    try:
        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
            table.table_type, DataEntityType.UNKNOWN
        )

        data_entity: DataEntity = DataEntity(
            oddrn=table.get_oddrn(oddrn_generator),
            name=table.table_name,
            type=data_entity_type,
            owner=table.owner_name,
            description=table.description,
            metadata=[map_metadata(table)],
            dataset=create_dataset(oddrn_generator, table),
        )

        if data_entity_type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(
                table.view_definition, oddrn_generator
            )
    except Exception as e:
        raise MappingDataError(f"Mapping table {table.table_name} failed") from e
    return data_entity


def create_dataset(oddrn_generator, table: Table):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("tables")
    columns = [
        map_column(oddrn_generator, column, table.owner_name)
        for column in table.columns
    ]

    return DataSet(
        parent_oddrn=parent_oddrn,
        field_list=columns,
        rows_number=int(table.table_rows) if table.table_rows is not None else None,
    )
