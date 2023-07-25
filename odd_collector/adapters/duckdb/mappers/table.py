from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import DuckDBGenerator

from .column import map_column
from .models import DuckDBColumn, DuckDBTable


def map_table(
    oddrn_generator: DuckDBGenerator, table: DuckDBTable, columns: list[DuckDBColumn]
) -> DataEntity:
    table.columns = columns
    oddrn_generator.set_oddrn_paths(
        catalogs=table.catalog,
        schemas=table.schema,
        tables=table.name,
    )
    field_list = []
    if columns is not None:
        for column in columns:
            processed_ds_fields = map_column(oddrn_generator, column)
            field_list.extend(processed_ds_fields)
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table.name),
        name=table.name,
        type=DataEntityType.TABLE,
        owner=table.schema,
        created_at=table.create_time,
        updated_at=table.update_time,
        metadata=[extract_metadata("duckdb", table, DefinitionType.DATASET)],
        dataset=DataSet(field_list=field_list),
    )
