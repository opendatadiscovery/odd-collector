from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from oddrn_generator import DuckDBGenerator
from odd_models.models import DataEntityType, DataSet
from odd_models.models import DataEntity
from .column import map_column
from .models import DuckDBTable, DuckDBColumn


def get_table(raw: dict, columns: list[dict]) -> DuckDBTable:
    columns = [get_column(column) for column in columns]
    table = DuckDBTable(
        catalog=raw.pop("table_catalog"),
        schema=raw.pop("table_schema"),
        name=raw.pop("table_name"),
        type=raw.pop("table_type"),
        odd_metadata=raw,
        columns=[column for column in columns],
    )
    return table


def get_column(column_raw: dict) -> DuckDBColumn:
    column = DuckDBColumn(
        table_catalog=column_raw.pop("table_catalog"),
        table_schema=column_raw.pop("table_schema"),
        table_name=column_raw.pop("table_name"),
        name=column_raw.pop("column_name"),
        type=column_raw.pop("data_type"),
        is_nullable=column_raw.pop("is_nullable"),
        odd_metadata=column_raw,
    )
    return column


def map_table(
    oddrn_generator: DuckDBGenerator,
    table_metadata: dict,
    columns_metadata: list[dict]
) -> DataEntity:
    table = get_table(table_metadata, columns_metadata)
    columns = table.columns
    oddrn_generator.set_oddrn_paths(
        catalogs=table.catalog,
        schemas=table.schema,
        tables=table.name,
    )
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table.name),
        name=table.name,
        type=DataEntityType.TABLE,
        owner=table.schema,
        created_at=table.create_time,
        updated_at=table.update_time,
        metadata=[extract_metadata("databricks", table, DefinitionType.DATASET)],
        dataset=DataSet(
            field_list=[map_column(oddrn_generator, column) for column in columns],
        ),
    )
