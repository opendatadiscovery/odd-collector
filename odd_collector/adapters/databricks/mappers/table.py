from typing import Any

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import DatabricksUnityCatalogGenerator

from odd_collector.helpers.datetime_from_ms import datetime_from_milliseconds

from .column import map_column
from .models import DatabricksColumn, DatabricksTable


def get_table(raw: dict) -> DatabricksTable:
    table_info = {key: raw[key] for key in ["catalog_name", "schema_name", "name"]}
    columns = [get_column(table_info, column) for column in raw["columns"]]
    table = DatabricksTable(
        catalog=raw.get("catalog_name"),
        schema=raw.get("schema_name"),
        name=raw.pop("name"),
        type=raw.get("table_type"),
        create_time=datetime_from_milliseconds(raw.get("created_at")),
        update_time=datetime_from_milliseconds(raw.get("updated_at")),
        odd_metadata=raw,
        columns=[column for column in columns],
    )
    return table


def get_column(table_info: dict, column_raw: dict) -> DatabricksColumn:
    column = DatabricksColumn(
        table_catalog=table_info.get("catalog_name"),
        table_schema=table_info.get("schema_name"),
        table_name=table_info.get("name"),
        name=column_raw.pop("name"),
        type=column_raw.get("type_text"),
        is_nullable=column_raw.get("nullable"),
        odd_metadata=column_raw,
    )
    return column


def map_table(
    oddrn_generator: DatabricksUnityCatalogGenerator,
    table_metadata: dict[str, Any],
) -> DataEntity:
    table = get_table(table_metadata)
    columns = table.columns
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
        metadata=[extract_metadata("databricks", table, DefinitionType.DATASET)],
        dataset=DataSet(
            field_list=field_list,
        ),
    )
