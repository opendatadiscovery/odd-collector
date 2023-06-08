from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.mappers.table import map_table
from odd_collector.adapters.hive.mappers.view import map_view

from ..models.base_table import BaseTable
from ..models.table import Table
from ..models.view import View


def map_base_table(table: BaseTable, generator: HiveGenerator) -> DataEntity:
    generator.set_oddrn_paths(tables=table.table_name)

    data_entity = None
    if isinstance(table, Table):
        data_entity = map_table(table, generator)
    elif isinstance(table, View):
        data_entity = map_view(table, generator)
    if data_entity is None:
        raise MappingDataError(f"Could not map {table=}")

    return data_entity
