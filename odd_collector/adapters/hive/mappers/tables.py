import logging
from datetime import datetime
from typing import List

from more_itertools import flatten
from odd_models.models import DataEntity, DataEntityType
from oddrn_generator import HiveGenerator

from . import StatsNamedTuple
from .columns.main import map_column
from .metadata import _metadata
from .utils import transform_datetime


def map_hive_table(
    host_name: str, table_stats, columns: dict, stats: List[StatsNamedTuple] = None
) -> DataEntity:

    hive_generator = HiveGenerator(
        host_settings=host_name,
        databases=table_stats.dbName,
    )
    hive_generator.set_oddrn_paths(**{"tables": table_stats.tableName})
    table_oddrn = hive_generator.get_oddrn_by_path("tables")
    owner = table_stats.owner
    created_at = f"{transform_datetime(datetime.fromtimestamp(table_stats.createTime))}"
    updated_at = (
        f"{transform_datetime(datetime.fromtimestamp(table_stats.lastAccessTime))}"
    )
    columns_mapping = list(
        flatten(
            [
                map_column(
                    c_name,
                    c_info["type"],
                    table_oddrn,
                    __get_stats(c_name, stats),
                    c_info["is_primary_key"],
                )
                for c_name, c_info in columns.items()
            ]
        )
    )
    try:
        result = DataEntity.parse_obj(
            {
                "type": DataEntityType.TABLE,
                "oddrn": table_oddrn,
                "name": table_stats.tableName,
                "owner": owner,
                "updated_at": updated_at,
                "created_at": created_at,
                "metadata": [_metadata(table_stats)],
                "dataset": {
                    "parent_oddrn": None,
                    "description": None,
                    "subtype": "DATASET_TABLE",
                    "rows_number": table_stats.parameters.get("numRows", None),
                    "field_list": columns_mapping or [],
                },
            }
        )
        return result
    except Exception as e:
        logging.error(
            f"Hive adapter can't build DataEntity for '{table_stats.tableName}' in '{table_stats.dbName}'"
            f" database: {e}"
        )
        return {}


def __get_stats(c_name: str, stats):
    try:
        if stats:
            result = next(i[1] for i in stats if c_name == i[0])
            return result
    except (StopIteration, TypeError, KeyError):
        logging.info(f"Hive adapter extracting column stats for '{c_name}' failed")
        return None
