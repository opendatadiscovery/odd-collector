import logging
from typing import List
from datetime import datetime
from more_itertools import flatten
from odd_models.models import DataEntity, DataEntityType
from oddrn_generator import HiveGenerator
from .metadata import _metadata
from .columns.main import map_column
from . import StatsNamedTuple


def map_hive_table(host_name: str, table_stats, columns: dict, stats: List[StatsNamedTuple] = None) -> DataEntity:
    table_oddrn = HiveGenerator(host_settings=host_name, databases=table_stats.dbName, tables=table_stats.tableName)\
        .get_data_source_oddrn()
    owner = HiveGenerator(host_settings=host_name, databases=table_stats.dbName, owners=table_stats.owner)\
        .get_data_source_oddrn()
    created_at = f'{datetime.fromtimestamp(table_stats.createTime)}'
    updated_at = f'{datetime.fromtimestamp(table_stats.lastAccessTime)}'
    columns_mapping = list(flatten([map_column(c_name, c_type, table_oddrn, __get_stats(c_name, stats))
                                    for c_name, c_type in columns.items()]))
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
                    "rows_number": table_stats.parameters.get('numRows', None),
                    "field_list": columns_mapping or [],
                },
            }
        )
        return result
    except Exception as e:
        logging.error(f"Hive adapter can't build DataEntity for '{table_stats.tableName}' in '{table_stats.dbName}'"
                      f" database: {e}")
        return {}


def __get_stats(c_name: str, stats):
    try:
        if stats:
            result = next(i[1] for i in stats if c_name == i[0])
            return result
    except (StopIteration, TypeError, KeyError):
        logging.info(f"Hive adapter extracting column stats for '{c_name}' failed")
        return None
