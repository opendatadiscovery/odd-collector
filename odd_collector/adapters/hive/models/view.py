from typing import Any, Optional

from hive_metastore_client.builders.table_builder import Table as HTable

from odd_collector.adapters.hive.logger import logger
from odd_collector.adapters.hive.models.base_table import BaseTable
from odd_collector.helpers.flatdict import FlatDict
from sql_metadata.parser import Parser

from odd_collector.helpers.datatime_from_timestamp import datetime_from_timestamp


class View(BaseTable):
    depends_on: list[str] = []

    @classmethod
    def from_hive(cls, table: HTable) -> "View":
        metadata = {
            "retention": table.retention,
            "ownerType": table.ownerType,
            "table_type": table.tableType,
            "temporary": table.temporary,
            "rewriteEnabled": table.rewriteEnabled,
            "viewExpandedText": table.viewExpandedText,
            "viewOriginalText": table.viewOriginalText,
            **FlatDict(table.parameters),
        }

        return cls(
            table_name=table.tableName,
            table_database=table.dbName,
            owner=table.owner,
            table_type=table.tableType,
            metadata=metadata,
            depends_on=get_view_depended_names(table.viewOriginalText),
            create_time=datetime_from_timestamp(table.createTime),
        )


def get_view_depended_names(view_text: Optional[str]) -> list[str]:
    if view_text is None:
        return []
    try:
        return Parser(view_text).tables
    except Exception as e:
        logger.warning(f"Could get table names from view {view_text}. {e}")
        return []
