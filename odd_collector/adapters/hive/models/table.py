from funcy import get_in
from hive_metastore_client.builders.table_builder import Table as HTable

from odd_collector.adapters.hive.models.base_table import BaseTable
from odd_collector.helpers.datatime_from_timestamp import datetime_from_timestamp
from odd_collector.helpers.flatdict import FlatDict


class Table(BaseTable):
    @classmethod
    def from_hive(cls, table: HTable) -> "Table":
        metadata = {
            "retention": table.retention,
            "ownerType": table.ownerType,
            "table_type": table.tableType,
            "temporary": table.temporary,
            **FlatDict(table.parameters),
        }
        return cls(
            table_name=table.tableName,
            table_database=table.dbName,
            owner=table.owner,
            table_type=table.tableType,
            metadata=metadata,
            rows_number=get_in(table.parameters, ["numRows"]),
            create_time=datetime_from_timestamp(table.createTime),
        )
