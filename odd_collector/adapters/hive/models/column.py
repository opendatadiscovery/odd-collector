from typing import Optional

from pydantic import BaseModel
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import ColumnStatistics

from odd_collector.adapters.hive.models.column_types import ColumnType


class Column(BaseModel):
    col_name: str
    col_type: ColumnType
    comment: Optional[str] = None
    statistic: Optional[ColumnStatistics] = None
    is_primary: Optional[bool] = False

    class Config:
        arbitrary_types_allowed = True
