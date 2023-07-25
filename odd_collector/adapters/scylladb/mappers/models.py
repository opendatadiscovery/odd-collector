import uuid
from dataclasses import dataclass
from typing import Any, Dict

from funcy import omit


@dataclass(frozen=True)
class TableMetadata:
    keyspace_name: str
    table_name: str
    bloom_filter_fp_chance: float
    caching: Dict[str, str]
    comment: str
    compaction: Dict[str, str]
    compression: Dict[str, str]
    crc_check_chance: float
    dclocal_read_repair_chance: float
    default_time_to_live: int
    extensions: Dict[str, Any]
    flags: list[str]
    gc_grace_seconds: int
    id: uuid
    max_index_interval: int
    memtable_flush_period_in_ms: int
    min_index_interval: int
    read_repair_chance: float
    speculative_retry: str

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {
                "keyspace_name",
                "table_name",
                "comment",
            },
        )


@dataclass(frozen=True)
class ColumnMetadata:
    keyspace_name: str
    table_name: str
    column_name: str
    clustering_order: str
    column_name_bytes: bytes
    kind: str
    position: int
    type: str

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {
                "keyspace_name",
                "table_name",
                "column_name",
                "column_name_bytes",
                "type",
            },
        )


@dataclass(frozen=True)
class ViewMetadata:
    keyspace_name: str
    view_name: str
    base_table_id: uuid
    base_table_name: str
    bloom_filter_fp_chance: float
    caching: Dict[str, str]
    comment: str
    compaction: Dict[str, str]
    compression: Dict[str, str]
    crc_check_chance: float
    dclocal_read_repair_chance: float
    default_time_to_live: int
    extensions: dict[str, Any]
    gc_grace_seconds: int
    id: uuid
    include_all_columns: bool
    max_index_interval: int
    memtable_flush_period_in_ms: int
    min_index_interval: int
    read_repair_chance: float
    speculative_retry: str
    where_clause: str
    view_definition: str

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {
                "keyspace_name",
                "view_name",
                "view_definition",
                "where_clause",
                "comment",
            },
        )
