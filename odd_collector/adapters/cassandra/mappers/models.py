from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableMetadata:
    keyspace_name: Any
    table_name: Any
    additional_write_policy: Any
    bloom_filter_fp_chance: Any
    caching: Any
    cdc: Any
    comment: Any
    compaction: Any
    compression: Any
    crc_check_chance: Any
    dclocal_read_repair_chance: Any
    default_time_to_live: Any
    extensions: Any
    flags: Any
    gc_grace_seconds: Any
    id: Any
    max_index_interval: Any
    memtable_flush_period_in_ms: Any
    min_index_interval: Any
    read_repair: Any
    read_repair_chance: Any
    speculative_retry: Any


@dataclass(frozen=True)
class ColumnMetadata:
    keyspace_name: Any
    table_name: Any
    column_name: Any
    clustering_order: Any
    column_name_bytes: Any
    kind: Any
    position: Any
    type: Any


@dataclass(frozen=True)
class ViewMetadata:
    keyspace_name: Any
    view_name: Any
    additional_write_policy: Any
    base_table_id: Any
    base_table_name: Any
    bloom_filter_fp_chance: Any
    caching: Any
    cdc: Any
    comment: Any
    compaction: Any
    compression: Any
    crc_check_chance: Any
    dclocal_read_repair_chance: Any
    default_time_to_live: Any
    extensions: Any
    gc_grace_seconds: Any
    id: Any
    include_all_columns: Any
    max_index_interval: Any
    memtable_flush_period_in_ms: Any
    min_index_interval: Any
    read_repair: Any
    read_repair_chance: Any
    speculative_retry: Any
    where_clause: Any
    view_definition: Any
