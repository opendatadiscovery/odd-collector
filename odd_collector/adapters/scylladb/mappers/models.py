from dataclasses import dataclass

from cassandra.cqltypes import (
    VarcharType,
    DoubleType,
    Int32Type,
    UUIDType,
    BytesType,
    MapType,
    SetType,
    BooleanType,
)

from funcy import omit


@dataclass(frozen=True)
class TableMetadata:
    keyspace_name: VarcharType
    table_name: VarcharType
    bloom_filter_fp_chance: DoubleType
    caching: MapType
    comment: VarcharType
    compaction: MapType
    compression: MapType
    crc_check_chance: DoubleType
    dclocal_read_repair_chance: DoubleType
    default_time_to_live: Int32Type
    extensions: MapType
    flags: SetType
    gc_grace_seconds: Int32Type
    id: UUIDType
    max_index_interval: Int32Type
    memtable_flush_period_in_ms: Int32Type
    min_index_interval: Int32Type
    read_repair_chance: DoubleType
    speculative_retry: VarcharType

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
    keyspace_name: VarcharType
    table_name: VarcharType
    column_name: VarcharType
    clustering_order: VarcharType
    column_name_bytes: BytesType
    kind: VarcharType
    position: Int32Type
    type: VarcharType

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
    keyspace_name: VarcharType
    view_name: VarcharType
    base_table_id: UUIDType
    base_table_name: VarcharType
    bloom_filter_fp_chance: DoubleType
    caching: MapType
    comment: VarcharType
    compaction: MapType
    compression: MapType
    crc_check_chance: DoubleType
    dclocal_read_repair_chance: DoubleType
    default_time_to_live: Int32Type
    extensions: MapType
    gc_grace_seconds: Int32Type
    id: UUIDType
    include_all_columns: BooleanType
    max_index_interval: Int32Type
    memtable_flush_period_in_ms: Int32Type
    min_index_interval: Int32Type
    read_repair_chance: DoubleType
    speculative_retry: VarcharType
    where_clause: VarcharType
    view_definition: str

    @property
    def odd_metadata(self):
        return omit(
            self.__dict__,
            {
                "keyspace_name",
                "view_name",
                "view_definition",
                "comment",
            },
        )
