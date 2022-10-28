from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class TableMetadata:
    table_catalog: Any
    table_schema: Any
    table_name: Any
    table_owner: Any
    table_type: Any
    row_count: Any
    is_transient: Any
    clustering_keys: Any
    bytes: Any
    retention_time: Any
    created: Any
    last_altered: Any
    auto_clustering_on: Any
    comment: Any
    view_definition: Any
    is_secure: Any


@dataclass(frozen=True)
class ColumnMetadata:
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_precision_radix: Any
    numeric_scale: Any
    collation_name: Any
    is_identity: Any
    identity_generation: Any
    identity_start: Any
    identity_increment: Any
    identity_cycle: Any
    comment: Any
