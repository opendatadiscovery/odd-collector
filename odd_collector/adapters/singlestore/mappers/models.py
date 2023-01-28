from abc import ABC
from dataclasses import dataclass, fields
from typing import Any


@dataclass(frozen=True)
class SingleStoreMetadata(ABC):
    pass


@dataclass(frozen=True)
class TableMetadata(SingleStoreMetadata):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    table_type: Any
    engine: Any
    version: Any
    row_format: Any
    table_rows: Any
    avg_row_length: Any
    data_length: Any
    max_data_length: Any
    index_length: Any
    data_free: Any
    auto_increment: Any
    create_time: Any
    update_time: Any
    check_time: Any
    table_collation: Any
    checksum: Any
    create_options: Any
    table_comment: Any
    distributed: Any
    storage_type: Any
    alter_time: Any
    create_user: Any
    alter_user: Any
    flags: Any
    view_definition: Any


@dataclass(frozen=True)
class ColumnMetadata(SingleStoreMetadata):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    is_sparse: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_scale: Any
    character_set_name: Any
    collation_name: Any
    column_type: Any
    column_key: Any
    extra: Any
    privileges: Any
    column_comment: Any
    datetime_precision: Any

    @classmethod
    def get_str_fields(cls):
        return ", ".join([field.name for field in fields(ColumnMetadata)])
