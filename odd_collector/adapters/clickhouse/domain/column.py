from dataclasses import dataclass
from typing import Any, Dict, Optional, List


@dataclass
class Column:
    database: str
    table: str
    name: str
    type: str
    position: Any
    default_kind: str
    default_expression: str
    data_compressed_bytes: Any
    data_uncompressed_bytes: Any
    marks_bytes: Any
    comment: str
    is_in_partition_key: bool
    is_in_sorting_key: bool
    is_in_primary_key: bool
    is_in_sampling_key: bool
    compression_codec: Any


@dataclass
class NestedColumn(Column):
    is_nested: Optional[bool]
    items: List["NestedColumn"]

    @classmethod
    def from_column(
            cls,
            column: Column,
            is_nested: Optional[bool],
            items: List["NestedColumn"]
    ):
        return cls(
            database=column.database,
            table=column.table,
            name=column.name,
            type=column.type,
            position=column.position,
            default_kind=column.default_kind,
            default_expression=column.default_expression,
            data_compressed_bytes=column.data_compressed_bytes,
            data_uncompressed_bytes=column.data_uncompressed_bytes,
            marks_bytes=column.marks_bytes,
            comment=column.comment,
            is_in_partition_key=column.is_in_partition_key,
            is_in_sorting_key=column.is_in_sorting_key,
            is_in_primary_key=column.is_in_primary_key,
            is_in_sampling_key=column.is_in_sampling_key,
            compression_codec=column.compression_codec,
            is_nested=is_nested,
            items=items
        )

    @classmethod
    def from_column_change_name(
            cls,
            column: Column,
            new_name: str,
            is_nested: Optional[bool],
            items: List["NestedColumn"]
    ):
        return cls(
            database=column.database,
            table=column.table,
            name=new_name,
            type=column.type,
            position=column.position,
            default_kind=column.default_kind,
            default_expression=column.default_expression,
            data_compressed_bytes=column.data_compressed_bytes,
            data_uncompressed_bytes=column.data_uncompressed_bytes,
            marks_bytes=column.marks_bytes,
            comment=column.comment,
            is_in_partition_key=column.is_in_partition_key,
            is_in_sorting_key=column.is_in_sorting_key,
            is_in_primary_key=column.is_in_primary_key,
            is_in_sampling_key=column.is_in_sampling_key,
            compression_codec=column.compression_codec,
            is_nested=is_nested,
            items=items
        )

    @classmethod
    def from_column_change_name_type(
            cls,
            column: Column,
            new_name: str,
            new_type: str,
            is_nested: Optional[bool],
            items: List["NestedColumn"]
    ):
        return cls(
            database=column.database,
            table=column.table,
            name=new_name,
            type=new_type,
            position=column.position,
            default_kind=column.default_kind,
            default_expression=column.default_expression,
            data_compressed_bytes=column.data_compressed_bytes,
            data_uncompressed_bytes=column.data_uncompressed_bytes,
            marks_bytes=column.marks_bytes,
            comment=column.comment,
            is_in_partition_key=column.is_in_partition_key,
            is_in_sorting_key=column.is_in_sorting_key,
            is_in_primary_key=column.is_in_primary_key,
            is_in_sampling_key=column.is_in_sampling_key,
            compression_codec=column.compression_codec,
            is_nested=is_nested,
            items=items
        )
