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

    @classmethod
    def from_related(cls, related: "Column", name: str, type: str):
        return cls(
            database=related.database,
            table=related.table,
            name=name,
            type=type,
            position=related.position,
            default_kind="",
            default_expression="",
            data_compressed_bytes=None,
            data_uncompressed_bytes=None,
            marks_bytes=None,
            comment="Nested column",
            is_in_partition_key=False,
            is_in_sorting_key=False,
            is_in_primary_key=False,
            is_in_sampling_key=False,
            compression_codec=None,
        )


@dataclass
class NestedColumn(Column):
    items: List["NestedColumn"]

    @classmethod
    def from_column(
        cls,
        column: Column,
        items: Optional[List["NestedColumn"]] = None,
        new_name: Optional[str] = None,
        new_type: Optional[str] = None,
    ):
        if items is None:
            items = []

        return cls(
            database=column.database,
            table=column.table,
            name=new_name if new_name else column.name,
            type=new_type if new_type else column.type,
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
            items=items,
        )
