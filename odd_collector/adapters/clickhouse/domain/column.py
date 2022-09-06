from dataclasses import dataclass
from typing import Any


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
