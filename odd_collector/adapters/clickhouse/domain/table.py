import dataclasses
from typing import Any


@dataclasses.dataclass
class Table:
    name: str
    database: str
    engine: str
    uuid: str
    total_rows: int
    total_bytes: int
    metadata_path: str
    data_paths: Any
    is_temporary: bool
    create_table_query: str
    metadata_modification_time: Any
