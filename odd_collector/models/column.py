from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class Column:
    table_catalog: str
    table_schema: str
    table_name: str
    name: str
    type: str
    is_nullable: bool
    default: Any = None
    comment: Optional[str] = None
    is_primary_key: bool = False
    is_sort_key: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
