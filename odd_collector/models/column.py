from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Column:
    table_catalog: str
    table_schema: str
    table_name: str
    name: str
    type: str
    is_nullable: bool
    default: Any = None
    comment: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
