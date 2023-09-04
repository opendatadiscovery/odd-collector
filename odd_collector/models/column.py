from dataclasses import dataclass, field
from typing import Any, Optional

from odd_collector_sdk.utils.metadata import HasMetadata


@dataclass(frozen=True)
class Column(HasMetadata):
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

    @property
    def odd_metadata(self):
        return self.metadata
