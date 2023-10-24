from dataclasses import dataclass, field
from typing import Any, Optional

from odd_collector.models import Column, Table


@dataclass(frozen=True)
class DatabricksColumn(Column):
    ...


@dataclass()
class DatabricksTable(Table):
    columns: Optional[list[DatabricksColumn]] = field(default_factory=list)
    odd_metadata: Optional[dict[str, Any]] = field(default_factory=dict)
